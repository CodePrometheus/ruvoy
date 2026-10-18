//! Calls from the application to clusters Envoy was configured with.
//!
//! Only the worker that owns a request may reach Envoy on its behalf, so the
//! runtime queues each call for that worker and parks the fiber that made it.
//! The worker delivers the answer into a stream the fiber is woken on.

use crate::{
    BridgeError, ResponseStream, StreamWaker,
    concurrency::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use std::{fmt, time::Duration};

/// Which clusters a filter lets the application call, and on what terms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpstreamSettings {
    /// Clusters the application may call; any other is refused before the call
    /// leaves Ruby.
    pub clusters: Vec<String>,
    /// How long Envoy waits for a call that sets no deadline of its own.
    pub timeout: Duration,
    /// Largest response body a call may bring back.
    pub max_response_bytes: usize,
    /// Most of a streamed response that may wait for the application to read
    /// it; beyond that the stream is reset.
    pub stream_buffer_bytes: usize,
}

impl UpstreamSettings {
    /// True when the application may call `cluster`.
    #[must_use]
    pub fn allows(&self, cluster: &str) -> bool {
        self.clusters.iter().any(|allowed| allowed == cluster)
    }
}

/// One call on its way to the worker.
#[derive(Debug)]
pub struct UpstreamRequest {
    /// Cluster to send it to.
    pub cluster: String,
    /// Headers, `:method`, `:path` and `host` included.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Request body; empty for none.
    pub body: Vec<u8>,
    /// How long Envoy waits for the response.
    pub timeout: Duration,
    /// Where the worker delivers the response.
    pub response: Arc<ResponseStream>,
}

/// One streamed call on its way to the worker.
#[derive(Debug)]
pub struct UpstreamStreamRequest {
    /// Cluster to send it to.
    pub cluster: String,
    /// Headers, `:method`, `:path` and `host` included.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Deadline for the whole exchange; zero for none.
    pub timeout: Duration,
    /// Where the worker takes the body from and delivers the response to.
    pub channel: Arc<StreamChannel>,
}

/// Both directions of one streamed call, shared by the fiber that made it
/// and the worker that carries it.
#[derive(Debug)]
pub struct StreamChannel {
    /// What the upstream sends back, for the fiber to read.
    pub response: Arc<ResponseStream>,
    /// The request body the application is still sending, for the worker to
    /// forward.
    pub request: Arc<ResponseStream>,
    reset_wanted: AtomicBool,
    timed_out: AtomicBool,
    trailers: Mutex<Option<Vec<(String, String)>>>,
}

impl StreamChannel {
    /// Opens both directions, each holding at most `budget` bytes.
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            response: Arc::new(ResponseStream::new(budget)),
            request: Arc::new(ResponseStream::new(budget)),
            reset_wanted: AtomicBool::new(false),
            timed_out: AtomicBool::new(false),
            trailers: Mutex::new(None),
        }
    }

    /// The application stops the stream: nothing more is read or sent, and
    /// the worker resets it.
    pub fn close(&self) {
        self.response.cancel();
        self.request.cancel();
        self.reset_wanted.store(true, Ordering::Release);
    }

    /// The application stops the stream because the upstream went quiet past
    /// its idle timeout.
    pub fn time_out(&self) {
        self.timed_out.store(true, Ordering::Release);
        self.close();
    }

    /// True when the application stopped the stream for going quiet.
    #[must_use]
    pub fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Acquire)
    }

    /// The stream fails with `error`, which the fiber reads in place of the
    /// rest of the response; the worker resets it if it is still open.
    pub fn fail(&self, error: BridgeError) {
        self.response.push_failure(error);
        self.request.cancel();
        self.reset_wanted.store(true, Ordering::Release);
    }

    /// True once the worker should reset the stream.
    #[must_use]
    pub fn wants_reset(&self) -> bool {
        self.reset_wanted.load(Ordering::Acquire)
    }

    /// Keeps the trailers the upstream ended with.
    pub fn set_trailers(&self, trailers: Vec<(String, String)>) {
        *self
            .trailers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(trailers);
    }

    /// The trailers the upstream ended with, if it sent any.
    #[must_use]
    pub fn trailers(&self) -> Option<Vec<(String, String)>> {
        self.trailers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// What a closed route never sent.
#[derive(Debug, Default)]
pub struct Unsent {
    /// Calls still queued.
    pub calls: Vec<UpstreamRequest>,
    /// Streams still queued.
    pub streams: Vec<UpstreamStreamRequest>,
}

/// One request's route to the upstreams its worker can reach.
#[derive(Clone)]
pub struct Upstreams {
    settings: Arc<UpstreamSettings>,
    calls: Arc<Mutex<Calls>>,
    waker: StreamWaker,
}

#[derive(Default)]
struct Calls {
    queued: Vec<UpstreamRequest>,
    streams: Vec<UpstreamStreamRequest>,
    /// Set once the request is gone, after which nothing queued would be sent.
    closed: bool,
}

impl Upstreams {
    /// Opens the route; `waker` asks the worker to send what was queued.
    #[must_use]
    pub fn new(settings: Arc<UpstreamSettings>, waker: StreamWaker) -> Self {
        Self {
            settings,
            calls: Arc::new(Mutex::new(Calls::default())),
            waker,
        }
    }

    /// The terms every call on this route is made on.
    #[must_use]
    pub fn settings(&self) -> &UpstreamSettings {
        &self.settings
    }

    /// Queues a call and wakes the worker. Refused once the request is gone.
    pub fn queue(&self, request: UpstreamRequest) -> Result<(), BridgeError> {
        self.push(|calls| calls.queued.push(request))
    }

    /// Queues a streamed call and wakes the worker. Refused once the request
    /// is gone.
    pub fn queue_stream(&self, request: UpstreamStreamRequest) -> Result<(), BridgeError> {
        self.push(|calls| calls.streams.push(request))
    }

    /// Asks the worker to look at its streams again, for body to send or a
    /// stream to stop.
    pub fn wake(&self) {
        (self.waker)();
    }

    /// Takes every call queued since the last take.
    pub fn take_queued(&self) -> Vec<UpstreamRequest> {
        std::mem::take(&mut self.lock().queued)
    }

    /// Takes every streamed call queued since the last take.
    pub fn take_queued_streams(&self) -> Vec<UpstreamStreamRequest> {
        std::mem::take(&mut self.lock().streams)
    }

    /// Refuses every later call and hands back the ones never sent.
    pub fn close(&self) -> Unsent {
        let mut calls = self.lock();
        calls.closed = true;
        Unsent {
            calls: std::mem::take(&mut calls.queued),
            streams: std::mem::take(&mut calls.streams),
        }
    }

    fn push(&self, add: impl FnOnce(&mut Calls)) -> Result<(), BridgeError> {
        {
            let mut calls = self.lock();
            if calls.closed {
                return Err(BridgeError::Upstream(
                    "the request this call belongs to has finished".to_owned(),
                ));
            }
            add(&mut calls);
        }
        (self.waker)();
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Calls> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for Upstreams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let calls = self.lock();
        f.debug_struct("Upstreams")
            .field("settings", &self.settings)
            .field("queued", &calls.queued.len())
            .field("streams", &calls.streams.len())
            .field("closed", &calls.closed)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn settings() -> Arc<UpstreamSettings> {
        Arc::new(UpstreamSettings {
            clusters: vec!["users".to_owned()],
            timeout: Duration::from_secs(1),
            max_response_bytes: 64,
            stream_buffer_bytes: 64,
        })
    }

    fn request() -> UpstreamRequest {
        UpstreamRequest {
            cluster: "users".to_owned(),
            headers: Vec::new(),
            body: Vec::new(),
            timeout: Duration::from_secs(1),
            response: Arc::new(ResponseStream::new(64)),
        }
    }

    #[test]
    fn only_listed_clusters_are_allowed() {
        assert!(settings().allows("users"));
        assert!(!settings().allows("user"));
        assert!(!settings().allows(""));
    }

    #[test]
    fn a_queued_call_wakes_the_worker_and_is_taken_once() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&wakes);
        let upstreams = Upstreams::new(
            settings(),
            Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
        );

        upstreams
            .queue(request())
            .expect("an open route should queue");
        upstreams
            .queue(request())
            .expect("an open route should queue");

        assert_eq!(wakes.load(Ordering::SeqCst), 2);
        assert_eq!(upstreams.take_queued().len(), 2);
        assert!(upstreams.take_queued().is_empty());
    }

    #[test]
    fn closing_hands_back_unsent_calls_and_refuses_later_ones() {
        let upstreams = Upstreams::new(settings(), Arc::new(|| {}));
        upstreams
            .queue(request())
            .expect("an open route should queue");

        assert_eq!(upstreams.close().calls.len(), 1);
        assert!(matches!(
            upstreams.queue(request()),
            Err(BridgeError::Upstream(_))
        ));
        assert!(upstreams.take_queued().is_empty());
    }

    fn stream_request() -> UpstreamStreamRequest {
        UpstreamStreamRequest {
            cluster: "users".to_owned(),
            headers: Vec::new(),
            timeout: Duration::ZERO,
            channel: Arc::new(StreamChannel::new(64)),
        }
    }

    #[test]
    fn a_queued_stream_wakes_the_worker_and_a_close_hands_it_back() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&wakes);
        let upstreams = Upstreams::new(
            settings(),
            Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
        );

        upstreams
            .queue_stream(stream_request())
            .expect("an open route should queue");

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(upstreams.close().streams.len(), 1);
        assert!(upstreams.queue_stream(stream_request()).is_err());
    }

    #[test]
    fn a_failed_channel_keeps_the_error_for_the_fiber_and_wants_a_reset() {
        let channel = StreamChannel::new(8);
        channel.fail(BridgeError::UpstreamOverflow("read too slowly".to_owned()));

        assert!(channel.wants_reset());
        assert!(channel.request.is_cancelled());
        assert!(matches!(
            channel.response.take_next(),
            Some(crate::StreamItem::Failed(BridgeError::UpstreamOverflow(_)))
        ));
    }

    #[test]
    fn closing_a_channel_drops_what_was_buffered_and_wants_a_reset() {
        let channel = StreamChannel::new(8);
        assert_eq!(
            channel.response.push_chunk_if_room(vec![1, 2, 3]),
            Some(true)
        );
        channel.close();

        assert!(channel.wants_reset());
        assert!(channel.response.is_cancelled());
        assert!(channel.response.take_next().is_none());
        assert!(!channel.timed_out());
    }

    #[test]
    fn a_channel_timed_out_is_closed_and_says_so() {
        let channel = StreamChannel::new(8);
        channel.time_out();

        assert!(channel.wants_reset());
        assert!(channel.response.is_cancelled());
        assert!(channel.timed_out());
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::concurrency::thread;

    /// The fiber queues while the worker may be tearing the request down; every
    /// call must end up either refused or handed back, never stranded.
    #[test]
    fn a_call_racing_the_close_is_refused_or_handed_back() {
        loom::model(|| {
            let upstreams = Upstreams::new(
                Arc::new(UpstreamSettings {
                    clusters: vec!["users".to_owned()],
                    timeout: Duration::from_secs(1),
                    max_response_bytes: 64,
                    stream_buffer_bytes: 64,
                }),
                Arc::from_std(std::sync::Arc::new(|| {})),
            );

            let caller = {
                let upstreams = upstreams.clone();
                thread::spawn(move || {
                    upstreams
                        .queue(UpstreamRequest {
                            cluster: "users".to_owned(),
                            headers: Vec::new(),
                            body: Vec::new(),
                            timeout: Duration::from_secs(1),
                            response: Arc::new(ResponseStream::new(64)),
                        })
                        .is_ok()
                })
            };
            let closer = {
                let upstreams = upstreams.clone();
                thread::spawn(move || upstreams.close().calls.len())
            };

            let queued = caller.join().expect("caller should not panic");
            let handed_back = closer.join().expect("closer should not panic");
            let left = upstreams.take_queued().len();

            assert_eq!(
                usize::from(queued),
                handed_back + left,
                "a queued call must be handed back by the close or still be queued"
            );
            assert!(
                !queued || handed_back == 1 || left == 1,
                "the call must be accounted for exactly once"
            );
        });
    }
}
