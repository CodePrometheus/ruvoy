//! Calls from the application to clusters Envoy was configured with.
//!
//! Only the worker that owns a request may reach Envoy on its behalf, so the
//! runtime queues each call for that worker and parks the fiber that made it.
//! The worker delivers the answer into a stream the fiber is woken on.

use crate::{
    BridgeError, ResponseStream, StreamWaker,
    concurrency::{Arc, Mutex, MutexGuard},
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
        {
            let mut calls = self.lock();
            if calls.closed {
                return Err(BridgeError::Upstream(
                    "the request this call belongs to has finished".to_owned(),
                ));
            }
            calls.queued.push(request);
        }
        (self.waker)();
        Ok(())
    }

    /// Takes every call queued since the last take.
    pub fn take_queued(&self) -> Vec<UpstreamRequest> {
        std::mem::take(&mut self.lock().queued)
    }

    /// Refuses every later call and hands back the ones never sent.
    pub fn close(&self) -> Vec<UpstreamRequest> {
        let mut calls = self.lock();
        calls.closed = true;
        std::mem::take(&mut calls.queued)
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

        assert_eq!(upstreams.close().len(), 1);
        assert!(matches!(
            upstreams.queue(request()),
            Err(BridgeError::Upstream(_))
        ));
        assert!(upstreams.take_queued().is_empty());
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
                thread::spawn(move || upstreams.close().len())
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
