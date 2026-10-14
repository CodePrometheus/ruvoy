use crate::{
    BridgeError, ResponseHead,
    concurrency::{Arc, Mutex, MutexGuard},
};
use magnus::{Value, value::Opaque};
use std::{collections::VecDeque, fmt};

/// What the Envoy worker still has to send downstream.
#[derive(Debug)]
pub enum StreamItem {
    /// Status and headers, always delivered first.
    Head(ResponseHead),
    /// A body chunk exactly as the application yielded it.
    Chunk(Vec<u8>),
    /// The application finished producing the body.
    End,
    /// The application failed; nothing more will be produced.
    Failed(BridgeError),
}

/// The hand-off between the Ruby owner thread and one Envoy worker stream.
///
/// The queue is bounded by buffered bytes rather than chunk count, because a
/// Rack body is free to yield either many small strings or a few large ones,
/// and only the byte total bounds memory.
#[derive(Debug)]
pub struct ResponseStream {
    inner: Mutex<ResponseStreamInner>,
    max_buffered_bytes: usize,
}

#[derive(Default)]
struct ResponseStreamInner {
    items: VecDeque<StreamItem>,
    buffered_bytes: usize,
    cancelled: bool,
    /// Set once the producer has said there is nothing more to come.
    ended: bool,
    /// Set while the downstream write buffer is over its high watermark.
    paused: bool,
    /// An `Async::Variable` the waiting fiber is parked on.
    ///
    /// Held as an opaque handle, which the garbage collector cannot see, so it
    /// is only ever held while a fiber is blocked on the variable and keeping
    /// it reachable.
    waiter: Option<Opaque<Value>>,
}

impl ResponseStream {
    /// Creates a queue that holds at most `max_buffered_bytes` of body data.
    #[must_use]
    pub fn new(max_buffered_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(ResponseStreamInner::default()),
            max_buffered_bytes: max_buffered_bytes.max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ResponseStreamInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Marks the stream dead. Called when the Envoy filter goes away, so the
    /// Ruby side can stop enumerating a body nobody will read.
    pub fn cancel(&self) {
        let mut inner = self.lock();
        inner.cancelled = true;
        inner.items.clear();
        inner.buffered_bytes = 0;
    }

    /// Parks a fiber on `waiter` until the other side makes progress.
    ///
    /// Refuses, holding nothing, when a read would not have to wait. The check
    /// shares the lock `push` takes, so progress cannot land between it and the
    /// store. Called from the Ruby thread only, which is also the only thread
    /// that takes the handle back out again.
    #[must_use]
    pub fn park(&self, waiter: Opaque<Value>) -> bool {
        let mut inner = self.lock();
        if inner.buffered_bytes > 0 || inner.ended || inner.cancelled {
            return false;
        }
        inner.waiter = Some(waiter);
        true
    }

    /// Takes the parked fiber's handle, if any.
    pub fn unpark(&self) -> Option<Opaque<Value>> {
        self.lock().waiter.take()
    }

    /// Parks a writing fiber on `waiter` until the queue can take more.
    ///
    /// Refuses, holding nothing, when a write would not have to wait or the
    /// stream is cancelled. The check shares the lock the worker takes when it
    /// drains and when it asks [`Self::writer_owed_wakeup`], so room cannot open
    /// up between the check and the store without the worker seeing the waiter.
    #[must_use]
    pub fn park_writer(&self, waiter: Opaque<Value>) -> bool {
        let mut inner = self.lock();
        if inner.cancelled || !self.full(&inner) {
            return false;
        }
        inner.waiter = Some(waiter);
        true
    }

    /// True when a writer is parked and may now continue: the queue has room
    /// again, or the stream is cancelled and the writer must hear it.
    #[must_use]
    pub fn writer_owed_wakeup(&self) -> bool {
        let inner = self.lock();
        inner.waiter.is_some() && (inner.cancelled || !self.full(&inner))
    }

    /// True once nothing more will arrive and everything queued has been taken.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        let inner = self.lock();
        inner.cancelled || (inner.ended && inner.buffered_bytes == 0)
    }

    /// True once the downstream has gone away.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.lock().cancelled
    }

    /// Records whether the downstream write buffer is over its high watermark.
    pub fn set_paused(&self, paused: bool) {
        self.lock().paused = paused;
    }

    /// Bytes queued but not yet handed to the worker.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.lock().buffered_bytes
    }

    /// How many more bytes the queue accepts before it is full.
    #[must_use]
    pub fn spare_capacity(&self) -> usize {
        let inner = self.lock();
        self.max_buffered_bytes.saturating_sub(inner.buffered_bytes)
    }

    /// True while the producer must wait: either the buffer is full or the
    /// downstream asked us to stop writing.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.full(&self.lock())
    }

    fn full(&self, inner: &ResponseStreamInner) -> bool {
        inner.paused || inner.buffered_bytes >= self.max_buffered_bytes
    }

    /// Queues the response head. Returns `false` once the stream is cancelled.
    pub fn push_head(&self, head: ResponseHead) -> bool {
        self.push(StreamItem::Head(head))
    }

    /// Queues one body chunk. Returns `false` once the stream is cancelled.
    pub fn push_chunk(&self, chunk: Vec<u8>) -> bool {
        self.push(StreamItem::Chunk(chunk))
    }

    /// Queues as much of `bytes` as the queue has room for and returns how many
    /// it took, so a body larger than the budget is handed over in slices
    /// instead of held whole. `None` once the stream is cancelled.
    pub fn push_chunk_within_budget(&self, bytes: &[u8]) -> Option<usize> {
        let mut inner = self.lock();
        if inner.cancelled {
            return None;
        }
        let taken = bytes
            .len()
            .min(self.max_buffered_bytes.saturating_sub(inner.buffered_bytes));
        if taken > 0 {
            inner.buffered_bytes += taken;
            inner
                .items
                .push_back(StreamItem::Chunk(bytes[..taken].to_vec()));
        }
        Some(taken)
    }

    /// Queues `chunk` whole if it fits the budget, or if the queue is empty: a
    /// single delivery larger than the whole budget says nothing about a slow
    /// reader. `Some(false)` when it does not fit; `None` once cancelled.
    #[must_use]
    pub fn push_chunk_if_room(&self, chunk: Vec<u8>) -> Option<bool> {
        let mut inner = self.lock();
        if inner.cancelled {
            return None;
        }
        let room = self.max_buffered_bytes.saturating_sub(inner.buffered_bytes);
        if inner.buffered_bytes > 0 && chunk.len() > room {
            return Some(false);
        }
        inner.buffered_bytes += chunk.len();
        inner.items.push_back(StreamItem::Chunk(chunk));
        Some(true)
    }

    /// Marks the body complete. Returns `false` once the stream is cancelled.
    pub fn push_end(&self) -> bool {
        self.push(StreamItem::End)
    }

    /// Reports a failure. Returns `false` once the stream is cancelled.
    pub fn push_failure(&self, error: BridgeError) -> bool {
        self.push(StreamItem::Failed(error))
    }

    fn push(&self, item: StreamItem) -> bool {
        let mut inner = self.lock();
        if inner.cancelled {
            return false;
        }
        match &item {
            StreamItem::Chunk(chunk) => inner.buffered_bytes += chunk.len(),
            StreamItem::End | StreamItem::Failed(_) => inner.ended = true,
            StreamItem::Head(_) => {}
        }
        inner.items.push_back(item);
        true
    }

    /// Takes the next item unless the downstream is paused, in which case the
    /// worker leaves it queued so the producer keeps feeling the backpressure.
    pub fn take_next(&self) -> Option<StreamItem> {
        let mut inner = self.lock();
        if inner.paused {
            return None;
        }
        let item = inner.items.pop_front()?;
        if let StreamItem::Chunk(chunk) = &item {
            inner.buffered_bytes = inner.buffered_bytes.saturating_sub(chunk.len());
        }
        Some(item)
    }
}

impl fmt::Debug for ResponseStreamInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseStreamInner")
            .field("items", &self.items.len())
            .field("buffered_bytes", &self.buffered_bytes)
            .field("cancelled", &self.cancelled)
            .field("ended", &self.ended)
            .field("paused", &self.paused)
            .field("parked", &self.waiter.is_some())
            .finish()
    }
}

/// Wakes the Envoy worker that owns the downstream stream.
///
/// The Ruby thread must never touch Envoy state directly, so producing a chunk
/// only signals the worker; the worker then drains the queue on its own thread.
pub type StreamWaker = Arc<dyn Fn() + Send + Sync + 'static>;

/// One streaming response: the shared queue plus the way to wake its worker.
#[derive(Clone)]
pub struct StreamHandle {
    /// The queue the producer writes into.
    pub stream: Arc<ResponseStream>,
    /// Signals the worker that the queue has new items.
    pub waker: StreamWaker,
}

impl StreamHandle {
    /// Pairs a queue with the waker of the worker that owns it.
    #[must_use]
    pub fn new(stream: Arc<ResponseStream>, waker: StreamWaker) -> Self {
        Self { stream, waker }
    }

    /// Signals the owning worker that there is something to send.
    pub fn wake(&self) {
        (self.waker)();
    }
}

/// Streams whose parked fiber is owed a wakeup.
///
/// Envoy workers append to it and signal the runtime; the reactor drains it and
/// resolves each waiter, which is the only thread allowed to touch them.
#[derive(Clone, Debug, Default)]
pub struct Wakeups(Arc<Mutex<Vec<Arc<ResponseStream>>>>);

impl Wakeups {
    /// Records that `stream` made progress a parked fiber is waiting on.
    pub fn owe(&self, stream: Arc<ResponseStream>) {
        self.lock().push(stream);
    }

    /// Takes everything owed so far.
    pub fn take(&self) -> Vec<Arc<ResponseStream>> {
        std::mem::take(&mut *self.lock())
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Arc<ResponseStream>>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamHandle")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use magnus::value::QUNDEF;

    fn waiter() -> Opaque<Value> {
        // SAFETY: the value is only stored and taken back, never handed to Ruby.
        let value = unsafe { QUNDEF.as_value() };
        Opaque::from(value)
    }

    #[test]
    fn parking_is_refused_once_a_read_would_not_wait() {
        let buffered = ResponseStream::new(64);
        buffered.push_chunk(vec![0; 4]);
        let ended = ResponseStream::new(64);
        ended.push_end();
        let cancelled = ResponseStream::new(64);
        cancelled.cancel();

        for stream in [buffered, ended, cancelled] {
            assert!(
                !stream.park(waiter()),
                "{stream:?} should refuse the waiter"
            );
            assert!(stream.unpark().is_none(), "{stream:?} should hold nothing");
        }
    }

    #[test]
    fn an_empty_stream_holds_the_waiter_until_it_is_taken() {
        let stream = ResponseStream::new(64);
        assert!(stream.park(waiter()));
        assert!(stream.unpark().is_some());
        assert!(stream.unpark().is_none());
    }

    #[test]
    fn a_chunk_larger_than_the_budget_is_taken_in_slices() {
        let stream = ResponseStream::new(4);
        let body = b"abcdefghij";

        assert_eq!(stream.push_chunk_within_budget(body), Some(4));
        assert_eq!(stream.push_chunk_within_budget(&body[4..]), Some(0));
        assert_eq!(stream.buffered_bytes(), 4);

        let Some(StreamItem::Chunk(first)) = stream.take_next() else {
            panic!("the first slice should be queued");
        };
        assert_eq!(first, b"abcd");
        assert_eq!(stream.push_chunk_within_budget(&body[4..]), Some(4));
        assert_eq!(stream.buffered_bytes(), 4);
    }

    #[test]
    fn a_writer_parks_only_while_the_queue_is_full() {
        let stream = ResponseStream::new(4);
        assert!(
            !stream.park_writer(waiter()),
            "an empty queue needs no wait"
        );

        assert_eq!(stream.push_chunk_within_budget(b"abcd"), Some(4));
        assert!(stream.park_writer(waiter()));
        assert!(
            !stream.writer_owed_wakeup(),
            "still full, so nothing is owed yet"
        );

        stream.take_next();
        assert!(stream.writer_owed_wakeup());
        assert!(stream.unpark().is_some());
        assert!(!stream.writer_owed_wakeup());
    }

    #[test]
    fn a_paused_downstream_parks_the_writer_until_it_resumes() {
        let stream = ResponseStream::new(4);
        stream.set_paused(true);
        assert!(stream.park_writer(waiter()));
        assert!(!stream.writer_owed_wakeup());

        stream.set_paused(false);
        assert!(stream.writer_owed_wakeup());
    }

    #[test]
    fn cancelling_owes_a_parked_writer_its_wakeup() {
        let stream = ResponseStream::new(4);
        assert_eq!(stream.push_chunk_within_budget(b"abcd"), Some(4));
        assert!(stream.park_writer(waiter()));

        stream.cancel();
        assert!(stream.writer_owed_wakeup());
        assert!(
            !stream.park_writer(waiter()),
            "a cancelled stream never parks"
        );
    }

    #[test]
    fn a_delivery_is_taken_whole_or_refused_whole() {
        let stream = ResponseStream::new(4);
        assert_eq!(
            stream.push_chunk_if_room(vec![1; 8]),
            Some(true),
            "an empty queue takes any delivery"
        );
        assert_eq!(
            stream.push_chunk_if_room(vec![1]),
            Some(false),
            "a full queue refuses the next one"
        );
        stream.take_next();
        assert_eq!(stream.push_chunk_if_room(vec![1; 3]), Some(true));
        assert_eq!(
            stream.push_chunk_if_room(vec![1; 2]),
            Some(false),
            "3 + 2 is over a budget of 4"
        );
        stream.cancel();
        assert_eq!(stream.push_chunk_if_room(vec![1]), None);
    }

    #[test]
    fn a_cancelled_stream_takes_nothing() {
        let stream = ResponseStream::new(4);
        stream.cancel();

        assert_eq!(stream.push_chunk_within_budget(b"abc"), None);
        assert_eq!(stream.buffered_bytes(), 0);
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::concurrency::thread;

    /// A producer and its worker share the queue, so byte accounting has to
    /// survive every interleaving of push and take.
    #[test]
    fn concurrent_push_and_take_conserve_every_chunk() {
        loom::model(|| {
            let stream = Arc::new(ResponseStream::new(64));

            let producer = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || {
                    stream.push_chunk(vec![0; 2]);
                    stream.push_chunk(vec![0; 3]);
                })
            };
            let consumer = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || {
                    let mut taken = 0;
                    while let Some(StreamItem::Chunk(chunk)) = stream.take_next() {
                        taken += chunk.len();
                    }
                    taken
                })
            };

            producer.join().expect("producer should not panic");
            let mut taken = consumer.join().expect("consumer should not panic");
            while let Some(StreamItem::Chunk(chunk)) = stream.take_next() {
                taken += chunk.len();
            }

            assert_eq!(taken, 5, "no chunk may be lost or delivered twice");
            assert_eq!(stream.buffered_bytes(), 0, "accounting must return to zero");
        });
    }

    /// Cancelling races the producer; whichever order wins, no bytes may stay
    /// charged to a queue nobody will drain.
    #[test]
    fn cancelling_never_strands_buffered_bytes() {
        loom::model(|| {
            let stream = Arc::new(ResponseStream::new(64));

            let producer = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || stream.push_chunk(vec![0; 4]))
            };
            let canceller = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || stream.cancel())
            };

            let accepted = producer.join().expect("producer should not panic");
            canceller.join().expect("canceller should not panic");

            assert!(stream.is_cancelled());
            if accepted {
                // The push landed first; cancel must have cleared it.
                assert!(stream.take_next().is_none());
            }
            assert_eq!(stream.buffered_bytes(), 0, "cancel must release the queue");
        });
    }

    /// A body sliced into the queue while the worker drains it must never
    /// take the queue past its budget, and must arrive whole and in order.
    #[test]
    fn slices_never_take_the_queue_past_its_budget() {
        loom::model(|| {
            let stream = Arc::new(ResponseStream::new(2));
            let body = vec![1u8, 2, 3, 4, 5];

            let producer = {
                let stream = Arc::clone(&stream);
                let body = body.clone();
                thread::spawn(move || {
                    let mut offset = 0;
                    while offset < body.len() {
                        let taken = stream
                            .push_chunk_within_budget(&body[offset..])
                            .expect("nothing cancels this stream");
                        assert!(stream.buffered_bytes() <= 2, "over the budget");
                        offset += taken;
                        if taken == 0 {
                            thread::yield_now();
                        }
                    }
                })
            };

            let mut received = Vec::new();
            while received.len() < body.len() {
                match stream.take_next() {
                    Some(StreamItem::Chunk(chunk)) => received.extend(chunk),
                    Some(_) => unreachable!("only chunks are pushed"),
                    None => thread::yield_now(),
                }
            }
            producer.join().expect("producer should not panic");
            assert_eq!(received, body);
        });
    }

    /// A writer parking on a full queue while the worker drains it must never
    /// be stranded: either the worker sees it parked and owes it a wakeup, or
    /// the room was already there and the park is refused.
    #[test]
    fn a_parked_writer_is_never_stranded() {
        loom::model(|| {
            let stream = Arc::new(ResponseStream::new(2));
            assert_eq!(stream.push_chunk_within_budget(&[1, 2]), Some(2));

            let writer = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || {
                    // SAFETY: the value is only stored and taken back, never handed to Ruby.
                    let waiter = Opaque::from(unsafe { magnus::value::QUNDEF.as_value() });
                    stream.park_writer(waiter)
                })
            };
            let worker = {
                let stream = Arc::clone(&stream);
                thread::spawn(move || {
                    stream.take_next();
                    stream.writer_owed_wakeup()
                })
            };

            let parked = writer.join().expect("writer should not panic");
            let owed = worker.join().expect("worker should not panic");
            assert!(!parked || owed, "a parked writer was left without a wakeup");
        });
    }
}
