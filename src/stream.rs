use crate::{
    BridgeError, ResponseHead,
    concurrency::{Arc, Mutex, MutexGuard},
};
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

#[derive(Debug, Default)]
struct ResponseStreamInner {
    items: VecDeque<StreamItem>,
    buffered_bytes: usize,
    cancelled: bool,
    /// Set while the downstream write buffer is over its high watermark.
    paused: bool,
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

    /// True while the producer must wait: either the buffer is full or the
    /// downstream asked us to stop writing.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        let inner = self.lock();
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
        if let StreamItem::Chunk(chunk) = &item {
            inner.buffered_bytes += chunk.len();
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

impl fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamHandle")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
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
}
