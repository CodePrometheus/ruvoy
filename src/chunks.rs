//! The Ruby-facing ends of a chunk queue.
//!
//! The same two ends serve every byte stream the runtime hands to Ruby: a
//! request body arriving from the client, a response going back to it, and
//! both directions of an upstream exchange. Waiting is the caller's business:
//! `park` registers the `Async::Variable` the reactor resolves once the other
//! side makes progress, and refuses when there is no reason to wait.

use crate::{BridgeError, StreamHandle, StreamItem, error::ruby_error, extensions::upstream_error};
use magnus::{Error, RModule, RString, Ruby, Value, method, prelude::*};

/// The reading end: the chunks a peer sends.
#[magnus::wrap(class = "Ruvoy::ChunkSource", free_immediately)]
pub(crate) struct ChunkSource {
    handle: StreamHandle,
}

impl ChunkSource {
    pub(crate) fn new(handle: StreamHandle) -> Self {
        Self { handle }
    }

    /// Takes the next chunk, or nil when none is buffered right now; raises
    /// the failure that ended the stream.
    fn next_chunk(ruby: &Ruby, source: &Self) -> Result<Option<RString>, Error> {
        let Some(item) = source.handle.stream.take_next() else {
            return Ok(None);
        };
        // A sender held back by a full queue resumes once there is room.
        source.handle.wake();
        match item {
            StreamItem::Chunk(bytes) => Ok(Some(ruby.str_from_slice(&bytes))),
            StreamItem::Failed(error) => Err(upstream_error(ruby, &error)),
            StreamItem::Head(_) | StreamItem::End => Ok(None),
        }
    }

    /// True once the peer will send nothing more.
    fn finished(&self) -> bool {
        self.handle.stream.is_finished()
    }

    fn park(&self, waiter: Value) -> bool {
        self.handle.stream.park(waiter.into())
    }

    fn unpark(&self) {
        self.handle.stream.unpark();
    }
}

/// The writing end: the chunks that go to the peer.
#[magnus::wrap(class = "Ruvoy::ChunkSink", free_immediately)]
pub(crate) struct ChunkSink {
    handle: StreamHandle,
}

impl ChunkSink {
    pub(crate) fn new(handle: StreamHandle) -> Self {
        Self { handle }
    }

    /// Queues what fits of `chunk` from `offset` on and returns how many bytes
    /// were taken, or nil once the stream is over.
    fn write(&self, chunk: RString, offset: usize) -> Option<usize> {
        // SAFETY: the bytes are copied before control returns to Ruby, so the
        // string cannot be moved or collected while the slice is alive.
        let bytes = unsafe { chunk.as_slice() };
        let taken = self
            .handle
            .stream
            .push_chunk_within_budget(bytes.get(offset..).unwrap_or_default())?;
        if taken > 0 {
            self.handle.wake();
        }
        Some(taken)
    }

    fn writable(&self) -> bool {
        !self.handle.stream.is_saturated()
    }

    fn cancelled(&self) -> bool {
        self.handle.stream.is_cancelled()
    }

    /// Parks the writing fiber until there is room again or the peer has gone.
    fn park(&self, waiter: Value) -> bool {
        self.handle.stream.park_writer(waiter.into())
    }

    fn unpark(&self) {
        self.handle.stream.unpark();
    }

    /// Ends the stream; the peer sees its end once the queue drains.
    fn finish(&self) {
        if self.handle.stream.push_end() {
            self.handle.wake();
        }
    }
}

pub(crate) fn define(ruby: &Ruby, module: RModule) -> Result<(), BridgeError> {
    let source = module
        .define_class("ChunkSource", ruby.class_object())
        .map_err(|error| ruby_error("defining Ruvoy::ChunkSource", error))?;
    source
        .define_method("next_chunk", method!(ChunkSource::next_chunk, 0))
        .and_then(|()| source.define_method("finished?", method!(ChunkSource::finished, 0)))
        .and_then(|()| source.define_method("park", method!(ChunkSource::park, 1)))
        .and_then(|()| source.define_method("unpark", method!(ChunkSource::unpark, 0)))
        .map_err(|error| ruby_error("defining Ruvoy::ChunkSource methods", error))?;

    let sink = module
        .define_class("ChunkSink", ruby.class_object())
        .map_err(|error| ruby_error("defining Ruvoy::ChunkSink", error))?;
    sink.define_method("write", method!(ChunkSink::write, 2))
        .and_then(|()| sink.define_method("writable?", method!(ChunkSink::writable, 0)))
        .and_then(|()| sink.define_method("cancelled?", method!(ChunkSink::cancelled, 0)))
        .and_then(|()| sink.define_method("park", method!(ChunkSink::park, 1)))
        .and_then(|()| sink.define_method("unpark", method!(ChunkSink::unpark, 0)))
        .and_then(|()| sink.define_method("finish", method!(ChunkSink::finish, 0)))
        .map_err(|error| ruby_error("defining Ruvoy::ChunkSink methods", error))
}
