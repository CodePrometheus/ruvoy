//! Wakeup plumbing between producer threads and the Ruby runtime thread.
//!
//! Ruby blocks on a file descriptor rather than on a Rust channel, so that the
//! fiber scheduler keeps control of when the thread parks. Producers push a
//! command onto a channel and then write one byte here to break that block.

use crate::BridgeError;
use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    sync::{
        Mutex,
        mpsc::{Receiver, RecvTimeoutError},
    },
    time::Duration,
};

/// Creates the non-blocking socket pair a runtime waits on.
pub(crate) fn pair() -> Result<(UnixStream, UnixStream), BridgeError> {
    let (reader, writer) = UnixStream::pair().map_err(io_error)?;
    reader.set_nonblocking(true).map_err(io_error)?;
    writer.set_nonblocking(true).map_err(io_error)?;
    Ok((reader, writer))
}

/// Signals the runtime that a command is waiting.
///
/// A full socket buffer already means an unread wakeup is pending, so
/// `WouldBlock` is success rather than an error.
pub(crate) fn signal(writer: &Mutex<UnixStream>) -> Result<(), BridgeError> {
    let mut writer = writer
        .lock()
        .map_err(|_| BridgeError::Io("wake writer mutex was poisoned".to_owned()))?;
    loop {
        match writer.write(&[1]) {
            Ok(1) => return Ok(()),
            Ok(_) => {
                return Err(BridgeError::Io(
                    "wake socket accepted zero bytes".to_owned(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(io_error(error)),
        }
    }
}

/// Consumes every pending wakeup byte so the next block is meaningful.
pub(crate) fn drain(reader: &mut UnixStream) -> Result<(), BridgeError> {
    let mut buffer = [0_u8; 256];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => {
                return Err(BridgeError::Io(
                    "wake socket closed before shutdown".to_owned(),
                ));
            }
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(io_error(error)),
        }
    }
}

pub(crate) fn recv_reply<T>(
    receiver: &Receiver<Result<T, BridgeError>>,
    timeout: Duration,
) -> Result<Result<T, BridgeError>, BridgeError> {
    receiver.recv_timeout(timeout).map_err(map_recv_timeout)
}

pub(crate) fn map_recv_timeout(error: RecvTimeoutError) -> BridgeError {
    match error {
        RecvTimeoutError::Timeout => BridgeError::ResponseTimeout,
        RecvTimeoutError::Disconnected => BridgeError::RuntimeStopped,
    }
}

fn io_error(error: io::Error) -> BridgeError {
    BridgeError::Io(error.to_string())
}
