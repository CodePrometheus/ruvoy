//! Keeping the object that hosts the Ruby VM resident.

use std::{ffi::CStr, fmt, mem::MaybeUninit, sync::OnceLock};

/// Why the host object could not be pinned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinFailure(String);

impl fmt::Display for PinFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PinFailure {}

/// Pins the shared object this code was linked into for the process lifetime.
///
/// The Ruby VM thread, the process-wide statics and the `atexit` handler all
/// live in this object. A host that unloads it while any of them are alive —
/// Envoy does exactly that when the last configuration referencing the module
/// goes away — leaves them pointing at unmapped memory.
///
/// Hosts usually offer a setting for this, but a missing setting must not be
/// able to unmap a running VM, so the object pins itself instead.
///
/// A failure is reported rather than fatal: the host may already have been
/// asked to keep the object loaded, and refusing to start would turn a possible
/// hazard into a certain outage.
pub fn pin_in_memory() -> Result<(), PinFailure> {
    static PINNED: OnceLock<Result<(), PinFailure>> = OnceLock::new();

    PINNED
        .get_or_init(|| {
            let Some(path) = own_object_path() else {
                return Err(PinFailure("this object has no resolvable path".to_owned()));
            };
            // `RTLD_NOLOAD` promotes the flags of an already-loaded object
            // instead of loading anything, but a binding mode is still
            // mandatory: glibc rejects a mode that names neither.
            //
            // SAFETY: `path` came from `dladdr` for an address inside this
            // object, so it names a loaded object, and the returned handle is
            // deliberately never closed.
            let handle = unsafe {
                libc::dlopen(
                    path.as_ptr(),
                    libc::RTLD_LAZY | libc::RTLD_NOLOAD | libc::RTLD_NODELETE,
                )
            };
            if handle.is_null() {
                return Err(PinFailure(format!(
                    "{}: {}",
                    path.to_string_lossy(),
                    last_dl_error()
                )));
            }
            Ok(())
        })
        .clone()
}

fn last_dl_error() -> String {
    // SAFETY: `dlerror` returns either null or a string owned by the linker.
    let error = unsafe { libc::dlerror() };
    if error.is_null() {
        return "unknown error".to_owned();
    }
    // SAFETY: a non-null return is a NUL-terminated string valid until the next
    // call on this thread, and it is copied here.
    unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned()
}

/// Resolves the path of the object containing this function.
fn own_object_path() -> Option<&'static CStr> {
    let mut info = MaybeUninit::<libc::Dl_info>::uninit();
    // SAFETY: the address belongs to this object and `info` is writable.
    let found = unsafe { libc::dladdr(own_object_path as *const libc::c_void, info.as_mut_ptr()) };
    if found == 0 {
        return None;
    }
    // SAFETY: a non-zero return means `dladdr` initialized the struct.
    let path = unsafe { info.assume_init() }.dli_fname;
    if path.is_null() {
        return None;
    }
    // SAFETY: `dli_fname` points at a NUL-terminated string owned by the
    // dynamic linker, which outlives the process.
    Some(unsafe { CStr::from_ptr(path) })
}
