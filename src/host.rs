//! Keeping the object that hosts the Ruby VM resident.

use std::{ffi::CStr, mem::MaybeUninit, sync::OnceLock};

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
/// Returns whether the object is now resident. A module that cannot pin itself
/// should refuse to load rather than wait to be unmapped mid-request.
#[must_use]
pub fn pin_in_memory() -> bool {
    static PINNED: OnceLock<bool> = OnceLock::new();

    *PINNED.get_or_init(|| {
        let Some(path) = own_object_path() else {
            return false;
        };
        // SAFETY: `path` came from `dladdr` for an address inside this object,
        // so it names a loaded object. `RTLD_NOLOAD` promotes the flags of that
        // object instead of loading anything, and the returned handle is
        // deliberately never closed.
        let handle =
            unsafe { libc::dlopen(path.as_ptr(), libc::RTLD_NOLOAD | libc::RTLD_NODELETE) };
        !handle.is_null()
    })
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
