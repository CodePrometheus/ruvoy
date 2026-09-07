//! Links the module against the Ruby the crate is built for.

fn main() {
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
}
