fn main() {
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
}
