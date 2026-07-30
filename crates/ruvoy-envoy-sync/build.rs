fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    if target_os == "macos" {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
        return;
    }
    if target_os != "linux" {
        return;
    }

    // Envoy dlopens the module, and dlopen does not consult the LD_LIBRARY_PATH
    // of whoever built it. Without an explicit RUNPATH the loader cannot find
    // libruby and Envoy refuses to start.
    println!("cargo:rerun-if-env-changed=RUBY");
    match ruby_libdir() {
        Some(libdir) => println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,{libdir}"),
        None => println!(
            "cargo:warning=could not resolve the Ruby libdir; the module will need LD_LIBRARY_PATH"
        ),
    }
}

fn ruby_libdir() -> Option<String> {
    let ruby = std::env::var("RUBY").unwrap_or_else(|_| "ruby".to_owned());
    let output = std::process::Command::new(ruby)
        .args(["-e", "print RbConfig::CONFIG['libdir']"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let libdir = String::from_utf8(output.stdout).ok()?;
    let libdir = libdir.trim();
    (!libdir.is_empty()).then(|| libdir.to_owned())
}
