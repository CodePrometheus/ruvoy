//! Links the module against the Ruby the crate is built for.

fn main() {
    // Everything this crate produces links against libruby: the binary, the
    // examples, and the test harness. The dynamic modules get their RUNPATH
    // from their own build scripts; without this the rest would depend on the
    // caller exporting LD_LIBRARY_PATH.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "linux" {
        return;
    }
    println!("cargo:rerun-if-env-changed=RUBY");
    if let Some(libdir) = ruby_libdir() {
        // Covers every linked target this package produces, including the unit
        // test harness. The per-target forms are rejected outright when the
        // package has no target of that kind.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{libdir}");
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
