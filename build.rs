//! Embeds the launcher's Info.plist so macOS privacy prompts attributed to
//! `oppa-launch` can name OPPA and say why. Only that binary gets it.
fn main() {
    let plist =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/oppa-launch/Info.plist");
    println!("cargo:rerun-if-changed={}", plist.display());
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        // One -Xlinker per argument: -Wl, would split a path at its commas.
        let path = plist.display().to_string();
        for argument in ["-sectcreate", "__TEXT", "__info_plist", &path] {
            println!("cargo:rustc-link-arg-bin=oppa-launch=-Xlinker");
            println!("cargo:rustc-link-arg-bin=oppa-launch={argument}");
        }
    }
}
