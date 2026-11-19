use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/douyin_webcast.proto");

    // Use vendored protoc so builds work out-of-the-box without installing protoc manually.
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build.rs runs single-threaded before compiling crates.
    unsafe {
        std::env::set_var("PROTOC", &protoc_path);
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&["proto/douyin_webcast.proto"], &["proto"])?;

    Ok(())
}
