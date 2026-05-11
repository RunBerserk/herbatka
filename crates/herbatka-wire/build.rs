fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("cargo:rerun-if-changed=proto/herbatka_fleet.proto");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.compile_protos(&["proto/herbatka_fleet.proto"], &["proto/"])?;
    Ok(())
}
