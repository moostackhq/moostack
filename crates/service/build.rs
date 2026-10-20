// Generates the service API's common part from the protos that the Go
// code is generated from too.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .compile_protos(&["../../api/service/v1/service.proto"], &["../.."])?;
    Ok(())
}
