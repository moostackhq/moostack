// Generates the DNS server's service API from the protos that the Go code is
// generated from too, with the messages every service API shares from
// moostack-service.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(false)
        .extern_path(".moostack.service.v1", "::moostack_service::v1")
        .compile_protos(&["../../api/dns/v1/dns.proto"], &["../.."])?;
    Ok(())
}
