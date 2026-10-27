use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    let protos = [
        "protos/temporal/api/enums/v1/stream.proto",
        "protos/temporal/api/stream/v1/message.proto",
        "protos/temporal/api/streamservice/v1/request_response.proto",
        "protos/temporal/sdk/streams/v1/runtime.proto",
    ];
    let includes = ["protos", "../protos/protos/api_upstream"];
    // protox compiles the protos in Rust, so the crate builds with no protoc, as Core's
    // `vendored-protox` build does.
    let descriptor_file = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("streams.bin");
    std::fs::write(
        &descriptor_file,
        protox::compile(protos, includes)?.encode_to_vec(),
    )?;
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .skip_protoc_run()
        .file_descriptor_set_path(&descriptor_file)
        // Use the Core types for the Temporal API messages, so that values need no conversion
        // between the crates. Not `.temporal.api.enums`, since Core lacks the Stream enums.
        .extern_path(
            ".temporal.api.common",
            "::temporalio_common::protos::temporal::api::common",
        )
        .extern_path(
            ".temporal.api.failure",
            "::temporalio_common::protos::temporal::api::failure",
        )
        .compile_protos(&protos, &includes)?;
    Ok(())
}
