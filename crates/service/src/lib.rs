//! What every cluster service has in common: the part of the service API
//! that the agent knows of, and how a service's container meets the agent.
//! It follows `api/service/v1`, whose Go package holds the same contract.

/// The `moostack.service.v1` messages, generated from
/// `api/service/v1/service.proto`.
pub mod v1 {
    tonic::include_proto!("moostack.service.v1");
}

/// The directory in a service's container where the agent mounts the
/// host's directory for the service.
pub const SOCKET_DIR: &str = "/run/moostack/service";

/// The unix socket in [`SOCKET_DIR`] that the service serves its API on.
pub const API_SOCKET: &str = "api.sock";

/// The metadata keys under which the agent tells a service who called:
/// [`CALLER_KIND_KEY`] holds the [`v1::CallerKind`]'s name, and
/// [`CALLER_NODE_KEY`] the calling node's name, for a peer node.
pub const CALLER_KIND_KEY: &str = "moostack-caller-kind";
pub const CALLER_NODE_KEY: &str = "moostack-caller-node";
