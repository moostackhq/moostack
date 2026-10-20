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

use std::error::Error;
use std::path::Path;

use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::server::Router;
use v1::CallerKind;

/// Every caller kind, for a method that anyone may call.
pub const EVERY_CALLER: &[CallerKind] = &[
    CallerKind::Operator,
    CallerKind::ControlPlane,
    CallerKind::PeerNode,
    CallerKind::LocalService,
];

/// Who called, as the agent told the service in the call's metadata.
/// Each is empty if the metadata didn't say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Caller {
    /// The caller kind's name, such as `CALLER_KIND_OPERATOR`.
    pub kind: String,
    /// The calling node's name, for a peer node.
    pub node: String,
}

impl Caller {
    /// The caller of `request`.
    pub fn of<T>(request: &tonic::Request<T>) -> Caller {
        let get = |key| {
            request
                .metadata()
                .get(key)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        Caller {
            kind: get(CALLER_KIND_KEY),
            node: get(CALLER_NODE_KEY),
        }
    }
}

/// The description of a service whose methods are `methods`, each by its
/// name with the caller kinds allowed to call it.
pub fn describe(methods: &[(&str, &[CallerKind])]) -> v1::DescribeResponse {
    v1::DescribeResponse {
        methods: methods
            .iter()
            .map(|(name, callers)| v1::Method {
                name: name.to_string(),
                callers: callers.iter().map(|&c| c as i32).collect(),
            })
            .collect(),
    }
}

/// Serves `router` on the API socket in [`SOCKET_DIR`], replacing one an
/// earlier run left there, until the service gets SIGTERM or SIGINT.
pub async fn serve(router: Router) -> Result<(), Box<dyn Error>> {
    let path = Path::new(SOCKET_DIR).join(API_SOCKET);
    match std::fs::remove_file(&path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    let listener = UnixListener::bind(&path)?;
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    router
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
        })
        .await?;
    Ok(())
}
