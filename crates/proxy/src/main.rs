//! The proxy, which serves its service API on the socket the agent
//! gives it. For now it is a skeleton: it answers its health call,
//! describes its methods, and has the sample methods Echo and Ping.

use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, Ordering};

use moostack_service::v1::{
    CallerKind, DescribeRequest, DescribeResponse, HealthRequest, HealthResponse,
};
use moostack_service::{Caller, EVERY_CALLER};
use tonic::{Request, Response, Status};

mod api {
    tonic::include_proto!("moostack.proxy.v1");
}

use api::proxy_server::{Proxy, ProxyServer};
use api::{EchoRequest, EchoResponse};

/// The proxy. It counts the calls of Echo and Ping it receives.
#[derive(Debug, Default)]
struct Service {
    calls: AtomicI64,
}

impl Service {
    /// Answers Echo and Ping: the request's payload and its caller.
    fn echo(&self, request: Request<EchoRequest>) -> EchoResponse {
        let caller = Caller::of(&request);
        EchoResponse {
            payload: request.into_inner().payload,
            caller_kind: caller.kind,
            caller_node: caller.node,
            calls: self.calls.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
}

#[tonic::async_trait]
impl Proxy for Service {
    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse::default()))
    }

    async fn describe(
        &self,
        _: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Ok(Response::new(moostack_service::describe(&[
            ("Health", EVERY_CALLER),
            ("Describe", EVERY_CALLER),
            ("Echo", EVERY_CALLER),
            ("Ping", &[CallerKind::PeerNode]),
        ])))
    }

    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(Service::echo(self, request)))
    }

    async fn ping(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(Service::echo(self, request)))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let router =
        tonic::transport::Server::builder().add_service(ProxyServer::new(Service::default()));
    match moostack_service::serve(router).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("proxy: {e}");
            ExitCode::FAILURE
        }
    }
}
