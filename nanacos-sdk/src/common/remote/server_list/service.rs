use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures::Future;
use rand::RngExt;
use tower::Service;

use super::ServerAddress;
use super::provider::ServerListProvider;
use crate::api::error::Error;

pub(crate) struct PollingServerListService {
    provider: Arc<dyn ServerListProvider>,
    index: AtomicUsize,
}

impl PollingServerListService {
    pub(crate) async fn from_provider(provider: Arc<dyn ServerListProvider>) -> Self {
        let initial_list = provider.current_server_list().await;
        Self {
            provider,
            index: AtomicUsize::new(rand::rng().random_range(0..initial_list.len())),
        }
    }
}

impl Service<()> for PollingServerListService {
    type Response = Arc<dyn ServerAddress>;

    type Error = Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let provider = self.provider.clone();
        let next_idx = self.index.fetch_add(1, Ordering::Relaxed);

        Box::pin(async move {
            let server_list = provider.current_server_list().await;
            let parsed = super::parse_host_port_vec(&server_list)?;
            let idx = next_idx % parsed.len();
            let (host, port) = &parsed[idx];
            let server_address = PollingServerAddress {
                host: host.to_string(),
                port: *port,
            };
            tracing::debug!("Polling server selected: {}:{} (index {})", host, port, idx);
            Ok(Arc::new(server_address) as Arc<dyn ServerAddress>)
        })
    }
}

struct PollingServerAddress {
    host: String,
    port: u16,
}

impl ServerAddress for PollingServerAddress {
    fn host(&self) -> String {
        self.host.clone()
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn is_available(&self) -> bool {
        true
    }
}
