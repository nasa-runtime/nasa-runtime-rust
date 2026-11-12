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
    /// 每次选择时读取的目录来源，可包含按需刷新逻辑。
    provider: Arc<dyn ServerListProvider>,
    /// 下次调用预占的轮询序号，失败和取消均不回退。
    index: AtomicUsize,
}

impl PollingServerListService {
    /// 业务作用：建立带随机起点的地址轮询器，分散多个客户端的首次连接目标。
    ///
    /// 参数说明：
    /// - `provider`：提供当前地址目录的共享来源，首次返回的列表必须非空。
    ///
    /// 返回：持有来源和轮询序号的服务；初始列表为空时随机范围无效而 panic，此处不建立远端连接。
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

    /// 业务作用：允许调用方提交一次地址选择，目录读取和校验延后到 call。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：立即就绪，不探测 endpoint、目录是否非空或候选地址可达性。
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// 业务作用：为一次连接尝试预占轮询序号，并在实际执行时从当前目录选择地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：读取目录并筛选端口后的地址 Future；没有可选项时返回错误。序号在 Future 执行前递增，取消请求不会回退序号。
    fn call(&mut self, _: ()) -> Self::Future {
        let provider = self.provider.clone();
        let next_idx = self.index.fetch_add(1, Ordering::Relaxed);

        Box::pin(async move {
            let server_list = provider.current_server_list().await;
            // 先拒绝空目录和无有效端口的目录，再取模；地址入选不代表已通过连接检查。
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
    /// 选中时复制的主机文本，不随目录更新变化。
    host: String,
    /// 选中地址的服务基础端口。
    port: u16,
}

impl ServerAddress for PollingServerAddress {
    /// 业务作用：向传输适配器移交本次轮询选中的主机文本副本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：选中主机的字符串，不重新读取目录或解析 DNS。
    fn host(&self) -> String {
        self.host.clone()
    }

    /// 业务作用：提供本次选择时已解析的服务基础端口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：冻结在地址对象中的端口，不随后续目录刷新改变。
    fn port(&self) -> u16 {
        self.port
    }

    /// 业务作用：允许传输适配器尝试连接已选地址，轮询地址自身不维护健康剔除状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终为真；连接失败由传输和重连流程处理，不能据此认定服务健康。
    fn is_available(&self) -> bool {
        true
    }
}
