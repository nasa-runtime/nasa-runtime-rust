pub mod grpc;
pub mod server_list;

use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// 客户端请求序号的起点，与步长共同保持通常分配为奇数。
const SEQUENCE_INITIAL_VALUE: i64 = 1;
/// 相邻客户端请求序号的增量。
const SEQUENCE_DELTA: i64 = 2;
/// 进程内请求序号来源，接近上界时允许重置。
static ATOMIC_SEQUENCE: AtomicI64 = AtomicI64::new(SEQUENCE_INITIAL_VALUE);

/// 业务作用：为客户端发出的远端请求分配进程内序号，区分请求与对应响应。
///
/// 参数说明: 无。
///
/// 返回：递增奇数的十进制文本；接近整数上界时重置计数，不承诺跨进程、重置或并发回绕期间的永久唯一性。
pub(crate) fn generate_request_id() -> String {
    let seq = ATOMIC_SEQUENCE.fetch_add(SEQUENCE_DELTA, Ordering::Relaxed);
    if seq > i64::MAX - 1000 {
        ATOMIC_SEQUENCE.store(SEQUENCE_INITIAL_VALUE, Ordering::SeqCst);
    }
    seq.to_string()
}

/// SDK 共享的延迟初始化 HTTP 客户端，复用连接池并约束连接及请求时长。
static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client initialization should succeed")
});

/// 业务作用：复用 SDK 的 HTTP 连接池及统一请求超时，供登录和地址发现调用使用。
///
/// 参数说明: 无。
///
/// 返回：延迟初始化的进程级客户端引用，连接超时为 5 秒、请求超时为 10 秒；构造失败时产生 panic。
pub(crate) fn http_client() -> &'static reqwest::Client {
    &HTTP_CLIENT
}
