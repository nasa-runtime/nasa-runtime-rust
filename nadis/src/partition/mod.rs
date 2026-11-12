//! Redis Stream 分区消费：来源租约、不可变计划、业务键顺序与独立确认责任。
//!
//! 一个 RunningPartition 拥有专属 napart Runner 集合，按 source、group 或 stream 划分执行与容量。
//! 不同 Redis 源的实例各自拥有独立执行域、容量与停机控制，同名 runner 不跨实例复用。
//! Redis key 布局、信封、retry-op 与 disposition 协议保持跨语言兼容。
//! 本地同 key 顺序不是跨进程业务锁，生产者应将同一业务键路由到同一物理来源。

mod activation;
mod execution;
mod limits;
mod plan;
mod publisher;
mod shutdown;
pub use limits::{PartitionExecutorCfg, PartitionExecutorScope, PartitionLimits};
pub use plan::{PartitionRecord, RecordIdentity};
pub use publisher::PublisherSnapshot;
pub use runtime::{ExecutionDomainSnapshot, PartitionSnapshot};
pub use shutdown::PartitionShutdownReport;

/// 分区消费管理命令协议。
pub mod command;
// disposition/retryop 是毒消息处置与重试的内部协议,mutator 与 FenceCtx 只能由本 crate
// 的 runtime 使用;对外公开会绕过 owner 授权边界。
pub(crate) mod disposition;
/// 分区任期、bootstrap 和 ACK fencing 协议。
pub mod fencing;
pub(crate) mod retryop;
/// 分区消费运行时内部结构和重平衡循环。
pub mod runtime;

use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::client::RedisClient;
use crate::config::{CompatibilityProfile, PartitionCfg, MAX_REDIS_NAME_BYTES};
use crate::error::{NasaRedisError, Result};
use crate::lock::DistributedLock;

// ─────────────────────────────────────────────────────────────────────────
// Java 兼容哈希用于保持跨语言分区路由。
//
// compat 前缀只表示"局部算法保持兼容":例如字符串 hash、整数 hash、浮点格式化。
// 它不等价于 CompatibilityProfile::LegacyV1,后者是整套 wire/锁/ACK/marker 运行模式。
// ─────────────────────────────────────────────────────────────────────────

/// 业务作用：按 Java `String.hashCode()` 的 UTF-16 编码单元和有符号溢出规则生成跨语言路由哈希。
///
/// 参数说明：
/// - `s`: 用于路由或协议身份计算的 UTF-8 字符串，按 UTF-16 编码单元参与哈希。
///
/// 返回：32 位有符号哈希，溢出按二进制回绕；空字符串为零，不做 Unicode 归一化。
pub fn compat_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(u as i32))
}

/// 业务作用：按 Java `Long.hashCode()` 合并整数的高低 32 位，保持跨语言数字路由一致。
///
/// 参数说明：
/// - `v`: 完整的有符号 64 位业务标识。
///
/// 返回：高低位异或后的有符号 32 位哈希，负值按原始二进制位参与，不转换为十进制文本。
pub fn compat_long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

/// 业务作用：以兼容字符串哈希的非负低 31 位选择物理分区。
///
/// 参数说明：
/// - `key`: 业务路由键文本。
/// - `count`: 已验证为非零的物理分区总数。
///
/// 返回：小于 count 的分区编号；相同文本及分区数保持同一结果。count 为零时 panic，调用方必须先验证布局。
pub fn route_str(key: &str, count: u32) -> u32 {
    ((compat_string_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 业务作用：以兼容整数哈希的非负低 31 位选择物理分区，无需把整数转换成文本。
///
/// 参数说明：
/// - `key`: 业务路由整数键。
/// - `count`: 已验证为非零的物理分区总数。
///
/// 返回：小于 count 的分区编号；相同整数及分区数保持同一结果。count 为零时 panic，调用方必须先验证布局。
pub fn route_i64(key: i64, count: u32) -> u32 {
    ((compat_long_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 业务作用：把管理命令的相对等待窗口转成协议毫秒值。
///
/// 参数说明：
/// - `timeout`: 调用方提供的命令有效窗口，允许零，亚毫秒部分向下舍弃。
///
/// 返回：窗口不超过统一运行时上限且毫秒值可表示时返回 u64；否则返回 Config，不发出 Redis 请求。
fn command_timeout_millis(timeout: std::time::Duration) -> Result<u64> {
    // 期限会进入持久记录，必须在任何写入前拒绝不可表示的窗口，不能截断成更短的有效期。
    if timeout > std::time::Duration::from_millis(crate::config::MAX_REDIS_RUNTIME_DURATION_MS) {
        return Err(NasaRedisError::Config(format!(
            "partition command timeout 超过运行时上限 {}ms",
            crate::config::MAX_REDIS_RUNTIME_DURATION_MS
        )));
    }
    u64::try_from(timeout.as_millis())
        .map_err(|_| NasaRedisError::Config("partition command timeout 毫秒值溢出".into()))
}

/// 业务作用：按 Java `Double.toString` 的记法和指数阈值输出浮点文本，供跨语言 key 与字段编码使用。
/// 绝对值位于 `[10⁻³, 10⁷)` 时使用十进制，整数补 `.0`；其它非零有限值使用大写 `E` 的科学计数法。
/// 有效数字采用 Rust 的最短可往返表示，与 Java 的部分边界值可能选位不同，不保证所有 f64 逐字节一致。
/// 依赖跨语言 key 一致性的调用方应限制输入范围或采用整数、字符串身份；JSON 编码不经本函数。
///
/// 参数说明：
/// - `v`: 待格式化浮点值，允许 NaN、无穷和带符号零。
///
/// 返回：符合上述记法的文本；保留负零，非有限值使用 `NaN`、`Infinity` 或 `-Infinity`。
pub fn compat_double_to_string(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        }
        .to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .to_string();
    }
    let neg = v < 0.0;
    // Rust `{:e}` = 最短尾数 + 指数(一位整数位):如 "1.23456e2"、"1e0"、"1e-1"。
    let s = format!("{:e}", v.abs());
    let (mant, exp_str) = s.split_once('e').expect("{:e} 必含 e");
    let exp: i32 = exp_str.parse().expect("指数可解析");
    let digits: String = mant.chars().filter(|c| *c != '.').collect(); // 全部有效数字
    let body = if (-3..=6).contains(&exp) {
        // 十进制:小数点在从左数第 (exp+1) 位之后
        let point = exp + 1;
        let n = digits.len() as i32;
        if point <= 0 {
            format!("0.{}{}", "0".repeat((-point) as usize), digits)
        } else if point >= n {
            format!("{}{}.0", digits, "0".repeat((point - n) as usize))
        } else {
            format!(
                "{}.{}",
                &digits[..point as usize],
                &digits[point as usize..]
            )
        }
    } else {
        // 科学计数法:d.ddddE±exp(尾数至少一位小数,大写 E)
        let frac = if digits.len() > 1 {
            &digits[1..]
        } else {
            "0"
        };
        format!("{}.{}E{}", &digits[..1], frac, exp)
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 分区物理键布局。
// ─────────────────────────────────────────────────────────────────────────

/// 分区组的 key 布局。字段即协议——改名 = 新 layout 版本 + 数据迁移。
#[derive(Debug, Clone)]
pub struct KeyLayout {
    /// stream 前缀,同时是 consumer group 名(Java defaultGroup 语义)。
    pub prefix: String,
    /// 本节点 profile(**仅** nodes ZSET key 按 profile 隔离,见 `nodes()`)。
    pub profile: CompatibilityProfile,
}

impl KeyLayout {
    /// 业务作用：生成分区 Stream 键，让两种兼容协议访问相同物理来源。
    ///
    /// 参数说明：
    /// - `p`: 目标物理分区编号，由调用方核对是否在组范围内。
    ///
    /// 返回：`{prefix}:{p}`，不追加 profile、不访问 Redis，也不证明来源持有权。
    pub fn stream(&self, p: u32) -> String {
        format!("{}:{}", self.prefix, p)
    }

    /// 业务作用：生成两种兼容协议共同竞争的分区锁业务键。
    ///
    /// 参数说明：
    /// - `p`: 目标物理分区编号，由调用方核对是否在组范围内。
    ///
    /// 返回：`{prefix}:lock:{p}`，不含 profile；DistributedLock 还会叠加配置的锁前缀。
    /// 仅生成名称，实际持有权由锁协议及后续持有者复验决定。
    pub fn lock_business_key(&self, p: u32) -> String {
        format!("{}:lock:{}", self.prefix, p)
    }

    /// 业务作用：按心跳时基隔离存活节点集合，避免不同协议的 score 相互驱逐。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：RustV2 为 `{prefix}:nodes:v2`，LegacyV1 为 `{prefix}:nodes`。
    /// 混部时份额在各自集合内计算，可能产生负载倾斜；来源独占性仍依赖共同锁键，不由节点数量证明。
    pub fn nodes(&self) -> String {
        match self.profile {
            CompatibilityProfile::RustV2 => format!("{}:nodes:v2", self.prefix),
            _ => format!("{}:nodes", self.prefix),
        }
    }

    /// 业务作用：再平衡唤醒 Pub/Sub 通道:`{prefix}:wake`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组内共享的通道名称；仅生成名称，不建立订阅，也不保证信号持久化。
    pub fn wake(&self) -> String {
        format!("{}:wake", self.prefix)
    }

    /// 业务作用：consumer group 名 = prefix(Java:组名与 stream 前缀同名,全节点共用)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用完整物理前缀，包含 Cluster 布局已有的 hash tag；不返回本地逻辑组短名。
    pub fn group(&self) -> &str {
        &self.prefix
    }
}

/// 业务作用：在 Cluster 布局产生远端副作用前核对分区协议键的同槽约束。
///
/// 参数说明：
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `lock_prefix`: 用于校验分区 key 同槽的锁 key 前缀。
/// - `count`: 已验证的物理分区总数，逐分区核对共享与专属协议键。
///
/// 返回：各协议键与对应 Stream 的 CRC16 slot 一致时成功；空组名或任一异槽返回 Config。
/// 只进行本地名称核对，不查询集群拓扑，也不保证不同组落在不同 slot 或节点。
fn assert_same_slot_cluster(layout: &KeyLayout, lock_prefix: &str, count: u32) -> Result<()> {
    use crate::keytag::redis_slot;
    // 空组名不能构成有效布局；其余非法 hash tag 由实际键的 slot 比较拒绝，避免多键动作执行一半。
    if layout.group().is_empty() {
        return Err(NasaRedisError::Config(
            "cluster 同槽自检:default_group 为空,`{}` 空 hash-tag 会使各 key 落不同 slot → 多键 Lua CROSSSLOT".into(),
        ));
    }
    for p in 0..count {
        let stream = layout.stream(p);
        let want = redis_slot(stream.as_bytes());
        // 同组所有参与 Lua / 多键操作的 key(含跨分区共享的 fence/nodes/dlq/wake)
        let keys = vec![
            stream.clone(),
            format!("{lock_prefix}{}", layout.lock_business_key(p)),
            crate::partition::fencing::fence_key(layout),
            crate::partition::disposition::marker_key(layout, p),
            crate::partition::disposition::quarantine_key(layout, "self-check"),
            crate::partition::disposition::parked_index_key(layout, p),
            crate::partition::disposition::dlq_stream_key(layout),
            crate::partition::retryop::marker_key(layout, p, "self-check"),
            crate::partition::command::cmd_stream_key(layout, p),
            crate::partition::command::result_key(layout, p, "self-check"),
            layout.nodes(),
            layout.wake(),
        ];
        for k in &keys {
            let got = redis_slot(k.as_bytes());
            if got != want {
                return Err(NasaRedisError::Config(format!(
                    "cluster 同槽自检失败:分区 {p} 的 key \"{k}\"(slot {got})与 stream \"{stream}\"(slot {want})不同槽——\
                     KeyLayout 命名可能破坏了 `{{group}}` hash-tag 不变量,多键 Lua 会 CROSSSLOT"
                )));
            }
        }
    }
    tracing::info!(group = %layout.group(), count, "cluster 同槽自检通过(所有分区 Lua key 同 slot)");
    Ok(())
}

/// 业务作用：为默认组或隔离组建立物理布局，并从历史起点创建各分区 Stream 的消费组。
///
/// 参数说明：
/// - `client`: 已完成配置与拓扑准备的 Redis 客户端。
/// - `base`: 未包裹的物理前缀；默认组为配置组名，隔离组还包含逻辑组短名。
/// - `count`: 要准备的物理分区总数，必须非零。
///
/// 返回：所有分区建组完成或已存在时返回布局；数量、同槽约束或 Redis 失败返回错误。
/// 此前已创建的 Stream/消费组保持不变，重试继续幂等准备；不取得来源锁或启动消费。
/// 建组完成只接受 OK，已经存在只接受精确 BUSYGROUP 错误码，不以错误正文中的词语推定就绪。
async fn build_group_layout(
    client: &Arc<RedisClient>,
    base: &str,
    count: u32,
) -> Result<KeyLayout> {
    if count == 0 {
        return Err(NasaRedisError::Config(format!(
            "partition 组 \"{base}\" count 必须 > 0"
        )));
    }
    // Cluster 同槽 relayout 使用 group 级 hash-tag，使本组全部 key（stream/marker/lock/fence 等）含同一
    // tag `base` → 同 slot;单节点 prefix 不变。consumer group 名 = prefix(`group()`),包裹后两端一致。
    let prefix = if client.is_cluster() {
        format!("{{{base}}}")
    } else {
        base.to_string()
    };
    let layout = KeyLayout {
        prefix,
        profile: client.profile(),
    };

    // 同 group 内 RustV2(:nodes:v2)与墙钟(:nodes)混部告警(物理已隔离,仅 fair-share 倾斜)。
    {
        let v2 = format!("{}:nodes:v2", layout.prefix);
        let v1 = format!("{}:nodes", layout.prefix);
        let n2 = client.z_card(&v2).await.unwrap_or(0);
        let n1 = client.z_card(&v1).await.unwrap_or(0);
        if n2 > 0 && n1 > 0 {
            tracing::warn!(group = %layout.prefix, v2_nodes = n2, v1_nodes = n1,
                "同 group 检测到 RustV2 与墙钟两 profile 共存——nodes 已物理隔离,但 fair-share 会倾斜");
        }
    }

    // 建立任何消费组前先拒绝异槽布局，避免部分分区已创建后才发现持久处置无法使用多键 Lua。
    if client.is_cluster() {
        assert_same_slot_cluster(&layout, &client.config().lock.prefix, count)?;
    }

    for p in 0..count {
        let stream = layout.stream(p);
        // XGROUP CREATE <stream> <group> 0-0 MKSTREAM(0-0=从头;已存在 BUSYGROUP 幂等)
        let r: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(layout.group())
            .arg("0-0")
            .arg("MKSTREAM")
            .query_async(&mut client.conn())
            .await;
        match r {
            Ok(reply) if reply == "OK" => {}
            // 只有确定的已存在事实可复用；任意成功文本或相似错误都不能授权后续来源启动。
            Err(e) if e.code() == Some("BUSYGROUP") => {}
            Ok(_) => {
                return Err(NasaRedisError::Parsing(
                    "invalid partition group creation response".into(),
                ));
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(layout)
}

// ─────────────────────────────────────────────────────────────────────────
// 事件信封使用标准 JSON 对象；LegacyV1 与 RustV2 使用相同字段和 data entry field。
// Jackson default-typing=true 的类名包装数组不属于此线格式；跨语言发送方须关闭该包装。
// profile 决定租约与 fencing 协议，不改变 Envelope 编码。
// ─────────────────────────────────────────────────────────────────────────

/// 业务作用：把信封字段的显式 null 解码为默认值，与字段缺省时的 serde 默认策略配合。
///
/// 参数说明：
/// - `deserializer`: serde 提供的信封字段反序列化器。
///
/// 返回：null 返回 T 的默认值，其它合法输入返回解码值；类型错误保持 serde 错误，不以默认值掩盖非法正文。
fn de_null_as_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// 写入 Stream entry 的 `data` field 的标准 JSON 信封，字段为 topic、event、data、passthrough。
/// topic/event 缺省或 null 解码为 `""`，data 缺省或 null 解码为 `Value::Null`，
/// passthrough 缺省或 null 解码为 `None`；空路由仍由消费计划匹配门禁拒绝。
/// 序列化输出标准 JSON，passthrough 为 None 时省略该字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(default, deserialize_with = "de_null_as_default")]
    /// 业务主题,通常映射到 stream 或 partition topic。
    pub topic: String,
    #[serde(default, deserialize_with = "de_null_as_default")]
    /// 业务事件名,消费端按它选择 handler。
    pub event: String,
    /// 业务数据(发布时序列化为 JSON 值;消费侧按注册的 T 反序列化)。
    /// `serde_json::Value` 原生把 `null` 解成 `Value::Null`;`#[serde(default)]` 覆盖字段缺省。
    #[serde(default)]
    pub data: serde_json::Value,
    /// 显式传递的上下文，逐条消费通过 PartitionRecord 暴露；不会自动捕获或恢复 thread-local/MDC。
    /// 缺省或 null 表示没有上下文。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough: Option<serde_json::Map<String, serde_json::Value>>,
}

/// stream entry 中装信封的 field 名(Java DATA_FIELD 同款)。
pub const DATA_FIELD: &str = "data";

// ─────────────────────────────────────────────────────────────────────────
// handler 注册形态。
// ─────────────────────────────────────────────────────────────────────────

/// 批量回调的兼容类型：输入为 `(entry_id, data JSON 值)` 列表，输出为失败 ID 子集。
/// 空输出表示全部成功；本类型本身不执行解码、异常隔离或持久确认。
pub type ErasedHandler = Arc<
    dyn Fn(Vec<(String, serde_json::Value)>) -> futures::future::BoxFuture<'static, Vec<String>>
        + Send
        + Sync,
>;

/// 兼容批量回调的 `(topic, event)` 路由表；PreparedPartition 使用另行冻结的消费计划。
pub type HandlerMap = HashMap<(String, String), ErasedHandler>;

// ─────────────────────────────────────────────────────────────────────────
// 准备对象负责注册计划，启动后移交运行对象管理消费、发布与停机。
// ─────────────────────────────────────────────────────────────────────────

/// 默认组的逻辑 ID，group-scoped 管理 API 使用空字符串定位默认组，隔离组使用逻辑短名。
/// 逻辑 ID 与 Redis Stream prefix 分离，default_group 与隔离组短名相同也不会覆盖路由。
pub const DEFAULT_GROUP_ID: &str = "";

/// 一个已 prepare 的分区组(默认组或隔离组):stream/consumer group 已建,handler 待注册。
/// 携带本组 **resolved** 配置(per-group 覆盖 → 父级/全局),start 时移交各自 `GroupRuntime`。
struct PreparedGroup {
    /// **逻辑 ID**(map key;默认组 = `""` sentinel,隔离组 = 逻辑短名)。与 Redis prefix 解耦,防撞 key。
    id: String,
    layout: KeyLayout,
    count: u32,
    plans: plan::PlanMap,
    /// 本组 resolved `PartitionCfg`(rebalance/min_idle/drain/handler/poison 等;默认组=父级)。
    cfg: PartitionCfg,
    /// 本组 resolved `StreamCfg`(batch/poll/inflight;默认组=全局)。
    stream_cfg: crate::config::StreamCfg,
}

/// 已准备物理布局的分区消费实例，允许注册不可变计划，尚未持有来源锁或执行 handler。
pub struct PreparedPartition {
    client: Arc<RedisClient>,
    lock: Arc<DistributedLock>,
    /// 本节点标识(consumer name + 心跳 member;每次启动唯一;**全组共用同一 node_id**)。
    node_id: String,
    /// 分区组集合：`[0]` = 默认组(id=`""`),其余 = 隔离组。
    groups: Vec<PreparedGroup>,
    /// topic → `groups` 下标（未命中时使用默认组 `[0]`）。
    topic_to_group: HashMap<String, usize>,
    /// 旧链式注册入口保留首个配置错误，启动前拒绝而不覆盖原 handler。
    registration_error: Option<NasaRedisError>,
    next_plan_id: u32,
}

/// 已建立监督任务的分区实例，提供发布、隔离处置、命令提交、观测与统一停机。
/// 由 start_suspended 返回时仍等待显式激活，运行后不能继续注册消费计划。
pub struct RunningPartition {
    stop: Arc<shutdown::ShutdownOperation>,
    start_gate: tokio_util::sync::CancellationToken,
    /// **默认组** runtime(= `groups[""]`;无 group 参数的管理 API 默认作用它,向后兼容单组语义)。
    inner: Arc<runtime::GroupRuntime>,
    /// 全部组(含默认组)runtime:**逻辑 ID** → runtime(默认组 key=`""`,**不会与隔离组逻辑名撞**)。
    groups: HashMap<String, Arc<runtime::GroupRuntime>>,
    /// topic → **逻辑 ID**（未命中时走 inner 中的默认组）。
    topic_to_group: HashMap<String, String>,
}

impl PreparedPartition {
    /// 业务作用：准备默认组和隔离组的物理消费布局、主题映射与待注册计划，不取得来源锁或调用业务。
    ///
    /// 参数说明：
    /// - `client`: 分区框架使用的 Redis 客户端。
    /// - `lock`: 分区 owner 竞争使用的分布式锁组件。
    ///
    /// 返回：设施已启用且全部布局及主题映射有效时返回可注册计划的对象；配置或 Redis 失败返回错误。
    /// 建组不是跨组事务，失败前已经创建的 Stream/消费组保留，后续准备可继续幂等复用。
    pub async fn prepare(client: Arc<RedisClient>, lock: Arc<DistributedLock>) -> Result<Self> {
        // 两种 profile 共用标准 JSON Envelope；启用设施与物理布局校验先于任何远端建组副作用。
        let cfg = client.config().partition.clone();
        // 未显式启用时拒绝准备，避免仅构造配置就创建 Stream 和消费组。
        if !cfg.enabled {
            return Err(NasaRedisError::Config(
                "partition.enabled=false:分区设施未启用。需启用请显式设 \
                 cfg.partition.enabled=true(对齐 Legacy 总开关语义)"
                    .into(),
            ));
        }
        // 物理分区数必须非零，否则发布取模与持锁来源集合都无法成立。
        if cfg.count == 0 {
            return Err(NasaRedisError::Config("partition.count 必须 > 0".into()));
        }
        //default_group 字符校验。cluster 下 prefix=`{group}` 作 hash-tag——
        // 空串 → `{}` 空 tag → 整 key 参与哈希、各 key 不同 slot → 所有多键 Lua CROSSSLOT;
        // 含 `{`/`}`/`:` 会破坏 tag 提取(`{a:b}` 取到 `a:b` 或嵌套混乱)。fail-closed 提前拦。
        if cfg.default_group.trim().is_empty()
            || cfg.default_group != cfg.default_group.trim()
            || cfg.default_group.len() > MAX_REDIS_NAME_BYTES
            || cfg.default_group.contains(['{', '}', ':'])
        {
            return Err(NasaRedisError::Config(format!(
                "partition.default_group 非法:必须无首尾空白、非空、不超过 {MAX_REDIS_NAME_BYTES} 字节且不得含 `{{`/`}}`/`:`(cluster 下作 hash-tag)"
            )));
        }
        // 每组独立创建物理布局并证明 Lua key 同槽；不同组的 hash tag 不承诺落在不同 master。
        // 本地执行与容量隔离由 executor.scope 决定，物理分组本身不保证慢任务互不影响。
        let mut groups: Vec<PreparedGroup> = Vec::with_capacity(1 + cfg.groups.len());
        let mut topic_to_group: HashMap<String, usize> = HashMap::new();

        let global_stream = client.config().stream.clone();
        // 默认组 = groups[0],逻辑 ID = "" sentinel(与 default_group prefix 解耦,防与隔离组逻辑名撞 key)。
        // 默认组无 per-group 覆盖:cfg=父级(groups 清空,runtime 不读)、stream=全局。
        let def_layout = build_group_layout(&client, &cfg.default_group, cfg.count).await?;
        let mut def_cfg = cfg.clone();
        def_cfg.groups = HashMap::new();
        groups.push(PreparedGroup {
            id: DEFAULT_GROUP_ID.to_string(),
            layout: def_layout,
            count: cfg.count,
            plans: HashMap::new(),
            cfg: def_cfg,
            stream_cfg: global_stream.clone(),
        });

        // 隔离组(yml partition.groups.{逻辑名};顺序无关,topic 路由唯一即可)
        for (logical, gcfg) in &cfg.groups {
            // 与 connect 期使用相同的名称合同；此处保留二道防线，阻止任何绕过 validate 的构造路径。
            if logical.trim().is_empty()
                || logical != logical.trim()
                || logical.len() > MAX_REDIS_NAME_BYTES
                || logical.contains(['{', '}', ':'])
            {
                return Err(NasaRedisError::Config(format!(
                    "partition.groups 含非法逻辑名:必须无首尾空白、非空、不超过 {MAX_REDIS_NAME_BYTES} 字节且不得含 `{{`/`}}`/`:`"
                )));
            }
            let gcount = if gcfg.count > 0 {
                gcfg.count
            } else {
                cfg.count
            };
            let base = format!("{}:{}", cfg.default_group, logical); // Java streamPrefix = defaultGroup:groupName
            let layout = build_group_layout(&client, &base, gcount).await?;
            let idx = groups.len();
            // topic 路由:topics 为空时，逻辑组名本身即唯一 topic
            let topics: Vec<String> = if gcfg.topics.is_empty() {
                vec![logical.clone()]
            } else {
                gcfg.topics.clone()
            };
            for t in topics {
                if t.trim().is_empty() || t != t.trim() || t.len() > MAX_REDIS_NAME_BYTES {
                    return Err(NasaRedisError::Config(format!(
                        "partition.groups 含非法 topic:必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
                    )));
                }
                if let Some(&prev) = topic_to_group.get(&t) {
                    return Err(NasaRedisError::Config(format!(
                        "topic \"{t}\" 被多个隔离组路由(组 \"{}\" 与 \"{}\");一个 topic 只能属一个组",
                        groups[prev].id, logical
                    )));
                }
                topic_to_group.insert(t, idx);
            }
            groups.push(PreparedGroup {
                id: logical.clone(),
                layout,
                count: gcount,
                plans: HashMap::new(),
                // per-group resolved 配置(覆盖 → 父级/全局)
                cfg: gcfg.resolved_partition(&cfg, gcount),
                stream_cfg: gcfg.resolved_stream(&global_stream),
            });
        }
        if !cfg.groups.is_empty() {
            tracing::info!(default = %cfg.default_group, isolation_groups = cfg.groups.len(),
                "partition 多组就绪:1 默认 + {} 隔离组(高频/低频隔离消费)", cfg.groups.len());
        }

        Ok(Self {
            client,
            lock,
            // node_id:进程级唯一并由全部组共用, incarnation 要求
            node_id: format!("n-{}", uuid::Uuid::new_v4().simple()),
            groups,
            topic_to_group,
            registration_error: None,
            next_plan_id: 1,
        })
    }

    /// 业务作用：登记兼容批量 handler；重复或非法路由保留首个注册，并在 start 返回配置错误。
    ///
    /// 参数说明：
    /// - `topic`: 要绑定的业务 topic。
    /// - `event`: 要绑定的业务事件名。
    /// - `f`: 批量处理该 topic/event 的异步 handler。
    ///
    /// 返回：返回当前准备对象供链式调用；需要立即取得注册错误时使用 `try_register_legacy`。
    pub fn register<T, F, Fut>(&mut self, topic: &str, event: &str, f: F) -> &mut Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        if let Err(error) = self.try_register_legacy(topic, event, f) {
            if self.registration_error.is_none() {
                self.registration_error = Some(error);
            }
        }
        self
    }

    /// 业务作用：在注册点校验并冻结兼容批量路由，保留逐条解码与成功子集一次 Vec 调用的语义。
    ///
    /// 参数说明：
    /// - `topic`: 非空、无首尾空白的业务主题。
    /// - `event`: 非空、无首尾空白的事件名称。
    /// - `f`: 接收本桶成功解码数据的批量异步 handler。
    ///
    /// 返回：成功返回当前准备对象；非法或重复路由返回错误，已有 handler 不受影响。
    pub fn try_register_legacy<T, F, Fut>(
        &mut self,
        topic: &str,
        event: &str,
        f: F,
    ) -> Result<&mut Self>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        activation::validate_route(topic, event)?;
        let idx = self.topic_to_group.get(topic).copied().unwrap_or(0);
        let key = (topic.to_string(), event.to_string());
        // 必须在创建和发布新闭包前拒绝冲突，否则旧 handler 会失去路由归属。
        if self.groups[idx].plans.contains_key(&key) {
            return Err(NasaRedisError::Config(format!(
                "partition route already registered: ({topic}, {event})"
            )));
        }
        let id = self.reserve_plan_id()?;
        self.groups[idx]
            .plans
            .insert(key, plan::ConsumerPlan::legacy::<T, _, _>(id, f));
        Ok(self)
    }

    /// 业务作用：签发不复用的计划身份，同时给有序和无序任务保留独立 TaskType。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份空间充足时递增；耗尽时拒绝登记。
    fn reserve_plan_id(&mut self) -> Result<u32> {
        let id = self.next_plan_id;
        if id >= u32::MAX / 2 {
            return Err(NasaRedisError::Config(
                "consumer plan identity exhausted".into(),
            ));
        }
        self.next_plan_id += 1;
        Ok(id)
    }

    /// 业务作用：把多个 topic 的同一事件绑定到共享业务键顺序域。
    ///
    /// 参数说明：
    /// - `topics`: 共同绑定此计划的非空主题集合，不能重复或与已注册路由冲突。
    /// - `event`: 各主题共同使用的事件名。
    /// - `key_fn`: 从已解码记录提取可选顺序键；None 使用无键执行通道。
    /// - `handler`: 接收单条 PartitionRecord 的异步业务处理器。
    ///
    /// 返回：全部路由合法且无冲突时原子登记计划；失败不改变已有路由。
    /// 本入口额外堆分配估算为零，需核算解码后堆内存时使用带 weight 的入口。
    pub fn register_partitioned<T, K, I, S, KF, H, Fut>(
        &mut self,
        topics: I,
        event: impl Into<String>,
        key_fn: KF,
        handler: H,
    ) -> Result<&mut Self>
    where
        T: DeserializeOwned + Send + 'static,
        K: std::hash::Hash,
        I: IntoIterator<Item = S>,
        S: Into<String>,
        KF: Fn(&T) -> Option<K> + Send + Sync + 'static,
        H: Fn(PartitionRecord<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        self.register_partitioned_with_weight(topics, event, key_fn, |_| 0, handler)
    }

    /// 业务作用：登记带额外堆分配估算的单记录计划，在调用业务前完成正文预算核算。
    ///
    /// 参数说明：
    /// - `topics`: 共同绑定此计划的非空主题集合，按已有主题映射选择物理组。
    /// - `event`: 各主题共同使用的非空事件名。
    /// - `key_fn`: 从已解码记录提取可选顺序键；None 使用无键执行通道。
    /// - `weight`: 估算已解码 T 额外持有的堆内存字节数，与原始正文预算合并。
    /// - `handler`: 接收单条 PartitionRecord 的异步业务处理器。
    ///
    /// 返回：全部路由校验后共享同一计划身份并同步登记；空集合、非法或重复路由、身份耗尽返回 Config。
    /// 校验失败不发布任何候选路由；本地顺序域仍受配置的执行域划分约束。
    pub fn register_partitioned_with_weight<T, K, I, S, KF, WF, H, Fut>(
        &mut self,
        topics: I,
        event: impl Into<String>,
        key_fn: KF,
        weight: WF,
        handler: H,
    ) -> Result<&mut Self>
    where
        T: DeserializeOwned + Send + 'static,
        K: std::hash::Hash,
        I: IntoIterator<Item = S>,
        S: Into<String>,
        KF: Fn(&T) -> Option<K> + Send + Sync + 'static,
        WF: Fn(&T) -> usize + Send + Sync + 'static,
        H: Fn(PartitionRecord<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let event = event.into();
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        let mut seen = std::collections::HashSet::new();
        if topics.is_empty() {
            return Err(NasaRedisError::Config(
                "consumer plan topics cannot be empty".into(),
            ));
        }
        for topic in &topics {
            activation::validate_route(topic, &event)?;
            let idx = self.topic_to_group.get(topic).copied().unwrap_or(0);
            if !seen.insert(topic)
                || self.groups[idx]
                    .plans
                    .contains_key(&(topic.clone(), event.clone()))
            {
                return Err(NasaRedisError::Config(format!(
                    "partition route already registered: ({topic}, {event})"
                )));
            }
        }
        let id = self.reserve_plan_id()?;
        let plan = plan::ConsumerPlan::typed(id, key_fn, weight, handler);
        // 候选全集通过校验后才同步发布各路由，避免多主题计划只登记一部分。
        for topic in topics {
            let idx = self.topic_to_group.get(&topic).copied().unwrap_or(0);
            self.groups[idx]
                .plans
                .insert((topic, event.clone()), plan.clone());
        }
        Ok(self)
    }

    /// 业务作用：复验全部注册与远端 Stream/group 合同，在所有组监督任务就绪后一次性开放消费。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部组就绪时返回运行句柄；配置或远端合同失败时关闭未激活任务并返回错误。
    pub async fn start(self) -> Result<RunningPartition> {
        self.start_inner(true).await
    }

    /// 业务作用：完成全部来源和执行域准备，消费仍等待宿主的整体启动屏障。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已准备的运行句柄；调用 activate 前不抢占来源或执行 handler，失败沿启动 owner 清理。
    pub async fn start_suspended(self) -> Result<RunningPartition> {
        self.start_inner(false).await
    }

    /// 业务作用：按宿主是否持有后续屏障建立受监督执行域。
    ///
    /// 参数说明：
    /// - `activate`: 全部准备成功后是否释放统一启动屏障；false 时由宿主后续显式激活。
    ///
    /// 返回：注册、容量、远端消费组和执行器健康全部通过时返回运行态；任一失败保留原错误并撤销部分启动。
    /// 已有 Redis Stream/消费组不回滚；等待中被取消仍由启动守卫承担本地任务的停止责任。
    async fn start_inner(self, activate: bool) -> Result<RunningPartition> {
        let PreparedPartition {
            client,
            lock,
            node_id,
            groups,
            topic_to_group,
            registration_error,
            next_plan_id,
        } = self;
        if let Some(error) = registration_error {
            return Err(error);
        }
        activation::validate_routes(&groups)?;
        let mut activation = activation::PlanActivation::new();
        let config = client.config().partition.clone();
        for group in &groups {
            config.limits.validate(group.stream_cfg.batch_size)?;
        }
        let execution_plan = execution::ExecutionPlan::build(
            &groups,
            &config.executor,
            &config.limits,
            (next_plan_id - 1) as usize,
        )?;
        let core =
            runtime::engine::RedisPartitionRuntime::new(config.limits.clone(), execution_plan)?;
        // 首次可取消的启动等待前登记全部域，防止部分 Runner 启动后失去回滚 owner。
        activation.track_core(core.clone());
        if let Err(error) = core.start(activation.start_gate()).await {
            return Err(activation.rollback(error).await);
        }
        let publisher = publisher::PublisherCoordinator::new(config.limits.clone());
        let ids: Vec<String> = groups.iter().map(|g| g.id.clone()).collect();
        let mut runtimes: HashMap<String, Arc<runtime::GroupRuntime>> = HashMap::new();
        let mut inner: Option<Arc<runtime::GroupRuntime>> = None;
        for (i, g) in groups.into_iter().enumerate() {
            // 物理组分别持有来源控制，消费执行与记录责任共用同一个 core。
            let rt = match runtime::GroupRuntime::start(
                Arc::clone(&client),
                Arc::clone(&lock),
                g.layout,
                g.count,
                g.plans,
                core.clone(),
                publisher.clone(),
                node_id.clone(),
                g.cfg,
                g.stream_cfg,
                activation.start_gate(),
            )
            .await
            {
                Ok(rt) => rt,
                // 任一组启动失败都须回滚已登记的完整资源集合，避免部分 Runner 或组任务失去清理 owner。
                Err(e) => {
                    return Err(activation.rollback(e).await);
                }
            };
            if i == 0 {
                inner = Some(Arc::clone(&rt)); // [0] = 默认组(id="")
            }
            activation.track(Arc::clone(&rt));
            runtimes.insert(g.id, rt);
        }
        let topic_to_group: HashMap<String, String> = topic_to_group
            .into_iter()
            .map(|(t, idx)| (t, ids[idx].clone()))
            .collect();
        // prepare 与 start 之间远端资源可能变化；所有复验完成前禁止任一组抢锁或调用业务。
        for runtime in runtimes.values() {
            if let Err(error) = runtime.ensure_stream_contract().await {
                return Err(activation.rollback(error).await);
            }
        }
        if core
            .executions
            .domains
            .iter()
            .any(|domain| domain.runner.health() != napart::RunnerHealth::Healthy)
        {
            return Err(activation
                .rollback(NasaRedisError::Config(
                    "partition runner is not healthy at activation".into(),
                ))
                .await);
        }
        let running = RunningPartition {
            start_gate: activation.start_gate(),
            stop: shutdown::ShutdownOperation::new(
                core,
                publisher,
                runtimes.values().cloned().collect(),
            ),
            inner: inner.expect("默认组恒存在(groups[0])"),
            groups: runtimes,
            topic_to_group,
        };
        activation.commit(activate);
        Ok(running)
    }
}

impl RunningPartition {
    /// 业务作用：读取当前停机证明与未完成责任，不改变运行态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未开始停机或仍有在途责任时 converged 为 false。
    pub fn shutdown_report(&self) -> PartitionShutdownReport {
        self.stop.report()
    }

    /// 业务作用：在宿主全部启动门禁成功后一次性开放消费。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未关闭时开放全部组；已经请求停机时保持关闭，旧句柄不能复活消费。
    pub fn activate(&self) {
        if !self
            .stop
            .core
            .roots_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.start_gate.cancel();
        }
    }

    /// 业务作用：同步关闭发布与消费根准入，让多来源宿主先统一停止接纳再等待。
    ///
    /// 参数说明：无。
    ///
    /// 返回：唯一排干操作继续持有已接纳记录、续租和 Commit 责任。
    pub fn begin_shutdown(&self) {
        self.stop.begin();
    }

    /// 业务作用：让宿主依赖的释放晚于消费和持久提交的实际退出。
    ///
    /// 参数说明：
    /// - `guard`: 需要保持到停机实际收口的宿主依赖守卫，每个运行态最多登记一个。
    ///
    /// 返回：登记成功后由停机操作释放；已收口时立即释放；重复登记原样返回守卫。
    pub fn retain_shutdown_dependency<T: Send + 'static>(
        &self,
        guard: T,
    ) -> std::result::Result<(), T> {
        self.stop.retain_dependency(guard)
    }

    /// 业务作用：根据 topic 选择目标组 runtime，命中映射时走隔离组，否则走默认组。
    /// 路由仅按 **topic**,与 event 无关(同 Java)。
    ///
    /// 参数说明：
    /// - `topic`: stream/partition 使用的业务主题。
    ///
    /// 返回：借用匹配组或默认组的运行时，不验证主题是否合法，也不改变当前持有状态。
    fn rt_for_topic(&self, topic: &str) -> &Arc<runtime::GroupRuntime> {
        self.topic_to_group
            .get(topic)
            .and_then(|g| self.groups.get(g))
            .unwrap_or(&self.inner)
    }

    /// 业务作用：按**逻辑组 ID** 取 runtime(默认组用 `""` / [`DEFAULT_GROUP_ID`];group-scoped 管理 API 用)。
    /// 不存在 → `Config` 错误(防 operator 拼错组名静默作用到错组)。
    ///
    /// 参数说明：
    /// - `group`: 本地逻辑组 ID；空字符串表示默认组，其余为配置的隔离组短名。
    ///
    /// 返回：存在时借用对应运行时；未知组返回 Config，不退回默认组。
    fn rt_by_group(&self, group: &str) -> Result<&Arc<runtime::GroupRuntime>> {
        self.groups.get(group).ok_or_else(|| {
            NasaRedisError::Config(format!(
                "分区组 \"{group}\" 不存在(默认组用 \"\"=DEFAULT_GROUP_ID,隔离组用其逻辑名)"
            ))
        })
    }

    /// 业务作用：返回全部逻辑组当前仍待异步 XDEL 的 entry 总数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：各组已受理但尚未完成删除或转入留存的责任数，按饱和相加汇总，不是同一时刻的原子快照。
    /// 尚未交接的 Commit 不在此计数内；零值不证明 Stream 正文已全部删除，也不替代消费终态判断。
    pub fn async_delete_pending(&self) -> usize {
        self.groups.values().fold(0usize, |total, runtime| {
            total.saturating_add(runtime.async_delete_pending())
        })
    }

    /// 业务作用：按主题选择物理组，再以字符串业务键稳定路由并发布标准信封。
    ///
    /// 参数说明：
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `key`: 用于稳定路由到分区的字符串 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
    ///
    /// 返回：确认发布时返回 Stream entry ID；非法路由、准入或序列化失败不发送。
    /// 发送后失败返回 PublishOutcomeUnknown；取消等待不撤销已受理发布，相同键不等于发布幂等身份。
    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        key: &str,
        data: &T,
    ) -> Result<String> {
        let rt = self.rt_for_topic(topic);
        rt.publish(topic, event, route_str(key, rt.count), data, None)
            .await
    }

    /// 业务作用：在本实例选定组内轮转发布，无需提供业务键，不承诺同键顺序。
    ///
    /// 参数说明：
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
    ///
    /// 返回：确认发布时返回 entry ID；发送前错误不写入，发送后错误表示结果未知。
    /// 轮转计数按实例和组独立维护；取消等待不撤销已经受理的发布。
    pub async fn publish_round_robin<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> Result<String> {
        self.rt_for_topic(topic)
            .publish_round_robin(topic, event, data)
            .await
    }

    /// 业务作用：发布(i64 路由 key:uid/orderId 等数字标识,免 to_string)。按 topic 路由到所属组。
    ///
    /// 参数说明：
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `key`: 用于稳定路由到分区的整数 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
    ///
    /// 返回：确认发布时返回 entry ID；发送前校验、准入或序列化错误不写入，发送后错误表示结果未知。
    /// 数字哈希与数字文本哈希不同，调用方应固定同一业务键的表示方式；取消等待不撤销已受理发布。
    pub async fn publish_i64<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        key: i64,
        data: &T,
    ) -> Result<String> {
        let rt = self.rt_for_topic(topic);
        rt.publish(topic, event, route_i64(key, rt.count), data, None)
            .await
    }

    /// 业务作用：为需要忽略空路由的调用方提供可明确区分未发布结果的兼容入口。
    ///
    /// 参数说明：
    /// - `topic`: 业务 topic,为空时不发布。
    /// - `event`: topic 下的业务事件名,为空时不发布。
    /// - `key`: 用于稳定路由到分区的字符串 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
    ///
    /// 返回：topic 或 event 为空时记录告警并返回 None；其余沿标准发布路径校验，成功返回 Some(entry ID)。
    /// 非空但非法的路由仍返回错误，发送后的结果未知语义与 publish 一致。
    pub async fn publish_if_valid<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        key: &str,
        data: &T,
    ) -> Result<Option<String>> {
        // 空路由没有可投递目标，兼容跳过发生在序列化和发送之前，不伪造服务端发布成功。
        if topic.is_empty() || event.is_empty() {
            tracing::warn!(topic, event, "publish_if_valid: 入参无效,未发布");
            return Ok(None);
        }
        Ok(Some(self.publish(topic, event, key, data).await?))
    }

    /// 业务作用：读取本节点在默认组内的持有分区快照，供诊断和状态展示。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复制默认组的本地 claimed 列表，包含仍持锁排干的来源；不复验 Redis，不能授权后续副作用。
    pub async fn claimed_partitions(&self) -> Vec<u32> {
        self.inner.claimed_partitions().await
    }

    // ── 毒消息管理 API（单 owner 进程内直调，跨节点通过 command outbox 路由）──

    /// 业务作用：查询默认组分区的持久隔离批次，不要求当前节点持有来源。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：冻结或在途处置返回 park_id，未隔离或终态返回 None；记录异常及 Redis 失败返回错误。
    /// 这是查询时快照，不能替代后续动作的持有权及批次复验。
    pub async fn parked(&self, p: u32) -> Result<Option<String>> {
        self.inner.parked_id(p).await
    }

    /// 业务作用：以本节点持有凭据恢复默认组捕获的隔离批次，把消息用新 ID 重投到源 Stream。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：持久恢复完成时返回新 entry IDs；失权、批次冲突、结果不确定或 Redis 失败返回错误。
    /// 原顺序位置不可恢复；成功后发送本地恢复提示，返回不证明协调器已经重新开放消费。
    pub async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        self.inner.resume_parked(p).await
    }

    /// 业务作用：以本节点持有凭据永久丢弃默认组捕获的隔离批次，持久终态保留为 Dropped。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：持久清理完成时成功；失权、批次或操作冲突及 Redis 失败返回错误，通信失败不证明未执行。
    /// 持久成功后请求本地恢复；返回不证明协调器已处理提示，也不恢复已丢弃的正文。
    pub async fn drop_parked(&self, p: u32) -> Result<()> {
        self.inner.drop_parked(p).await
    }

    /// 业务作用：以本节点持有凭据把默认组捕获的隔离批次发布到 DLQ，持久终态为 Dlqed。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：确认完成时返回 DLQ entry IDs；失权、批次冲突、发布不确定或 Redis 失败返回错误。
    /// 本地恢复提示在持久成功后发送，返回不证明协调器已重新开放消费。
    pub async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        self.inner.dlq_parked(p).await
    }

    /// 业务作用：为默认组处于不确定恢复状态的隔离批次重新预约 ID 并强制发布。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：持久完成时返回新 entry IDs；状态不允许、失权、证据异常或副作用不确定时返回错误。
    /// 调用方明确接受既有发布可能已完成所带来的重复与变序；成功提示不证明本地消费已开放。
    pub async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        self.inner.force_republish(p).await
    }

    /// 业务作用：在调用方确认原操作不再推进后，强制接管默认组隔离批次的在途发布或丢弃动作。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    ///
    /// 返回：原子复验捕获的批次、状态、旧操作与持有权后继续至终态，发布动作返回新 entry IDs。
    /// 冲突、失权、证据不足或副作用不确定返回错误；接管不能撤销旧发布，调用方须接受可能重复或变序。
    /// 成功后请求协调器复验并恢复，返回不构成本地解除冻结的证明。
    pub async fn force_takeover(&self, p: u32) -> Result<Vec<String>> {
        self.inner.force_takeover(p).await
    }

    // ── 管理命令 outbox(跨节点管理路由;任意节点可提交,owner 执行)──

    /// 业务作用：提交管理命令(at-least-once:Unknown 后可同 op_id 重复提交,owner 按 result 去重)。
    /// deadline 以 Redis TIME 为基准(producer 传相对 timeout_ms,防节点时钟漂移)。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    /// - `operation_id`: 调用方提供的幂等操作 ID。
    /// - `action`: 要提交的管理动作。
    /// - `timeout`: 命令接纳超时窗口。
    ///
    /// 返回：提交或复用同一操作的持久结果，不表示动作已成功；分区越界、冲突、非法窗口或 Redis 失败返回错误。
    /// 结果未知时重试必须保留原操作身份与参数；提交方不必持有来源，实际处置由持有节点复验后执行。
    pub async fn submit_command(
        &self,
        p: u32,
        operation_id: &str,
        action: command::CmdAction,
        timeout: std::time::Duration,
    ) -> Result<command::CmdResult> {
        // 组外分区没有本运行时可派发的 owner，必须在创建结果和命令流之前拒绝，避免留下无法处理的请求。
        if p >= self.inner.count {
            return Err(NasaRedisError::Config(
                "partition command target is outside the default group".into(),
            ));
        }
        let timeout_ms = command_timeout_millis(timeout)?;
        command::submit(
            &self.inner.client,
            &self.inner.layout,
            p,
            operation_id,
            action,
            timeout_ms,
        )
        .await
    }

    /// 业务作用：查询命令结果(producer 轮询;None = 尚无记录)。
    ///
    /// 参数说明：
    /// - `p`: 默认组内的分区编号。
    /// - `operation_id`: 要查询的幂等操作 ID。
    ///
    /// 返回：当前结果记录或 None；记录解码及 Redis 失败返回错误。None 也可能表示已过期，不能证明动作从未执行。
    pub async fn command_result(
        &self,
        p: u32,
        operation_id: &str,
    ) -> Result<Option<command::CmdResult>> {
        command::query(&self.inner.client, &self.inner.layout, p, operation_id).await
    }

    // ── group-scoped 管理/诊断 API(多组:隔离组 Park 后的恢复入口)──
    // 上面无 group 参数的 API = 默认组(向后兼容单组);**隔离组**用下列 `*_in(group, ...)`,group = 逻辑组 ID
    // (默认组用 `""`=`DEFAULT_GROUP_ID`,隔离组用其逻辑名)。

    /// 业务作用：读取各组中本节点持有的分区，返回映射的 key 为逻辑组 ID
    /// (默认组 = `""`=`DEFAULT_GROUP_ID`,隔离组 = 逻辑名)。group-scoped 管理 API 的 `group` 参数用此 key。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：逐组复制本地持有列表，包含持锁排干来源；不是全组同一时刻的快照，不能替代服务端权威复验。
    pub async fn claimed_partitions_by_group(&self) -> HashMap<String, Vec<u32>> {
        let mut out = HashMap::new();
        for (id, rt) in &self.groups {
            out.insert(id.clone(), rt.claimed_partitions().await);
        }
        out
    }

    /// 业务作用：按组提供本节点持有分区的诊断视图，默认组显示为 `<default>`。
    /// **仅诊断/日志/actuator 用**;管理 API 入参仍用 `DEFAULT_GROUP_ID`(`""`),不要把 `<default>` 回传 `*_in`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以展示名为键的本地分区列表，不提供持有权授权；逻辑组恰为 `<default>` 时展示为 `group:<default>`，保留两组结果。
    pub async fn claimed_partitions_display(&self) -> HashMap<String, Vec<u32>> {
        self.claimed_partitions_by_group()
            .await
            .into_iter()
            .map(|(id, v)| {
                let k = if id == DEFAULT_GROUP_ID {
                    "<default>".to_string()
                } else if id == "<default>" {
                    // 默认组展示名不是逻辑身份；配置禁止逻辑组含冒号，此转义不会覆盖另一个合法组名。
                    format!("group:{id}")
                } else {
                    id
                };
                (k, v)
            })
            .collect()
    }

    /// 业务作用：查询指定逻辑组分区的持久隔离批次，不要求当前节点持有来源。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：冻结或在途处置返回 park_id，未隔离或终态返回 None；未知组、记录异常或 Redis 失败返回错误。
    /// 返回快照不授权后续处置，也不保证读取后批次仍然相同。
    pub async fn parked_in(&self, group: &str, p: u32) -> Result<Option<String>> {
        self.rt_by_group(group)?.parked_id(p).await
    }

    /// 业务作用：以本节点持有凭据恢复指定逻辑组捕获的隔离批次，将消息用新 ID 重投回源 Stream。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：持久完成时返回新 entry IDs；未知组、失权、批次冲突或副作用不确定返回错误。
    /// 原顺序位置不可恢复；成功后的本地恢复提示仍须协调器复验，返回不证明已开放消费。
    pub async fn resume_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.resume_parked(p).await
    }

    /// 业务作用：以本节点持有凭据永久丢弃指定逻辑组捕获的隔离批次。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：持久 Dropped 终态及清理完成时成功；未知组、失权、冲突或 Redis 失败返回错误。
    /// 通信错误不证明未执行，成功也不证明协调器已处理恢复提示；丢弃的正文不再保留用于恢复。
    pub async fn drop_parked_in(&self, group: &str, p: u32) -> Result<()> {
        self.rt_by_group(group)?.drop_parked(p).await
    }

    /// 业务作用：以本节点持有凭据将指定逻辑组捕获的隔离批次发布到该组 DLQ。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：持久完成时返回 DLQ entry IDs；未知组、失权、冲突或发布不确定返回错误。
    /// 成功后请求本地恢复，返回不证明协调器已重新开放消费。
    pub async fn dlq_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.dlq_parked(p).await
    }

    /// 业务作用：为指定逻辑组处于不确定恢复状态的隔离批次重新预约 ID 并强制发布。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：持久完成时返回新 entry IDs；未知组、状态或持有权不符及副作用不确定返回错误。
    /// 调用方接受先前发布可能已完成的重复与变序风险；返回成功不证明本地恢复提示已处理。
    pub async fn force_republish_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_republish(p).await
    }

    /// 业务作用：在调用方确认原操作不再推进后，强制接管指定逻辑组隔离批次的在途动作。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：复验捕获批次、状态、旧操作和持有权后继续，完成发布时返回新 entry IDs。
    /// 未知组、冲突、失权或副作用不确定返回错误；接管不撤销旧发布，也不消除重复与变序风险。
    pub async fn force_takeover_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_takeover(p).await
    }

    /// 业务作用：指定组提交管理命令(跨节点 outbox)。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    /// - `operation_id`: 调用方提供的幂等操作 ID。
    /// - `action`: 要提交的管理动作。
    /// - `timeout`: 命令接纳超时窗口。
    ///
    /// 返回：提交或复用同一操作的持久结果，不表示动作已经成功；未知组、分区越界、非法窗口、冲突或 Redis 失败返回错误。
    /// 提交方不必持有来源；未知结果重试须保留操作身份与参数，由实际持有节点复验并执行。
    pub async fn submit_command_in(
        &self,
        group: &str,
        p: u32,
        operation_id: &str,
        action: command::CmdAction,
        timeout: std::time::Duration,
    ) -> Result<command::CmdResult> {
        let rt = self.rt_by_group(group)?;
        // 以目标组自身的分区数判断，不借默认组容量接受隔离组外的命令；拒绝先于任何远端写入。
        if p >= rt.count {
            return Err(NasaRedisError::Config(
                "partition command target is outside the selected group".into(),
            ));
        }
        let timeout_ms = command_timeout_millis(timeout)?;
        command::submit(&rt.client, &rt.layout, p, operation_id, action, timeout_ms).await
    }

    /// 业务作用：指定组查询管理命令结果。
    ///
    /// 参数说明：
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    /// - `operation_id`: 要查询的幂等操作 ID。
    ///
    /// 返回：当前结果记录或 None；未知组、解码或 Redis 失败返回错误。结果可能已过期，None 不证明从未执行。
    pub async fn command_result_in(
        &self,
        group: &str,
        p: u32,
        operation_id: &str,
    ) -> Result<Option<command::CmdResult>> {
        let rt = self.rt_by_group(group)?;
        command::query(&rt.client, &rt.layout, p, operation_id).await
    }

    /// 业务作用：便利:按 topic 路由查其所属组的 Parked(operator 只知 topic 不知组时用)。
    ///
    /// 参数说明：
    /// - `topic`: 用于解析目标逻辑组的业务 topic。
    /// - `p`: 组内分区编号。
    ///
    /// 返回：冻结或在途处置返回 park_id，未隔离或终态返回 None；查询异常返回错误。
    /// 未映射主题使用默认组，结果只作快照，不授权处置或保证后续批次不变。
    pub async fn parked_for_topic(&self, topic: &str, p: u32) -> Result<Option<String>> {
        self.rt_for_topic(topic).parked_id(p).await
    }

    /// 业务作用：按默认组配置的等待窗口请求统一停机，关闭新准入并等待既有责任收口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已收敛或仍有剩余责任的停机报告；超时结束本次等待，后台排干与必要续租继续受监督。
    /// 默认组与隔离组由同一操作协调，持有句柄重复调用不会启动另一份停机。
    pub async fn shutdown(&self) -> PartitionShutdownReport {
        let timeout = std::time::Duration::from_millis(self.inner.cfg.drain_timeout_ms);
        self.shutdown_until(std::time::Instant::now() + timeout)
            .await
    }

    /// 业务作用：在调用方期限内等待同一停机操作，超时只返回未收敛报告。
    ///
    /// 参数说明：
    /// - `deadline`: 单调时钟上的绝对等待期限，已过期时也会先请求停机。
    ///
    /// 返回：已排干资源证明或剩余责任；后台排干与租约继续受监督。
    pub async fn shutdown_until(&self, deadline: std::time::Instant) -> PartitionShutdownReport {
        self.stop.wait(deadline).await
    }

    /// 业务作用：显式撤销来源权威并请求强制停止，未取得退出证明时不主动 unlock。
    ///
    /// 参数说明：
    /// - `deadline`: 单调时钟上的强停等待期限，超时不伪造已经完成退出的证明。
    ///
    /// 返回：forced 报告及本地执行不确定性。
    pub async fn force_shutdown_until(
        &self,
        deadline: std::time::Instant,
    ) -> PartitionShutdownReport {
        self.stop.force(deadline).await
    }

    /// 业务作用：取得全部消费组共用的资源、提交与就绪快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含业务键和记录 ID 的低基数快照。
    pub fn snapshot(&self) -> PartitionSnapshot {
        self.stop.core.snapshot()
    }

    /// 业务作用：取得发布票据、字节和未知结果的聚合快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前代理的发布容量与累计结果。
    pub fn publisher_snapshot(&self) -> PublisherSnapshot {
        self.stop.publisher.snapshot()
    }
}

impl Drop for RunningPartition {
    /// 业务作用：析构时请求同一停机操作，不将异步清理请求解释为退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 Tokio 环境存在时开始受监督排干；无环境时撤销本地权威。
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.stop.begin();
        } else {
            self.stop.core.revoke_all();
            for group in self.groups.values() {
                group.cancel_best_effort();
            }
        }
    }
}
