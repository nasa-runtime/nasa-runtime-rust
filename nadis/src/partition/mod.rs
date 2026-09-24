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
// 一、Java 兼容哈希(KeyLayoutV1 路由;混跑/读 Java 数据的前提)
//
// compat 前缀只表示"局部算法保持兼容":例如字符串 hash、整数 hash、浮点格式化。
// 它不等价于 CompatibilityProfile::LegacyV1,后者是整套 wire/锁/ACK/marker 运行模式。
// ─────────────────────────────────────────────────────────────────────────

/// 业务作用：Java `String.hashCode()` 逐位复刻:
/// h = `s[0]*31^(n-1) + ... + s[n-1]`,按 **UTF-16 code unit** 计算(不是字节、不是 char)。
/// wrapping 运算对齐 Java int 溢出语义。权威向量:"abc" → 96354。
///
/// # 参数
/// - `s`: 要按历史字符串哈希规则处理的文本。
pub fn compat_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(u as i32))
}

/// 业务作用：Java `Long.hashCode()`:`(int)(value ^ (value >>> 32))`(>>> 是无符号右移)。
///
/// # 参数
/// - `v`: 要按历史长整型哈希规则处理的整数。
pub fn compat_long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

/// 业务作用：路由:`(hash & Integer.MAX_VALUE) % count`(屏蔽符号位,Java RedisPartition 同款)。
///
/// # 参数
/// - `key`: 业务路由键文本。
/// - `count`: 分区总数。
pub fn route_str(key: &str, count: u32) -> u32 {
    ((compat_string_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 业务作用：按历史整数哈希规则把 i64 key 路由到分区。
///
/// # 参数
/// - `key`: 业务路由整数键。
/// - `count`: 分区总数。
pub fn route_i64(key: i64, count: u32) -> u32 {
    ((compat_long_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 业务作用：把管理命令的相对等待窗口转成协议毫秒值。
///
/// `Duration::as_millis()` 返回 `u128`；直接 `as u64` 会让极端输入回绕。管理命令还会把该值写入
/// Redis 持久记录，因此在发出任何副作用前按统一运行时上限拒绝。
fn command_timeout_millis(timeout: std::time::Duration) -> Result<u64> {
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
/// 参数说明：`v` 为待格式化浮点值，允许 NaN、无穷和带符号零。
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
// 二、KeyLayoutV1:全部分区相关 key 的唯一命名出处
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
    /// 业务作用：分区 stream:`{prefix}:{p}`(如 SINGLE-CONSUME:0..63)。**不含 profile**(跨 profile 单 owner 信任根)。
    ///
    /// # 参数
    /// - `p`: 分区编号。
    pub fn stream(&self, p: u32) -> String {
        format!("{}:{}", self.prefix, p)
    }

    /// 业务作用：分区锁的【业务 key】:`{prefix}:lock:{p}`。**不含 profile**(锁是分区归属最终仲裁,两 profile 抢同一把锁)。
    /// 注意:传给 DistributedLock 后会再叠锁前缀,最终 Redis key =
    /// `DISTRIBUTED-LOCK:{prefix}:lock:{p}` —— Java 双前缀逐字节一致。
    ///
    /// # 参数
    /// - `p`: 分区编号。
    pub fn lock_business_key(&self, p: u32) -> String {
        format!("{}:lock:{}", self.prefix, p)
    }

    /// 业务作用：活节点 ZSET。
    ///   · RustV2 → `{prefix}:nodes:v2`(Redis TIME 时基,独立心跳域);
    ///   · LegacyV1 → `{prefix}:nodes`(墙钟时基,**与 Java 节点互通**,不能加后缀)。
    /// 这样 RustV2 与墙钟节点(LegacyV1/Java)**物理隔离**,跨时基误驱逐(score 时基不一致驱逐存活异
    /// profile 节点 → 双 claim 窗口)从根上消除。lock/group/stream **一律不加后缀**(它们是跨 profile 单
    /// owner 的共同信任根,见上)。副作用仅 fair-share 失衡(混部期负载倾斜),非数据正确性问题——锁互斥
    /// + fence 仍保证每分区单 owner。与 `rebalance` 心跳时钟分流收敛到同一 `self.profile` 判定,避免漂移。
    pub fn nodes(&self) -> String {
        match self.profile {
            CompatibilityProfile::RustV2 => format!("{}:nodes:v2", self.prefix),
            _ => format!("{}:nodes", self.prefix),
        }
    }

    /// 业务作用：再平衡唤醒 Pub/Sub 通道:`{prefix}:wake`。
    pub fn wake(&self) -> String {
        format!("{}:wake", self.prefix)
    }

    /// 业务作用：consumer group 名 = prefix(Java:组名与 stream 前缀同名,全节点共用)。
    pub fn group(&self) -> &str {
        &self.prefix
    }
}

///业务作用：cluster 启动期纯本地同槽自检。对每个分区,断言所有参与多键 Lua 的 key
/// (stream/lock/fence/disposition marker/quarantine/retryop marker/cmd-stream/dlq/nodes/wake)
/// 与本分区 stream 落**同一 CRC16 slot**;不一致即 fail-closed(防 KeyLayout 命名漂移静默破裂同槽不变量)。
///
/// # 参数
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `lock_prefix`: 用于校验分区 key 同槽的锁 key 前缀。
/// - `count`: Redis 命令、分页或批处理使用的数量上限。
fn assert_same_slot_cluster(layout: &KeyLayout, lock_prefix: &str, count: u32) -> Result<()> {
    use crate::keytag::redis_slot;
    // group 为空 → prefix=`{}` 空 tag → 整 key 参与哈希、各 key 不同 slot(同源)。
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

/// 业务作用：为一个组(默认或隔离)准备 KeyLayout + 建 stream/consumer group。`base` = 未包裹的前缀基名
/// (默认组 = `default_group`;隔离组 = `default_group:逻辑名`)。cluster 下包成 `{base}` hash-tag,
/// 使本组全部 key 同 slot(避多键 Lua CROSSSLOT);不同组 → 不同 slot(天然分散到不同 master)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `base`: 归档、配置或路径拼接使用的基础名称。
/// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    // cluster 启动期纯本地 CRC16 同槽自检(零 RTT,fail-closed)。
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
            Ok(_) => {}
            Err(e) if e.to_string().contains("BUSYGROUP") => {}
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

/// 业务作用：反序列化:**显式 `null` 或字段缺省 → `T::default()`**(
/// "任意语言标准 JSON 互通 + 忽略 null 的反序列化")。配 `#[serde(default)]` 一起用——`#[serde(default)]`
/// **只覆盖字段缺省**(missing),本函数额外把**显式 `null`** 当默认值(很多语言/库对空字段默认写 `null`,
/// 若不容忍则消费端 `from_slice::<Envelope>` 解码失败 → 消息进毒,无法消费)。
///
/// # 参数
/// - `deserializer`: serde 提供的信封字段反序列化器。
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
// 四、handler 注册形态
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
// 五、typestate 三阶段(:register 只在 Prepared;start(self) 消费进 Running)
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

/// 保存已解析的分片路由结果；用于后续命令直接选择目标连接。
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

/// 阶段二:运行中(coordinator/再平衡/心跳已启动)。只可 publish 与 shutdown。
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
    /// 业务作用：prepare:为每个分区建 stream + consumer group(MKSTREAM;BUSYGROUP 容忍=幂等),
    /// 不 tryLock、不启动任何消费(三阶段第一步)。
    ///
    /// # 参数
    /// - `client`: 分区框架使用的 Redis 客户端。
    /// - `lock`: 分区 owner 竞争使用的分布式锁组件。
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
    /// 参数说明: 无。
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
    /// 参数说明：`topics` 为主题集合；`event` 为事件；`key_fn` 提取可选键；`handler` 接收单条已解码记录。
    /// 返回：全部路由合法且无冲突时原子登记计划；失败不改变已有路由。
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
    /// 参数说明：`topics`、`event` 定义路由；`key_fn` 提取业务键；`weight` 估算 T 的额外堆分配；`handler` 执行业务。
    /// 返回：路由集合一次性登记；空集合、重复路由或身份耗尽返回配置错误。
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
    /// 参数说明：无。
    /// 返回：已准备的运行句柄；调用 activate 前不抢占来源或执行 handler，失败沿启动 owner 清理。
    pub async fn start_suspended(self) -> Result<RunningPartition> {
        self.start_inner(false).await
    }

    /// 业务作用：按宿主是否持有后续屏障建立受监督执行域。
    /// 参数说明：`activate` 决定本次准备成功后是否立即开放消费。
    /// 返回：全部合同通过后返回运行态；任一失败保留原错误并撤销部分准备。
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
                g.cfg,        // per-group resolved PartitionCfg
                g.stream_cfg, // per-group resolved StreamCfg
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
    /// 参数说明：无。
    /// 返回：未开始停机或仍有在途责任时 converged 为 false。
    pub fn shutdown_report(&self) -> PartitionShutdownReport {
        self.stop.report()
    }

    /// 业务作用：在宿主全部启动门禁成功后一次性开放消费。
    /// 参数说明：无。
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
    /// 参数说明：无。
    /// 返回：唯一排干操作继续持有已接纳记录、续租和 Commit 责任。
    pub fn begin_shutdown(&self) {
        self.stop.begin();
    }

    /// 业务作用：让宿主依赖的释放晚于消费和持久提交的实际退出。
    /// 参数说明：`guard` 是依赖所有权守卫；每个运行态只允许登记一个宿主守卫。
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
    /// # 参数
    /// - `topic`: stream/partition 使用的业务主题。
    fn rt_for_topic(&self, topic: &str) -> &Arc<runtime::GroupRuntime> {
        self.topic_to_group
            .get(topic)
            .and_then(|g| self.groups.get(g))
            .unwrap_or(&self.inner)
    }

    /// 业务作用：按**逻辑组 ID** 取 runtime(默认组用 `""` / [`DEFAULT_GROUP_ID`];group-scoped 管理 API 用)。
    /// 不存在 → `Config` 错误(防 operator 拼错组名静默作用到错组)。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    fn rt_by_group(&self, group: &str) -> Result<&Arc<runtime::GroupRuntime>> {
        self.groups.get(group).ok_or_else(|| {
            NasaRedisError::Config(format!(
                "分区组 \"{group}\" 不存在(默认组用 \"\"=DEFAULT_GROUP_ID,隔离组用其逻辑名)"
            ))
        })
    }

    /// 业务作用：返回全部逻辑组当前仍待异步 XDEL 的 entry 总数。
    ///
    /// 该值是运行时观测 gauge，不参与 ACK、重试或消费终态判断。持续大于零表示 Redis 删除
    /// 未收敛；达到内部有界积压后，消费会被反压，避免已 ACK 的待删 ID 被静默丢弃。
    pub fn async_delete_pending(&self) -> usize {
        self.groups.values().fold(0usize, |total, runtime| {
            total.saturating_add(runtime.async_delete_pending())
        })
    }

    /// 业务作用：发布(严格守门):空 topic/event → Err(InvalidPublish 语义,用 Config 错误域)。
    /// 返回写入的 stream entry ID。**按 topic 路由到所属组**(隔离组或默认组),分区数用该组的 `count`。
    ///
    /// # 参数
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `key`: 用于稳定路由到分区的字符串 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
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

    /// 业务作用：**round-robin 发布**：无路由 key 时全局轮转均摊到各分区。
    /// 适合无需同 key 顺序保证、只要均摊吞吐的场景;需同 key 顺序请用 `publish`/`publish_i64`。
    ///
    /// # 参数
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
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
    /// # 参数
    /// - `topic`: 业务 topic,同时决定进入默认组还是隔离组。
    /// - `event`: topic 下的业务事件名。
    /// - `key`: 用于稳定路由到分区的整数 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
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

    /// 业务作用：兼容入口:入参无效返回 Ok(None) + 计数,**名字明示可能不发布**——
    /// 给依赖 Java 静默语义的迁移业务;新代码一律用 publish()。
    ///
    /// # 参数
    /// - `topic`: 业务 topic,为空时不发布。
    /// - `event`: topic 下的业务事件名,为空时不发布。
    /// - `key`: 用于稳定路由到分区的字符串 key。
    /// - `data`: 要序列化进 stream envelope 的业务数据。
    pub async fn publish_if_valid<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        key: &str,
        data: &T,
    ) -> Result<Option<String>> {
        if topic.is_empty() || event.is_empty() {
            tracing::warn!(topic, event, "publish_if_valid: 入参无效,未发布");
            return Ok(None);
        }
        Ok(Some(self.publish(topic, event, key, data).await?))
    }

    /// 业务作用：读取本节点当前持有的分区，用于诊断与运行状态展示。
    pub async fn claimed_partitions(&self) -> Vec<u32> {
        self.inner.claimed_partitions().await
    }

    // ── 毒消息管理 API（单 owner 进程内直调，跨节点通过 command outbox 路由）──

    /// 业务作用：查询分区是否 Parked(返回 park_id;诊断/运维入口)。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn parked(&self, p: u32) -> Result<Option<String>> {
        self.inner.parked_id(p).await
    }

    /// 业务作用：resume:把 Park 的消息重投回源 stream(以新 ID;顺序位置不可恢复—— 如实声明),
    /// 分区恢复消费。返回新 entry IDs。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        self.inner.resume_parked(p).await
    }

    /// 业务作用：drop:放弃 Park 的消息(quarantine 删除,marker 终态 Dropped 留审计),分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn drop_parked(&self, p: u32) -> Result<()> {
        self.inner.drop_parked(p).await
    }

    /// 业务作用：dlq:把 Parked 消息按 planned-ID 协议发布到 DLQ stream({prefix}:dlq),
    /// marker 终态 Dlqed,分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        // owner-fenced:经 inner 校验本节点是 owner 后再处置 + 通知解除 Parked
        self.inner.dlq_parked(p).await
    }

    /// 业务作用：force:resume 进入 ResumeIndeterminate 后的唯一推进入口——
    /// 为不确定的 reservation 分配全新 planned ID 重发,**显式接受可能重复/变序**,
    /// 审计链(新旧 planned 对照)写入 marker。成功后分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        self.inner.force_republish(p).await
    }

    /// 业务作用：**通用 liveness-takeover**：operator 确认原 op 已死后，强制接管某分区卡死的在途
    /// `*Publishing`/`Dropping`(原 op 永久丢失、无法自行重提 → 普通续作 `OperationConflict` 推不动)。
    /// 覆写 transition op 为本节点稳定 op 后续作到终态,分区恢复消费。⚠ **显式接受"可能与原 op 重复推进"
    /// 风险**(取向同 `force_republish`);仅在确认原 op 进程已死时使用。返回续作产出的新 entry IDs。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn force_takeover(&self, p: u32) -> Result<Vec<String>> {
        self.inner.force_takeover(p).await
    }

    // ── 管理命令 outbox(跨节点管理路由;任意节点可提交,owner 执行)──

    /// 业务作用：提交管理命令(at-least-once:Unknown 后可同 op_id 重复提交,owner 按 result 去重)。
    /// deadline 以 Redis TIME 为基准(producer 传相对 timeout_ms,防节点时钟漂移)。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    /// - `operation_id`: 调用方提供的幂等操作 ID。
    /// - `action`: 要提交的管理动作。
    /// - `timeout`: 命令接纳超时窗口。
    pub async fn submit_command(
        &self,
        p: u32,
        operation_id: &str,
        action: command::CmdAction,
        timeout: std::time::Duration,
    ) -> Result<command::CmdResult> {
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
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    /// - `operation_id`: 要查询的幂等操作 ID。
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
    pub async fn claimed_partitions_by_group(&self) -> HashMap<String, Vec<u32>> {
        let mut out = HashMap::new();
        for (id, rt) in &self.groups {
            out.insert(id.clone(), rt.claimed_partitions().await);
        }
        out
    }

    /// 业务作用：按组提供本节点持有分区的诊断视图，默认组显示为 `<default>`。
    /// **仅诊断/日志/actuator 用**;管理 API 入参仍用 `DEFAULT_GROUP_ID`(`""`),不要把 `<default>` 回传 `*_in`。
    pub async fn claimed_partitions_display(&self) -> HashMap<String, Vec<u32>> {
        self.claimed_partitions_by_group()
            .await
            .into_iter()
            .map(|(id, v)| {
                let k = if id == DEFAULT_GROUP_ID {
                    "<default>".to_string()
                } else {
                    id
                };
                (k, v)
            })
            .collect()
    }

    /// 业务作用：指定组某分区是否 Parked(返回 park_id)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn parked_in(&self, group: &str, p: u32) -> Result<Option<String>> {
        self.rt_by_group(group)?.parked_id(p).await
    }

    /// 业务作用：指定组 resume:Park 消息重投回源 stream,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn resume_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.resume_parked(p).await
    }

    /// 业务作用：指定组 drop:放弃 Park 消息,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn drop_parked_in(&self, group: &str, p: u32) -> Result<()> {
        self.rt_by_group(group)?.drop_parked(p).await
    }

    /// 业务作用：指定组 dlq:Park 消息发布到 DLQ,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn dlq_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.dlq_parked(p).await
    }

    /// 业务作用：指定组 force_republish(ResumeIndeterminate 推进)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn force_republish_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_republish(p).await
    }

    /// 业务作用：指定组 force_takeover(liveness-takeover)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn force_takeover_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_takeover(p).await
    }

    /// 业务作用：指定组提交管理命令(跨节点 outbox)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    /// - `operation_id`: 调用方提供的幂等操作 ID。
    /// - `action`: 要提交的管理动作。
    /// - `timeout`: 命令接纳超时窗口。
    pub async fn submit_command_in(
        &self,
        group: &str,
        p: u32,
        operation_id: &str,
        action: command::CmdAction,
        timeout: std::time::Duration,
    ) -> Result<command::CmdResult> {
        let rt = self.rt_by_group(group)?;
        let timeout_ms = command_timeout_millis(timeout)?;
        command::submit(&rt.client, &rt.layout, p, operation_id, action, timeout_ms).await
    }

    /// 业务作用：指定组查询管理命令结果。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    /// - `operation_id`: 要查询的幂等操作 ID。
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
    /// # 参数
    /// - `topic`: 用于解析目标逻辑组的业务 topic。
    /// - `p`: 组内分区编号。
    pub async fn parked_for_topic(&self, topic: &str, p: u32) -> Result<Option<String>> {
        self.rt_for_topic(topic).parked_id(p).await
    }

    /// 业务作用：优雅停机:**所有组并发停机**(关 admission → 等 in-flight → 逐分区
    /// 释锁 → 摘心跳 → pub wake)。`inner` 即 `groups[默认组]`(同一 Arc),由 `groups` 统一停一次。
    pub async fn shutdown(&self) -> PartitionShutdownReport {
        let timeout = std::time::Duration::from_millis(self.inner.cfg.drain_timeout_ms);
        self.shutdown_until(std::time::Instant::now() + timeout)
            .await
    }

    /// 业务作用：在调用方期限内等待同一停机操作，超时只返回未收敛报告。
    /// 参数说明：`deadline` 为绝对等待期限。
    /// 返回：已排干资源证明或剩余责任；后台排干与租约继续受监督。
    pub async fn shutdown_until(&self, deadline: std::time::Instant) -> PartitionShutdownReport {
        self.stop.wait(deadline).await
    }

    /// 业务作用：显式撤销来源权威并请求强制停止，未取得退出证明时不主动 unlock。
    /// 参数说明：`deadline` 为强停等待期限。
    /// 返回：forced 报告及本地执行不确定性。
    pub async fn force_shutdown_until(
        &self,
        deadline: std::time::Instant,
    ) -> PartitionShutdownReport {
        self.stop.force(deadline).await
    }

    /// 业务作用：取得全部消费组共用的资源、提交与就绪快照。
    /// 参数说明: 无。
    /// 返回：不含业务键和记录 ID 的低基数快照。
    pub fn snapshot(&self) -> PartitionSnapshot {
        self.stop.core.snapshot()
    }

    /// 业务作用：取得发布票据、字节和未知结果的聚合快照。
    /// 参数说明: 无。
    /// 返回：当前代理的发布容量与累计结果。
    pub fn publisher_snapshot(&self) -> PublisherSnapshot {
        self.stop.publisher.snapshot()
    }
}

impl Drop for RunningPartition {
    /// 业务作用：析构时请求同一停机操作，不将异步清理请求解释为退出证明。
    /// 参数说明: 无。
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
