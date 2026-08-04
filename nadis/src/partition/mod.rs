// ============================================================================
// src/partition/mod.rs —— Kafka 式 Stream 分区框架:路由 / Key 布局 / 发布 / typestate。
// 对齐既有 RedisPartition 与 BatchStreamMessageListenerContainer 的公开语义。
//
// 当前实现边界：
//   · 兼容路由、三阶段 typestate、串行 worker、全局在飞预算、PEL 重投与再平衡已接线；
//   · disposition/retry-op、Park/DLQ/ForceRepublish、管理命令、V2 FencingStamp、
//     bootstrap owner 接管、墓碑 ACK、autoTrim 与 ACK 后异步 XDEL 已接线；
//   · Cluster 为保证所有多 key Lua 同槽，按“分区组”使用一个 hash tag：同组全部分区固定到
//     一个 slot，不是每分区一个 slot。需要跨 master 扩吞吐时，应拆成多个隔离组；
//   · 尚未提供单组跨 slot PollDomain 分片或独立 GroupConsumer lease 抽象。
// ============================================================================

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
// 一、原实现 兼容哈希(KeyLayoutV1 路由;混跑/读 原实现 数据的前提)
//
// compat 前缀只表示"局部算法保持兼容":例如字符串 hash、整数 hash、浮点格式化。
// 它不等价于 CompatibilityProfile::LegacyV1,后者是整套 wire/锁/ACK/marker 运行模式。
// ─────────────────────────────────────────────────────────────────────────

/// 原实现 `String.hashCode()` 逐位复刻:
/// h = `s[0]*31^(n-1) + ... + s[n-1]`,按 **UTF-16 code unit** 计算(不是字节、不是 char)。
/// wrapping 运算对齐 原实现 int 溢出语义。权威向量:"abc" → 96354。
///
/// # 参数
/// - `s`: 要按历史字符串哈希规则处理的文本。
pub fn compat_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(u as i32))
}

/// 原实现 `Long.hashCode()`:`(int)(value ^ (value >>> 32))`(>>> 是无符号右移)。
///
/// # 参数
/// - `v`: 要按历史长整型哈希规则处理的整数。
pub fn compat_long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

/// 路由:`(hash & Integer.MAX_VALUE) % count`(屏蔽符号位,原实现 RedisPartition 同款)。
///
/// # 参数
/// - `key`: 业务路由键文本。
/// - `count`: 分区总数。
pub fn route_str(key: &str, count: u32) -> u32 {
    ((compat_string_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 按历史整数哈希规则把 i64 key 路由到分区。
///
/// # 参数
/// - `key`: 业务路由整数键。
/// - `count`: 分区总数。
pub fn route_i64(key: i64, count: u32) -> u32 {
    ((compat_long_hash(key) & 0x7FFF_FFFF) as u32) % count
}

/// 把管理命令的相对等待窗口转成协议毫秒值。
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

/// **逐位复刻既有系统的 `Double.toString`，保证浮点值跨语言格式一致**。
/// Rust `f64::to_string()` 与 原实现 系统性分叉:整数值无 `.0`(`1.0`→"1" vs 原实现 "1.0")、科学计数法阈值
/// 与写法不同(`1e10`→"10000000000" vs 原实现 "1.0E10")。浮点作 @JsonArrayKey/id/bucket subId 或 HASH 字段
/// 存储时,格式不一致 = 跨语言 key/值字节分叉、数据不通。本函数对齐 原实现 规则:
///   · `m ∈ [10⁻³, 10⁷)` → 十进制(整数值补 `.0`);否则 → 科学计数法 `d.dddE±exp`(大写 E);
///   · 有效数字取**最短可往返**表示(Rust `{:e}` 与 原实现 FloatingDecimal 对绝大多数值一致)。
/// ⚠ 残留:极少数"最短表示"在 Rust/原实现 间选位不同的边界值仍可能 1 位之差(深层 dtoa 差异),
/// 典型业务值(价格/数量)一致;JSON 模式由 serde_json 自有格式,不经本函数。
///
/// # 参数
/// - `v`: 要按历史浮点文本格式化规则输出的值。
pub fn compat_double_to_string(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
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
        let frac = if digits.len() > 1 { &digits[1..] } else { "0" };
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
    /// stream 前缀,同时是 consumer group 名(原实现 defaultGroup 语义)。
    pub prefix: String,
    /// 本节点 profile(**仅** nodes ZSET key 按 profile 隔离,见 `nodes()`)。
    pub profile: CompatibilityProfile,
}

impl KeyLayout {
    /// 分区 stream:`{prefix}:{p}`(如 SINGLE-CONSUME:0..63)。**不含 profile**(跨 profile 单 owner 信任根)。
    ///
    /// # 参数
    /// - `p`: 分区编号。
    pub fn stream(&self, p: u32) -> String {
        format!("{}:{}", self.prefix, p)
    }

    /// 分区锁的【业务 key】:`{prefix}:lock:{p}`。**不含 profile**(锁是分区归属最终仲裁,两 profile 抢同一把锁)。
    /// 注意:传给 DistributedLock 后会再叠锁前缀,最终 Redis key =
    /// `DISTRIBUTED-LOCK:{prefix}:lock:{p}` —— 原实现 双前缀逐字节一致。
    ///
    /// # 参数
    /// - `p`: 分区编号。
    pub fn lock_business_key(&self, p: u32) -> String {
        format!("{}:lock:{}", self.prefix, p)
    }

    /// 活节点 ZSET。
    ///   · RustV2 → `{prefix}:nodes:v2`(Redis TIME 时基,独立心跳域);
    ///   · 原实现V1 → `{prefix}:nodes`(墙钟时基,**与 原实现 节点互通**,不能加后缀)。
    /// 这样 RustV2 与墙钟节点(原实现V1/真 原实现)**物理隔离**,跨时基误驱逐(score 时基不一致驱逐存活异
    /// profile 节点 → 双 claim 窗口)从根上消除。lock/group/stream **一律不加后缀**(它们是跨 profile 单
    /// owner 的共同信任根,见上)。副作用仅 fair-share 失衡(混部期负载倾斜),非数据正确性问题——锁互斥
    /// + fence 仍保证每分区单 owner。与 `rebalance` 心跳时钟分流收敛到同一 `self.profile` 判定,避免漂移。
    pub fn nodes(&self) -> String {
        match self.profile {
            CompatibilityProfile::RustV2 => format!("{}:nodes:v2", self.prefix),
            _ => format!("{}:nodes", self.prefix),
        }
    }

    /// 再平衡唤醒 Pub/Sub 通道:`{prefix}:wake`。
    pub fn wake(&self) -> String {
        format!("{}:wake", self.prefix)
    }

    /// consumer group 名 = prefix(原实现:组名与 stream 前缀同名,全节点共用)。
    pub fn group(&self) -> &str {
        &self.prefix
    }
}

///cluster 启动期纯本地同槽自检。对每个分区,断言所有参与多键 Lua 的 key
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

/// 为一个组(默认或隔离)准备 KeyLayout + 建 stream/consumer group。`base` = 未包裹的前缀基名
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
// 三、事件信封(`Envelope` = **标准 serde JSON**,原实现↔Rust 双向互通的线格式)。
// 产品决定(2026-06-14):**只支持标准 JSON,不支持 Jackson default-typing=true**(那套
// `["类名",{}]` 多态字节 codec = codec-jackson-compat,已撤销不交付)。原实现 端关闭
// default-typing(标准 JSON)即可:原实现 写 → Rust 消费、Rust 写 → 原实现 消费,均走本 Envelope。
// 原实现V1 / RustV2 两 profile 的 partition **都用这同一套标准 JSON Envelope**(profile 只影响
// fencing 强弱,不影响线格式),故跨语言 stream 互通两 profile 皆可(RustV2 额外有 V2 fence,
// 对 原实现 不可见)。
// ─────────────────────────────────────────────────────────────────────────

/// 反序列化:**显式 `null` 或字段缺省 → `T::default()`**(
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

/// stream 消息体(写入 entry 的 `data` field;对照 原实现 `PooledEvtData` 四字段,字段名对齐)。
/// **标准 serde JSON**——任意语言(原实现 default-typing=false 等)写出的 `{topic,event,data,passthrough}`
/// 与本结构逐字段互通。**反序列化忽略 null / 容忍缺省**:topic/event 缺省或 null → `""`、data 缺省或
/// null → `Value::Null`、passthrough 缺省或 null → `None`(Option 原生),故"原实现 不传 data / 写 topic:null"
/// 等不再解码失败进毒。序列化仍输出纯标准 JSON(passthrough=None 时省略)。
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
    /// 透传上下文(traceId 等)。**产品决定(2026-06-14):Rust 不做 原实现 的 thread-local/MDC 隐式
    /// 透传**——需要传上下文就**显式**放进业务的 `data` 或显式参数。本字段仅保留 **wire 兼容**(与
    /// 原实现 标准 JSON envelope 互通),框架**不自动捕获/注入**。
    /// Option 原生:缺省/null → None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough: Option<serde_json::Map<String, serde_json::Value>>,
}

/// stream entry 中装信封的 field 名(原实现 DATA_FIELD 同款)。
pub const DATA_FIELD: &str = "data";

// ─────────────────────────────────────────────────────────────────────────
// 四、handler 注册形态
// ─────────────────────────────────────────────────────────────────────────

/// 类型擦除后的批量 handler:输入 = 一批 `(entry_id, data JSON 值)`。
/// 返回**失败 ID 子集**(业务类型 `T` 解码也要逐 ID,坏 data 只让该 ID 进
/// failed;handler 失败只影响实际传入它的 ID)。空返回 = 全部成功。
///   · 每条 value 单独 `from_value::<T>`,坏的进 failed,不进 handler;
///   · handler 只收成功解码的 `Vec<T>`;handler Err → 这批传入的 ID 全进 failed;
///   · handler panic 由本闭包内 catch_unwind 归约为"这批传入的 ID 全 failed",不外溢。
pub type ErasedHandler = Arc<
    dyn Fn(Vec<(String, serde_json::Value)>) -> futures::future::BoxFuture<'static, Vec<String>>
        + Send
        + Sync,
>;

/// (topic, event) → handler 的路由表(prepare 阶段填充,start 后只读)。
pub type HandlerMap = HashMap<(String, String), ErasedHandler>;

// ─────────────────────────────────────────────────────────────────────────
// 五、typestate 三阶段(:register 只在 Prepared;start(self) 消费进 Running)
// ─────────────────────────────────────────────────────────────────────────

/// 阶段一:已 prepare(stream/consumer group 已建,**未抢任何锁**——
/// 原实现 注释原文:"避免 listener 还没 register 完就启 task 拉消息找不到 listener 而丢消息")。
/// 默认组的**逻辑 ID**(map key sentinel,对照 原实现 `DEFAULT_GROUP_NAME=""`)。group-scoped 管理 API
/// 用它定位默认组;隔离组用其逻辑短名。**与 Redis stream prefix 解耦**——默认组 prefix=`default_group`,
/// 但 map key 恒为 `""`,从而 `default_group` 与某隔离组逻辑名相同也不会撞 key。
pub const DEFAULT_GROUP_ID: &str = "";

/// 一个已 prepare 的分区组(默认组或隔离组):stream/consumer group 已建,handler 待注册。
/// 携带本组 **resolved** 配置(per-group 覆盖 → 父级/全局),start 时移交各自 `GroupRuntime`。
struct PreparedGroup {
    /// **逻辑 ID**(map key;默认组 = `""` sentinel,隔离组 = 逻辑短名)。与 Redis prefix 解耦,防撞 key。
    id: String,
    layout: KeyLayout,
    count: u32,
    handlers: HandlerMap,
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
    /// **多组**(对照 原实现 `RedisPartition.groups`):`[0]` = 默认组(id=`""`),其余 = 隔离组。
    groups: Vec<PreparedGroup>,
    /// topic → `groups` 下标(未命中 = 默认组 `[0]`;对照 原实现 `topicToGroupName`)。
    topic_to_group: HashMap<String, usize>,
}

/// 阶段二:运行中(coordinator/再平衡/心跳已启动)。只可 publish 与 shutdown。
pub struct RunningPartition {
    /// **默认组** runtime(= `groups[""]`;无 group 参数的管理 API 默认作用它,向后兼容单组语义)。
    inner: Arc<runtime::GroupRuntime>,
    /// 全部组(含默认组)runtime:**逻辑 ID** → runtime(默认组 key=`""`,**不会与隔离组逻辑名撞**)。
    groups: HashMap<String, Arc<runtime::GroupRuntime>>,
    /// topic → **逻辑 ID**(未命中 = 默认组,走 `inner`;对照 原实现 `resolveStream` 路由)。
    topic_to_group: HashMap<String, String>,
}

impl PreparedPartition {
    /// prepare:为每个分区建 stream + consumer group(MKSTREAM;BUSYGROUP 容忍=幂等),
    /// 不 tryLock、不启动任何消费(三阶段第一步)。
    ///
    /// # 参数
    /// - `client`: 分区框架使用的 Redis 客户端。
    /// - `lock`: 分区 owner 竞争使用的分布式锁组件。
    pub async fn prepare(client: Arc<RedisClient>, lock: Arc<DistributedLock>) -> Result<Self> {
        // 产品决定(2026-06-14):partition 的线格式是**标准 serde JSON**(`Envelope`),与 原实现
        // (default-typing=false 标准 JSON)双向互通——原实现 写 Rust 消费、Rust 写 原实现 消费均可。
        // 故 原实现V1 / RustV2 **两 profile 的 partition 都支持**(不再因缺少 jackson-compat 而拒绝
        // 原实现V1);profile 仅影响 fencing:RustV2 用 V2 fence stamp(更强,对 原实现 不可见),原实现V1
        // 用 V1 holds 双检查 + 裸 XACK(与 原实现 锁层一致)。**不支持的只有 Jackson default-typing=true**
        // 那套多态字节(已声明)。⚠ 跨语言共享 stream 前应做 golden-bytes 验证 原实现 标准 JSON
        // envelope 字段与本 `Envelope` 对齐(topic/event/data/passthrough + `data` field 名)。
        let cfg = client.config().partition.clone();
        // enabled 接线(此前 enabled=false 仍创建 stream/group
        // ——"配置看起来有效、实际完全不生效"比不提供更危险)。enabled 是总开关
        // (对齐 原实现 partition.enabled),false 时 prepare fail-fast,绝不静默建组。
        if !cfg.enabled {
            return Err(NasaRedisError::Config(
                "partition.enabled=false:分区设施未启用。需启用请显式设 \
                 cfg.partition.enabled=true(对齐 Legacy 总开关语义)"
                    .into(),
            ));
        }
        // count 防御(已在 RedisConfig::validate 于 connect 时 fail-fast;此处再挡一次,
        // 防 connect 后改配置的路径)。
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
        // ── 多组准备(对照 原实现 RedisPartition.groups):默认组 + 各隔离组 ──
        // 每组各自 build_group_layout(cluster `{base}` hash-tag relayout + 同槽自检 + XGROUP CREATE)。
        // 隔离组 base = `{default_group}:{逻辑名}`(原实现 streamPrefix),与默认组互不阻塞、cluster 下各自
        // 一个 slot(高频/低频隔离 + 天然分散到不同 master)。
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
            handlers: HashMap::new(),
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
            let base = format!("{}:{}", cfg.default_group, logical); // 原实现 streamPrefix = defaultGroup:groupName
            let layout = build_group_layout(&client, &base, gcount).await?;
            let idx = groups.len();
            // topic 路由:空 topics → 逻辑名本身即唯一 topic(对照 原实现)
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
                handlers: HashMap::new(),
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
            // node_id:进程级唯一(对照 原实现 nodeId;全组共用), incarnation 要求
            node_id: format!("n-{}", uuid::Uuid::new_v4().simple()),
            groups,
            topic_to_group,
        })
    }

    /// 注册 handler(仅 Prepared 阶段;Running 后路由表只读——typestate 保证)。
    /// T = 业务消息类型,注册时单态化反序列化(运行期无反射)。
    ///
    /// # 参数
    /// - `topic`: 要绑定的业务 topic。
    /// - `event`: 要绑定的业务事件名。
    /// - `f`: 批量处理该 topic/event 的异步 handler。
    pub fn register<T, F, Fut>(&mut self, topic: &str, event: &str, f: F) -> &mut Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: ErasedHandler = Arc::new(move |items: Vec<(String, serde_json::Value)>| {
            let f = Arc::clone(&f);
            Box::pin(async move {
                //**逐 ID 反序列化为 T**——坏 data 只让该 ID 进 failed,
                // 不连坐整桶;handler 只收成功解码的 Vec<T>。
                let mut typed: Vec<T> = Vec::with_capacity(items.len());
                let mut good_ids: Vec<String> = Vec::with_capacity(items.len());
                let mut failed: Vec<String> = Vec::new();
                for (id, v) in items {
                    match serde_json::from_value::<T>(v) {
                        Ok(t) => {
                            typed.push(t);
                            good_ids.push(id);
                        }
                        Err(e) => {
                            tracing::error!(id, err = %e, "业务类型解码失败,该 ID 单独进重投/毒消息");
                            failed.push(id);
                        }
                    }
                }
                if typed.is_empty() {
                    return failed;
                }
                // handler 失败/panic → 这批传入的 good_ids 全进 failed(只影响实际传入它的 ID)
                let fut = std::panic::AssertUnwindSafe(f(typed));
                match futures::FutureExt::catch_unwind(fut).await {
                    Ok(Ok(())) => failed, // 仅 typed-decode 失败
                    Ok(Err(e)) => {
                        tracing::warn!(err = %e, "handler 返回失败,本桶成功解码的 ID 转重投");
                        failed.extend(good_ids);
                        failed
                    }
                    Err(_) => {
                        tracing::error!("handler panic,本桶成功解码的 ID 转重投(不外溢整批)");
                        failed.extend(good_ids);
                        failed
                    }
                }
            })
        });
        // 路由到 topic 所属组(未隔离 = 默认组 [0];对照 原实现 topicToGroupName)。
        let idx = self.topic_to_group.get(topic).copied().unwrap_or(0);
        self.groups[idx]
            .handlers
            .insert((topic.to_string(), event.to_string()), h);
        self
    }

    /// start:消费 self(typestate),为**每个组**启动 coordinator + 心跳/再平衡 + wake 订阅。
    pub async fn start(self) -> Result<RunningPartition> {
        let PreparedPartition {
            client,
            lock,
            node_id,
            groups,
            topic_to_group,
        } = self;
        let ids: Vec<String> = groups.iter().map(|g| g.id.clone()).collect();
        let mut runtimes: HashMap<String, Arc<runtime::GroupRuntime>> = HashMap::new();
        let mut started: Vec<Arc<runtime::GroupRuntime>> = Vec::new();
        let mut inner: Option<Arc<runtime::GroupRuntime>> = None;
        for (i, g) in groups.into_iter().enumerate() {
            // 每组各自一个 GroupRuntime(独立 coordinator/poll/worker;node_id、lock、cfg 参数共用)。
            let rt = match runtime::GroupRuntime::start(
                Arc::clone(&client),
                Arc::clone(&lock),
                g.layout,
                g.count,
                g.handlers,
                node_id.clone(),
                g.cfg,        // per-group resolved PartitionCfg
                g.stream_cfg, // per-group resolved StreamCfg
            )
            .await
            {
                Ok(rt) => rt,
                // **回滚**:某组起失败时,先优雅停掉已起的组,避免其后台消费/持锁游离泄漏(多组才有的新风险;
                // 单组失败时无已起组)。再上抛原错误。
                Err(e) => {
                    futures::future::join_all(started.iter().map(|rt| rt.shutdown())).await;
                    return Err(e);
                }
            };
            if i == 0 {
                inner = Some(Arc::clone(&rt)); // [0] = 默认组(id="")
            }
            started.push(Arc::clone(&rt));
            runtimes.insert(g.id, rt);
        }
        let topic_to_group: HashMap<String, String> = topic_to_group
            .into_iter()
            .map(|(t, idx)| (t, ids[idx].clone()))
            .collect();
        Ok(RunningPartition {
            inner: inner.expect("默认组恒存在(groups[0])"),
            groups: runtimes,
            topic_to_group,
        })
    }
}

impl RunningPartition {
    /// topic → 目标组 runtime(对照 原实现 `resolveStream`:topicToGroupName 命中走隔离组,否则默认组)。
    /// 路由仅按 **topic**,与 event 无关(同 原实现)。
    ///
    /// # 参数
    /// - `topic`: stream/partition 使用的业务主题。
    fn rt_for_topic(&self, topic: &str) -> &Arc<runtime::GroupRuntime> {
        self.topic_to_group
            .get(topic)
            .and_then(|g| self.groups.get(g))
            .unwrap_or(&self.inner)
    }

    /// 按**逻辑组 ID** 取 runtime(默认组用 `""` / [`DEFAULT_GROUP_ID`];group-scoped 管理 API 用)。
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

    /// 返回全部逻辑组当前仍待异步 XDEL 的 entry 总数。
    ///
    /// 该值是运行时观测 gauge，不参与 ACK、重试或消费终态判断。持续大于零表示 Redis 删除
    /// 未收敛；达到内部有界积压后，消费会被反压，避免已 ACK 的待删 ID 被静默丢弃。
    pub fn async_delete_pending(&self) -> usize {
        self.groups.values().fold(0usize, |total, runtime| {
            total.saturating_add(runtime.async_delete_pending())
        })
    }

    /// 发布(严格守门):空 topic/event → Err(InvalidPublish 语义,用 Config 错误域)。
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

    /// **round-robin 发布**：无路由 key 时全局轮转均摊到各分区，对齐既有 `publish(partition==null)`。
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

    /// 发布(i64 路由 key:uid/orderId 等数字标识,免 to_string)。按 topic 路由到所属组。
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

    /// 兼容入口:入参无效返回 Ok(None) + 计数,**名字明示可能不发布**——
    /// 给依赖 原实现 静默语义的迁移业务;新代码一律用 publish()。
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

    /// 本节点当前持有的分区(诊断;对照 原实现 claimedPartitions)。
    pub async fn claimed_partitions(&self) -> Vec<u32> {
        self.inner.claimed_partitions().await
    }

    // ── 毒消息管理 API（单 owner 进程内直调，跨节点通过 command outbox 路由）──

    /// 查询分区是否 Parked(返回 park_id;诊断/运维入口)。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn parked(&self, p: u32) -> Result<Option<String>> {
        self.inner.parked_id(p).await
    }

    /// resume:把 Park 的消息重投回源 stream(以新 ID;顺序位置不可恢复—— 如实声明),
    /// 分区恢复消费。返回新 entry IDs。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        self.inner.resume_parked(p).await
    }

    /// drop:放弃 Park 的消息(quarantine 删除,marker 终态 Dropped 留审计),分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn drop_parked(&self, p: u32) -> Result<()> {
        self.inner.drop_parked(p).await
    }

    /// dlq:把 Parked 消息按 planned-ID 协议发布到 DLQ stream({prefix}:dlq),
    /// marker 终态 Dlqed,分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        // owner-fenced:经 inner 校验本节点是 owner 后再处置 + 通知解除 Parked
        self.inner.dlq_parked(p).await
    }

    /// force:resume 进入 ResumeIndeterminate 后的唯一推进入口——
    /// 为不确定的 reservation 分配全新 planned ID 重发,**显式接受可能重复/变序**,
    /// 审计链(新旧 planned 对照)写入 marker。成功后分区恢复消费。
    ///
    /// # 参数
    /// - `p`: 默认组内的分区编号。
    pub async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        self.inner.force_republish(p).await
    }

    /// **通用 liveness-takeover**：operator 确认原 op 已死后，强制接管某分区卡死的在途
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

    /// 提交管理命令(at-least-once:Unknown 后可同 op_id 重复提交,owner 按 result 去重)。
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

    /// 查询命令结果(producer 轮询;None = 尚无记录)。
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

    /// 各组本节点持有的分区(对照 原实现 `claimedPartitions(): Map<groupName, List<Integer>>`)。key=逻辑组 ID
    /// (默认组 = `""`=`DEFAULT_GROUP_ID`,隔离组 = 逻辑名)。group-scoped 管理 API 的 `group` 参数用此 key。
    pub async fn claimed_partitions_by_group(&self) -> HashMap<String, Vec<u32>> {
        let mut out = HashMap::new();
        for (id, rt) in &self.groups {
            out.insert(id.clone(), rt.claimed_partitions().await);
        }
        out
    }

    /// 同上,但默认组 key 显示为 `<default>`(对照 原实现 诊断输出; 运维可读性)。
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

    /// 指定组某分区是否 Parked(返回 park_id)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn parked_in(&self, group: &str, p: u32) -> Result<Option<String>> {
        self.rt_by_group(group)?.parked_id(p).await
    }

    /// 指定组 resume:Park 消息重投回源 stream,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn resume_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.resume_parked(p).await
    }

    /// 指定组 drop:放弃 Park 消息,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn drop_parked_in(&self, group: &str, p: u32) -> Result<()> {
        self.rt_by_group(group)?.drop_parked(p).await
    }

    /// 指定组 dlq:Park 消息发布到 DLQ,分区恢复消费。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn dlq_parked_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.dlq_parked(p).await
    }

    /// 指定组 force_republish(ResumeIndeterminate 推进)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn force_republish_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_republish(p).await
    }

    /// 指定组 force_takeover(liveness-takeover)。
    ///
    /// # 参数
    /// - `group`: 逻辑组 ID;默认组使用 [`DEFAULT_GROUP_ID`]。
    /// - `p`: 组内分区编号。
    pub async fn force_takeover_in(&self, group: &str, p: u32) -> Result<Vec<String>> {
        self.rt_by_group(group)?.force_takeover(p).await
    }

    /// 指定组提交管理命令(跨节点 outbox)。
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

    /// 指定组查询管理命令结果。
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

    /// 便利:按 topic 路由查其所属组的 Parked(operator 只知 topic 不知组时用)。
    ///
    /// # 参数
    /// - `topic`: 用于解析目标逻辑组的业务 topic。
    /// - `p`: 组内分区编号。
    pub async fn parked_for_topic(&self, topic: &str, p: u32) -> Result<Option<String>> {
        self.rt_for_topic(topic).parked_id(p).await
    }

    /// 优雅停机:**所有组并发停机**(关 admission → 等 in-flight → 逐分区
    /// 释锁 → 摘心跳 → pub wake)。`inner` 即 `groups[默认组]`(同一 Arc),由 `groups` 统一停一次。
    pub async fn shutdown(&self) {
        futures::future::join_all(self.groups.values().map(|rt| rt.shutdown())).await;
    }
}

impl Drop for RunningPartition {
    /// 未显式 shutdown 时停止所有后台 owner，禁止 coordinator、worker 或 Redis 锁看门狗泄漏。
    fn drop(&mut self) {
        // Drop 无法在当前栈上等待 Redis unlock 或业务 drain；正常路径仍必须调用 shutdown。
        // 异常路径先关闭 admission，再由当前 runtime 复用完整 drain/unlock；无 runtime 时才 abort。
        for runtime in self.groups.values() {
            runtime.shutdown_on_drop();
        }
    }
}
