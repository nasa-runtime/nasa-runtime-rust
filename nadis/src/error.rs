// ============================================================================
// src/error.rs —— 统一错误类型。
//
// 要点(文档):
//   · ExecutionUnknown:请求确认已写出后断线 = "可能已执行",绝不透明重试非幂等命令;
//   · StillReentered:unlock 时仍有其他重入 permit(本地重入模型);
//   · ProtocolMarker:nasa:protocol:{namespace} 校验失败即拒绝启动。
// ============================================================================

use thiserror::Error;

/// `nadis` 统一错误类型,区分配置错误、Redis 底层错误、命令结果不确定和协议状态错误。
///
/// 调用方需要特别区分 [`NasaRedisError::ExecutionUnknown`] 与 [`NasaRedisError::NotExecuted`]:
/// 前者表示可能已写到 Redis,后者表示本地确认尚未发出。
#[derive(Debug, Error)]
pub enum NasaRedisError {
    /// 配置错误(profile 缺失、namespace 为空等)——启动期 fail-fast。
    #[error("配置错误: {0}")]
    Config(String),

    /// Redis 协议标记(nasa:protocol:{{namespace}})校验失败:profile/命名配置与既有数据不符。
    #[error("协议标记校验失败: {0}")]
    ProtocolMarker(String),

    /// 底层 redis 错误(连接/命令/服务器返回错误)。
    #[error("redis 错误: {0}")]
    Redis(#[from] redis::RedisError),

    /// **启动建连探针失败**:保留失败**阶段**(`connect`=DNS/TCP/ConnectionManager;
    /// `topology`=`INFO cluster`;`cluster-discovery`=Cluster slot/topology;`protocol-marker`=协议标记
    /// SET/GET)、**脱敏 endpoint**(`scheme://host:port/db`,不含凭据)与**底层 redis 错误链**——供上层启动
    /// 诊断一次性定位是哪个节点、哪一步、什么根因(WRONGPASS/NOPERM/connection refused/超时…),而不退化成
    /// 泛化的"Redis 不可达"。只出现在建连路径,不参与运行期命令重试判定。
    #[error("probe failed during {stage} on {endpoint}")]
    ConnectProbe {
        /// 失败发生的建连阶段词。
        stage: &'static str,
        /// 脱敏后的目标 endpoint(`scheme://host:port[/db]`)。
        endpoint: String,
        /// 底层 redis 错误,保留原始服务端/传输分类。
        #[source]
        source: redis::RedisError,
    },

    /// 响应解析错误(redis 1.2 起与 RedisError 分离:只有 message,无网络/服务器语义)。
    #[error("响应解析错误: {0}")]
    Parsing(#[from] redis::ParsingError),

    /// 写出后断线——命令可能已执行,调用方按业务幂等性自行决断,框架不重试。
    #[error("执行结果不确定(可能已执行): {0}")]
    ExecutionUnknown(String),

    /// **确定未执行**:PipelineSession 未 `execute()` 即 drop——命令从未发出,
    /// 与 `ExecutionUnknown`(可能已发)区分,调用方可安全重建会话重发。
    #[error("命令未发送(会话未 execute 即丢弃): {0}")]
    NotExecuted(String),

    /// PipelineSession 本地缓冲超限(max_commands / max_bytes)——立即报错,不是背压。
    #[error("pipeline 会话超限: {0}")]
    SessionLimit(String),

    /// unlock 时仍有其他重入 permit,服务端锁未释放。
    #[error("仍有 {remaining} 个重入 permit,锁未释放")]
    StillReentered {
        /// 尚未释放的本地重入 permit 数量。
        remaining: u32,
    },

    /// 当前并未持有该锁(unlock 重复调用 / 锁已 lost)。
    #[error("未持有锁: {0}")]
    LockNotHeld(String),

    /// 等锁超时。
    #[error("等锁超时: {0}")]
    LockTimeout(String),

    /// 序列化/反序列化失败(`Json<T>` 包装)。
    #[error("编解码失败: {0}")]
    Codec(String),

    /// resume 发布结果不确定(planned entry 缺失且 last-generated-id 已越过——
    /// 无法区分"从未执行"与"已发布后被删"; 保守语义):禁止自动重预约,
    /// 仅 force_republish 显式接受重复风险后可推进。
    #[error("发布结果不确定(PublishOutcomeIndeterminate): {0}")]
    PublishIndeterminate(String),

    /// 操作处于不确定终态,普通请求被拒(需先 force/人工对账)。
    #[error("操作存疑(OperationInDoubt): {0}")]
    OperationInDoubt(String),

    /// **owner/任期已变更**:disposition owner-fence 校验判定调用者不再是
    /// 当前 partition owner(NOT_HOLDER / STALE_STAMP / ROUND/NONCE 变化)。这**不是**确定性
    /// 业务拒绝——管理命令应**保留 PEL 交新 owner 续作**,绝不 Rejected+ACK/XDEL 永久丢弃。
    /// 故归入 `is_infra_retryable`=true。
    #[error("owner/任期已变更(OwnershipChanged): {0}")]
    OwnershipChanged(String),

    /// **operation 冲突**:异 operation_id 撞上他宗在途 `*Publishing`/`Dropping`
    /// ——op B 不得续作 op A 的在途转换。这**不是**确定性业务拒绝,而是 **pre-side-effect 拒绝**
    /// (assert_op_owns 在动作副作用前挡下)。`execute_one` 有专属处置:**producer 窗口内**保留 PEL
    /// (候原 op 同 op_id 重提续作),**超窗**写 Rejected 终态停忙等。
    /// `is_infra_retryable`=**false**:专属臂处理,不走通用 infra-retry,消除顺序依赖。
    #[error("operation 冲突(OperationConflict): {0}")]
    OperationConflict(String),
}

impl NasaRedisError {
    /// 业务作用：是否为**基础设施级可重试**错误(连接断开/超时/服务端瞬时类)。
    /// 用于管理命令决策:这类错误**不得**把命令推进到 Rejected
    /// 终态、不得 ACK+XDEL command——保留 PEL 交下一轮或新 owner 重试;反之**确定性失败**
    /// (WRONGTYPE / 语法 ERR / NOPERM / EXECABORT / 解析等)立即 Rejected,绝不空转重试。
    ///
    /// 修正:此前把整个 `Redis(_)` 与 `Parsing(_)` 一律当可重试——WRONGTYPE 这类
    /// 确定性错误会一直留 Executing 空转到 deadline 才转 Rejected,诊断延迟且浪费 Redis。
    /// 现按 `redis::RedisError` 的 IO/超时/断连判定 + 服务端 code 分类。
    pub fn is_infra_retryable(&self) -> bool {
        match self {
            NasaRedisError::Redis(e) => redis_is_transient(e),
            // 写出后断线:命令可能已执行;disposition/command 动作均幂等可重入,留 PEL 重做安全
            NasaRedisError::ExecutionUnknown(_) => true,
            // owner/任期变更:不是业务失败,留 PEL 交新 owner
            NasaRedisError::OwnershipChanged(_) => true,
            // operation 冲突:**返 false**——`execute_one` 对 OperationConflict
            // 有**专属 match 臂**(pre-side-effect → 按 producer 窗口 bound,见 command.rs),不再走通用
            // infra-retry 臂。此处返 false 消除"专属臂必须排在 infra 臂之上"的隐式顺序依赖(任何重排都
            // 不会让 OperationConflict 落入无界重试。
            NasaRedisError::OperationConflict(_) => false,
            // Parsing / Config / ProtocolMarker / OperationInDoubt / PublishIndeterminate /
            // SessionLimit / Lock* / Codec:确定性,不重试
            _ => false,
        }
    }
}

/// 业务作用：`redis::RedisError` 是否瞬时可重试:IO/超时/断连,或服务端 TRYAGAIN/LOADING/
/// CLUSTERDOWN/MASTERDOWN(集群/主从切换的暂时态);WRONGTYPE/ERR/NOPERM/CROSSSLOT/
/// EXECABORT/NOSCRIPT 等确定性服务端错误一律不重试。
///
/// # 参数
/// - `e`: 错误对象或外部错误值。
fn redis_is_transient(e: &redis::RedisError) -> bool {
    if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() {
        return true;
    }
    matches!(
        e.code(),
        Some("TRYAGAIN") | Some("LOADING") | Some("CLUSTERDOWN") | Some("MASTERDOWN")
    )
}

/// nadis 组件统一结果类型。
pub type Result<T> = std::result::Result<T, NasaRedisError>;
