//! Outbox 的 MySQL 后端。
//!
//! **写侧**:`append` 经 [`natx::conn`]——在 `natx::run`/`#[transactional]` 事务内调用则
//! 事件 INSERT 与业务写**同一事务原子提交/回滚**,消除“业务已提交但事件丢失”窗口;事务外则走连接池。
//!
//! **投递侧(dispatcher)**:`dispatch_batch` 轮询未投递行(按 `id` 升序保序)→ 复用
//! [`naoutbox_core::dispatch_in_order`] 保序至少一次投递 → 把成功前缀 `mark_dispatched`。投递失败的行
//! 留 `dispatched=0` 下轮重试。**至少一次**:下游消费者按 `event_id` 幂等去重。每轮先竞争 MySQL
//! advisory claim，同一数据库任一时刻只有一个 dispatcher 轮询/发布/标记；连接断开时 claim 自动释放。
//! 成功只按本轮精确 id 标记，绝不使用 `id <= max` 越权标记其它 dispatcher 的记录。
//! 事务确认提交后会推进进程内提交代际，受管 dispatcher 据此立即尝试投递；定时轮询继续承担
//! 跨进程写入、进程崩溃和通知合并后的持久化兜底，不作为正常提交的固定等待时间。
//!
//! **可选 DLT(毒丸死信,opt-in)**:[`MySqlOutbox::dispatch_batch_with_dlt`] 在保序投递之上给**首个失败行**
//! 记 `attempts`;连续失败达 `max_attempts` 即标 `dead=1` 移出投递流(死信),下一轮从其后继续——毒丸不再
//! 永久阻塞后续事件。**代价是明确的**:死信之后、同聚合根的后续事件会先于它落地(局部有序对毒丸让步);
//! 不接受此让步的场景继续用 [`MySqlOutbox::dispatch_batch`],该入口永不跳过,毒丸会阻塞直到人工介入。死信行保留
//! 全部字段供人工重放(`dead=0` 复活)。
//!
//! 底层错误一律脱敏为 [`OutboxStoreError`](不回显 SQL/凭据/payload)。

#![forbid(unsafe_code)]

mod retention;
pub use retention::{
    RetentionRoundReport, RETENTION_COMMIT_UNCERTAIN_REASON, RETENTION_LOCK_CONTENTION_REASON,
};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use naoutbox_core::{
    dispatch_in_order, DispatchReport, OutboxEvent, OutboxPublisher, OutboxWriteContext,
    TENANT_QUOTA_EXCEEDED_REASON,
};
use sqlx::{pool::PoolConnection, MySql, MySqlConnection, Row as _};
use tokio::sync::watch;

/// 进程内提交代际只承担低延迟唤醒；数据库轮询仍是跨进程和崩溃恢复的最终事实来源。
static COMMIT_SIGNAL: OnceLock<watch::Sender<u64>> = OnceLock::new();

/// 未路由 aggregate_type 的默认 lane；未安装任何路由时全部事件归入本 lane,
/// 行为与未分片的单 dispatcher 完全一致(分片是显式 opt-in)。
pub const DEFAULT_CHANNEL: &str = "global";

/// 进程级冻结的 aggregate_type → channel 路由。写侧按它为新行落 channel 列;
/// 一经安装不可变更——运行期换路由会把同一聚合根的先后事件拆进两个 lane。
static CHANNEL_ROUTES: OnceLock<BTreeMap<String, String>> = OnceLock::new();

/// 业务作用：校验 lane/通道名落在封闭字符集,可安全进入列值、advisory lock 名与指标标签。
///
/// 参数说明：
/// - `channel`: 待校验通道名。
///
/// 返回：小写字母数字与 `_-.`、长度 1..=64 时返回真。
pub fn valid_channel_name(channel: &str) -> bool {
    !channel.is_empty()
        && channel.len() <= 64
        && channel.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
}

/// 业务作用：安装进程级冻结通道路由——分片的显式 opt-in 开关,决定新事件的 lane 归属。
///
/// 路由必须在任何业务 append 之前安装(应用装配阶段),且进程生命周期内只允许一份:
/// 重复安装同一内容按幂等成功,内容不同立即拒绝——变更路由必须走新部署,保证同一
/// 聚合根二元组自始至终落在同一 lane。
///
/// 参数说明：
/// - `routes`: aggregate_type 到通道名的冻结映射;未列出的类型归入 [`DEFAULT_CHANNEL`]。
///
/// 返回：安装成功或幂等重放返回 `Ok`;通道名非法或与已安装内容不一致返回错误。
pub fn install_channel_routes(routes: BTreeMap<String, String>) -> Result<(), OutboxStoreError> {
    for (aggregate_type, channel) in &routes {
        if aggregate_type.is_empty() || aggregate_type.len() > 128 || !valid_channel_name(channel) {
            return Err(OutboxStoreError::new(
                "channel routes require bounded aggregate types and canonical channel names",
            ));
        }
    }
    let installed = CHANNEL_ROUTES.get_or_init(|| routes.clone());
    if *installed != routes {
        return Err(OutboxStoreError::new(
            "channel routes are frozen for the process lifetime",
        ));
    }
    Ok(())
}

/// 业务作用：按冻结路由解析事件的 lane 归属;路由缺失或类型未列出时归入默认 lane。
///
/// 参数说明：
/// - `aggregate_type`: 事件聚合根类型。
///
/// 返回：稳定通道名;派生只依赖聚合类型,同一聚合根二元组必然同 lane。
pub fn channel_of(aggregate_type: &str) -> &'static str {
    match CHANNEL_ROUTES.get() {
        // 静态 OnceLock 的内容借用天然 'static。
        Some(routes) => routes
            .get(aggregate_type)
            .map(String::as_str)
            .unwrap_or(DEFAULT_CHANNEL),
        None => DEFAULT_CHANNEL,
    }
}

/// 进程级冻结的租户在飞事件配额表。列出的租户在 append 事务内原子预留、投递/死信
/// 裁决时同事务释放;未列出的租户不记账不设限——账本行锁会把同租户并发 append 串行化,
/// 这是配额的固有代价,只应由显式选择配额的租户承担。
static TENANT_QUOTAS: OnceLock<BTreeMap<String, u64>> = OnceLock::new();

/// 当前进程按租户在飞配额拒绝的 append 数(低基数计数,不携带租户标签)。
static QUOTA_REJECTIONS: AtomicU64 = AtomicU64::new(0);

/// 业务作用：安装进程级冻结的租户在飞事件配额——配额的显式 opt-in 开关。
///
/// 配额必须在任何业务 append 之前安装(应用装配阶段),进程生命周期内只允许一份:
/// 重复安装同一内容按幂等成功,内容不同立即拒绝。把某租户纳入配额前,该租户的全部
/// 写入必须已改走受信上下文入口,否则旧路径写入的行会在释放时造成账本漂移
/// (由有界对账收敛)。
///
/// 参数说明：
/// - `quotas`: 租户到在飞事件上限的冻结映射;`0` 表示禁止该租户任何新 append。
///
/// 返回：安装成功或幂等重放返回 `Ok`;租户名越界或与已安装内容不一致返回错误。
pub fn install_outbox_tenant_quotas(quotas: BTreeMap<String, u64>) -> Result<(), OutboxStoreError> {
    for tenant in quotas.keys() {
        if tenant.is_empty() || tenant.len() > 256 || tenant.chars().any(char::is_control) {
            return Err(OutboxStoreError::new(
                "outbox tenant quotas require bounded tenant names",
            ));
        }
    }
    let installed = TENANT_QUOTAS.get_or_init(|| quotas.clone());
    if *installed != quotas {
        return Err(OutboxStoreError::new(
            "outbox tenant quotas are frozen for the process lifetime",
        ));
    }
    Ok(())
}

/// 业务作用：查询租户的在飞事件上限;未安装配额或未列出的租户返回 `None`(不设限)。
///
/// 参数说明：
/// - `tenant`: 租户身份。
///
/// 返回：列出的租户返回上限,其余返回 `None`。
pub fn outbox_tenant_quota_of(tenant: &str) -> Option<u64> {
    TENANT_QUOTAS
        .get()
        .and_then(|quotas| quotas.get(tenant).copied())
}

/// 业务作用：判定本进程是否真正启用了 Outbox 租户配额。
///
/// 配额是显式 opt-in,**未启用时投递与死信裁决绝不触碰配额账本**:非采用者既不承担
/// 多表 UPDATE 与显式事务的开销,也不因缺少 `outbox_tenant_quota` 表而中断投递。
/// 空映射视为未启用——它表达"提交了计划但没有任何受限租户",语义上等同不启用。
///
/// 参数说明: 无。
///
/// 返回：安装了非空配额表返回真。
fn tenant_quotas_enabled() -> bool {
    TENANT_QUOTAS.get().is_some_and(|quotas| !quotas.is_empty())
}

/// 业务作用：校验所有 Outbox 部署都依赖的通用持久结构——受信租户归因列存在且宽度
/// 满足公开身份合同(256 字节)。
///
/// 与租户配额无关:`OutboxWriteContext` 的租户上界是公开合同,`outbox_event.tenant`
/// 是每一笔受信写入都会落的列。未启用配额的存量部署若仍是窄列,191..=256 字节的
/// 合法租户可以通过全部身份校验,却在第一笔业务写入处被数据库拒绝。因此本校验在
/// 任何启用 Outbox 的应用 Ready 前无条件执行。
///
/// 参数说明: 无。
///
/// 返回：列存在且宽度达标返回 `Ok`;缺列或宽度不足返回指向修复迁移的脱敏错误。
pub async fn verify_outbox_event_schema() -> Result<(), OutboxStoreError> {
    let mut conn = natx::conn().await.map_err(map_err)?;
    verify_outbox_event_schema_on(conn.as_mut()).await
}

/// 业务作用：在调用方已经持有的 MySQL 会话上复验 Outbox 通用事件表合同，避免组合验表
/// 为同一启动门禁额外占用连接池槽位。
///
/// 参数说明：
/// - `connection`：Ready 阶段独占持有的数据库会话。
///
/// 返回：tenant 列存在且宽度达标返回 `Ok`；否则返回稳定、脱敏的迁移指引。
async fn verify_outbox_event_schema_on(
    connection: &mut MySqlConnection,
) -> Result<(), OutboxStoreError> {
    let tenant_width: Option<Option<i64>> = sqlx::query_scalar(
        "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox_event' \
         AND COLUMN_NAME = 'tenant'",
    )
    .fetch_optional(connection)
    .await
    .map_err(map_err)?;
    match tenant_width {
        None => Err(OutboxStoreError::new(
            "outbox requires the outbox_event.tenant column (run outbox_event_tenant migration)",
        )),
        Some(width) if width.is_none_or(|width| width < TENANT_COLUMN_MIN_WIDTH) => {
            Err(OutboxStoreError::new(
                "outbox_event.tenant must be at least VARCHAR(256) (run outbox_tenant_width migration)",
            ))
        }
        Some(_) => Ok(()),
    }
}

/// 租户列的最小合同宽度,与 `OutboxWriteContext`/Saga `TenantId` 的公开上界一致。
const TENANT_COLUMN_MIN_WIDTH: i64 = 256;

/// 业务作用：校验启用配额所需的持久结构已就位——把"迁移漏跑"从运行期投递中断
/// 提前成暴露在启动门禁上的失败。
///
/// 只在真正启用配额时有意义:未启用的部署不依赖这些结构,调用本函数直接放行。
///
/// 参数说明: 无。
///
/// 返回：未启用或结构齐备返回 `Ok`;缺列/缺表返回指明缺失项的脱敏错误。
pub async fn verify_outbox_tenant_quota_schema() -> Result<(), OutboxStoreError> {
    if !tenant_quotas_enabled() {
        return Ok(());
    }
    let mut conn = natx::conn().await.map_err(map_err)?;
    // 配额校验复用当前会话执行通用合同，合法的单连接池不会因验表内部再次取连接而
    // 自我等待；独立调用本入口时仍完整覆盖 tenant 列存在性与宽度。
    verify_outbox_event_schema_on(conn.as_mut()).await?;
    let has_ledger: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = 'outbox_tenant_quota'",
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(map_err)?;
    if has_ledger == 0 {
        return Err(OutboxStoreError::new(
            "outbox tenant quotas require the outbox_tenant_quota table (run outbox_tenant_quota migration)",
        ));
    }
    let ledger_width: Option<i64> = sqlx::query_scalar(
        "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox_tenant_quota' \
         AND COLUMN_NAME = 'tenant_id'",
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(map_err)?;
    if ledger_width.is_none_or(|width| width < TENANT_COLUMN_MIN_WIDTH) {
        return Err(OutboxStoreError::new(
            "outbox_tenant_quota.tenant_id must be at least VARCHAR(256) (run outbox_tenant_width migration)",
        ));
    }
    // 每个设限租户的账本必须已由对账初始化:存量待投递行未入账时,其投递/死信释放
    // 会扣掉新行名额。Ready 期 fail-fast,不把部署错误拖到首次业务 append。
    if let Some(quotas) = TENANT_QUOTAS.get() {
        for tenant in quotas.keys() {
            let initialized: Option<i8> = sqlx::query_scalar(
                "SELECT initialized FROM outbox_tenant_quota WHERE tenant_id = ?",
            )
            .bind(tenant)
            .fetch_optional(conn.as_mut())
            .await
            .map_err(map_err)?;
            if initialized != Some(1) {
                return Err(OutboxStoreError::new(
                    "outbox tenant quota ledger for a capped tenant is not initialized; run reconcile_outbox_tenant_quota after every writer runs the accounting binary",
                ));
            }
        }
    }
    Ok(())
}

/// 业务作用：读取当前进程按租户配额拒绝的 append 总数,供低基数观测面导出。
///
/// 参数说明: 无。
///
/// 返回：进程内累计拒绝数。
pub fn outbox_quota_rejections_total() -> u64 {
    QUOTA_REJECTIONS.load(Ordering::Relaxed)
}

/// 配额账本建表语句(部署应由迁移拥有 schema;此处便于演示环境自举)。
const CREATE_QUOTA_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS outbox_tenant_quota ( \
     tenant_id VARCHAR(256) NOT NULL, \
     in_flight BIGINT UNSIGNED NOT NULL DEFAULT 0, \
     initialized TINYINT NOT NULL DEFAULT 0, \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (tenant_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 死信处置事实表建表语句:每条被保留清理删除的死信都持久化批准标识与收据身份。
const CREATE_DEAD_DISPOSAL_SQL: &str = "CREATE TABLE IF NOT EXISTS outbox_dead_disposal ( \
     event_id CHAR(36) NOT NULL, \
     approval VARCHAR(128) NOT NULL, \
     receipt_event_id CHAR(36) NOT NULL, \
     disposed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (event_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// outbox 表建表语句(部署应由迁移拥有 schema;此处便于演示环境自举)。
const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS outbox_event ( \
     id BIGINT NOT NULL AUTO_INCREMENT, \
     event_id CHAR(36) NOT NULL, \
     aggregate_type VARCHAR(128) NOT NULL, \
     aggregate_id VARCHAR(190) NOT NULL, \
     event_type VARCHAR(128) NOT NULL, \
     payload LONGBLOB NOT NULL, \
     traceparent VARCHAR(64) NULL, \
     tenant VARCHAR(256) NOT NULL DEFAULT 'system', \
     channel VARCHAR(64) NOT NULL DEFAULT 'global', \
     dispatched TINYINT NOT NULL DEFAULT 0, \
     attempts INT NOT NULL DEFAULT 0, \
     dead TINYINT NOT NULL DEFAULT 0, \
     dead_at TIMESTAMP(6) NULL, \
     created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
     dispatched_at TIMESTAMP NULL, \
     PRIMARY KEY (id), \
     UNIQUE KEY uk_event_id (event_id), \
     KEY idx_dispatchable (dispatched, dead, id), \
     KEY idx_channel_dispatchable (channel, dispatched, dead, id), \
     KEY idx_tenant_dispatchable (tenant, dispatched, dead, id), \
     KEY idx_retention_dispatched (dispatched, dead, dispatched_at), \
     KEY idx_retention_dead (dead, dead_at), \
     KEY idx_dead (dead, id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// outbox store I/O 失败(脱敏;不含 SQL/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStoreError {
    /// 稳定脱敏原因。
    pub reason: String,
}

impl OutboxStoreError {
    /// 业务作用：用稳定脱敏原因构造 Outbox 持久层错误。
    ///
    /// 参数说明：
    /// - `reason`：允许向上游暴露的稳定失败分类。
    ///
    /// 返回：不携带 SQL、凭据或事件正文的存储错误。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OutboxStoreError {
    /// 业务作用：输出不含 SQL、连接信息或 payload 的稳定存储错误。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：稳定摘要写入成功时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "outbox store error: {}", self.reason)
    }
}

impl std::error::Error for OutboxStoreError {}

/// 一条待投递行(带 DB 主键 id,供投递成功后 `mark_dispatched`)。
#[derive(Debug, Clone)]
struct PendingRow {
    id: i64,
    event: OutboxEvent,
}

/// 绑定 MySQL session advisory lock 的 dispatcher 连接。
///
/// 正常路径必须先调用 [`Self::release`]：确认 `RELEASE_LOCK` 成功后连接才会回到 pool。错误返回、
/// future cancellation 或 panic 会走 [`Drop`]，把 pooled connection 标为 close-on-drop；物理连接关闭
/// 后 MySQL 才能可靠释放该 session 持有的 named lock。不能只把连接普通归还 pool，否则锁会跟着
/// session 留在池中。
struct DispatchClaim {
    connection: Option<PoolConnection<MySql>>,
    /// lane 名;`None` 表示未分片的全表 dispatcher claim(锁名不带通道后缀)。
    channel: Option<String>,
}

impl DispatchClaim {
    /// 业务作用：用专用池连接创建尚未释放的 dispatcher claim guard，预先启用断线释放兜底。
    ///
    /// 参数说明：
    /// - `connection`：即将竞争 named lock 的独占池连接。
    ///
    /// 返回：持有连接关闭责任的 claim guard。
    fn new(connection: PoolConnection<MySql>) -> Self {
        Self {
            connection: Some(connection),
            channel: None,
        }
    }

    /// 业务作用：创建 lane 独占 claim guard——每个 lane 的锁名互不相同,停摆半径收窄到
    /// 单 lane,其余 lane 的投递权威不受影响。
    ///
    /// 参数说明：
    /// - `connection`：即将竞争该 lane named lock 的独占池连接。
    /// - `channel`：lane 名。
    ///
    /// 返回：持有连接关闭责任的 lane claim guard。
    fn for_channel(connection: PoolConnection<MySql>, channel: &str) -> Self {
        Self {
            connection: Some(connection),
            channel: Some(channel.to_string()),
        }
    }

    /// 业务作用：借用绑定 named lock 的底层 MySQL session，确保领取、发布标记与释放使用同一会话。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：claim guard 独占持有的可变 MySQL 连接。
    fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("dispatch claim connection must exist until release")
    }

    /// 业务作用：在当前 session 明确没有取得 claim 时解除兜底关闭，让健康连接正常回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；本方法消费 guard 并归还连接所有权。
    fn disarm(mut self) {
        let _ = self.connection.take();
    }

    /// 业务作用：显式释放 named lock；只有服务端确认释放成功才允许连接回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；释放结果不确定时保留关闭责任，由 `Drop` 关闭物理会话。
    async fn release(mut self) {
        let released: Result<Option<i64>, _> =
            match self.channel.take() {
                Some(channel) => sqlx::query_scalar(
                    "SELECT RELEASE_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE(), ':', ?), 256))",
                )
                .bind(channel)
                .fetch_one(self.connection())
                .await,
                None => {
                    sqlx::query_scalar(
                        "SELECT RELEASE_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE()), 256))",
                    )
                    .fetch_one(self.connection())
                    .await
                }
            };
        if matches!(released, Ok(Some(1))) {
            let _ = self.connection.take();
        }
        // 失败/非 owner:保留 connection 给 Drop，关闭 session 兜底释放可能仍持有的锁。
    }
}

impl Drop for DispatchClaim {
    /// 业务作用：未确认 `RELEASE_LOCK` 时关闭物理连接，阻止 named lock 随池连接泄漏。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；只标记连接在析构时关闭，不执行阻塞 I/O。
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

/// MySQL outbox。无自身状态:每次操作经 `natx::conn()` 取连接,自动感知 ambient 事务。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlOutbox;

impl MySqlOutbox {
    /// 业务作用：创建无状态 Outbox 入口，不提前建连或取得 dispatcher claim。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可在事务写侧或独立投递侧复用的轻量句柄。
    pub fn new() -> Self {
        Self
    }

    /// 业务作用：为受控自举环境创建 Outbox 表，并把历史演示表补齐稳定身份、死信字段和投递索引。
    ///
    /// 参数说明: 无；连接来自已经初始化的 `natx` 默认池。
    ///
    /// 返回：表结构达到当前运行合同后成功；建表、历史回填或强制约束失败时返回脱敏存储错误。
    pub async fn ensure_schema() -> Result<(), OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        sqlx::query(CREATE_TABLE_SQL)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        // 既有表升级兜底(CREATE IF NOT EXISTS 不改旧表):best-effort 补 DLT 两列,已存在则忽略错误。
        // 部署环境仍应由迁移拥有 schema;这里只服务演示环境自举的平滑升级。
        for statement in [
            "ALTER TABLE outbox_event ADD COLUMN attempts INT NOT NULL DEFAULT 0",
            "ALTER TABLE outbox_event ADD COLUMN dead TINYINT NOT NULL DEFAULT 0",
            "ALTER TABLE outbox_event ADD COLUMN event_id CHAR(36) NULL",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        // 旧 demo 表补事件 id；UUID 只在 migration 期生成一次，后续重投始终复用同一 id。
        sqlx::query("UPDATE outbox_event SET event_id = UUID() WHERE event_id IS NULL")
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        let _ = sqlx::query(
            "ALTER TABLE outbox_event MODIFY event_id CHAR(36) NOT NULL, \
             ADD UNIQUE KEY uk_event_id (event_id)",
        )
        .execute(conn.as_mut())
        .await;
        // 指标抓取与 dispatcher 共用控制面数据库；必须先补齐覆盖索引，避免历史表按总行数扫描，
        // 也避免本地死信长期停留在 dispatched=0 区间时拖慢每一轮待投递查询。
        for statement in [
            "ALTER TABLE outbox_event ADD INDEX idx_dispatchable (dispatched, dead, id)",
            "ALTER TABLE outbox_event ADD INDEX idx_dead (dead, id)",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        // 通道列与按通道领取索引:历史行由列默认值一次性归入 'global' 默认 lane;
        // 该归属规则一经发布不可更改,否则同一聚合根的历史事件会被拆进两个 lane。
        for statement in [
            "ALTER TABLE outbox_event ADD COLUMN channel VARCHAR(64) NOT NULL DEFAULT 'global'",
            "ALTER TABLE outbox_event ADD INDEX idx_channel_dispatchable (channel, dispatched, dead, id)",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        // 死信时刻列:死信保留期必须从"进入死信"起算,而不是从创建起算——长期待投递
        // 后刚标死的行不允许立即成为清理候选。
        let _ = sqlx::query("ALTER TABLE outbox_event ADD COLUMN dead_at TIMESTAMP(6) NULL")
            .execute(conn.as_mut())
            .await;
        // 保留清理的时间过滤与最老候选聚合按生命周期时刻走索引:缺它们时聚合退化为
        // 全表扫描,预算在第一次检查前就被大表吃光。
        for statement in [
            "ALTER TABLE outbox_event ADD INDEX idx_retention_dispatched (dispatched, dead, dispatched_at)",
            "ALTER TABLE outbox_event ADD INDEX idx_retention_dead (dead, dead_at)",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        // 租户列:历史行与未携带上下文的 append 固定归入 system 租户;租户身份只能由
        // 受信写入上下文填充,不得从 payload 解析。配额账本表随建。
        for statement in [
            "ALTER TABLE outbox_event ADD COLUMN tenant VARCHAR(256) NOT NULL DEFAULT 'system'",
            "ALTER TABLE outbox_event ADD INDEX idx_tenant_dispatchable (tenant, dispatched, dead, id)",
            // 早期演示库列宽 190:对齐 TenantId 合同上限,191..=256 字节的合法租户不得
            // 在双写处被窄化拒绝。
            "ALTER TABLE outbox_event MODIFY COLUMN tenant VARCHAR(256) NOT NULL DEFAULT 'system'",
            "ALTER TABLE outbox_tenant_quota MODIFY COLUMN tenant_id VARCHAR(256) NOT NULL",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        sqlx::query(CREATE_QUOTA_TABLE_SQL)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        sqlx::query(CREATE_DEAD_DISPOSAL_SQL)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        // 旧演示库的配额账本补初始化标记列;生产环境由正式 migration 拥有该变更。
        let _ = sqlx::query(
            "ALTER TABLE outbox_tenant_quota ADD COLUMN initialized TINYINT NOT NULL DEFAULT 0",
        )
        .execute(conn.as_mut())
        .await;
        Ok(())
    }

    /// 业务作用：追加一条 Outbox 事件，并在数据库明确提交后唤醒当前进程的 dispatcher。
    ///
    /// 参数说明：
    /// - `event`：与业务事实共享事务、携带稳定事件身份的待发布事件。
    ///
    /// 返回：事务内完成 INSERT 并登记提交后唤醒、或事务外完成独立提交时成功；连接与写入失败返回
    /// 脱敏存储错误。事务回滚时不会发送唤醒。
    pub async fn append(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let transactional = natx::in_transaction();
        let mut conn = natx::conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await?;
        drop(conn);
        if transactional {
            register_commit_notification()?;
        } else {
            notify_committed_append();
        }
        Ok(())
    }

    /// 业务作用：只在当前 ambient MySQL 事务内追加关键事件，并把投递唤醒绑定到最外层提交确认。
    ///
    /// 审计、资金等关键双写应使用此入口；事务上下文缺失时明确失败，绝不回退到 autocommit pool。
    ///
    /// 参数说明：
    /// - `event`：必须与当前业务事实原子提交的待发布事件。
    ///
    /// 返回：INSERT 与提交后唤醒均成功登记时完成；缺少事务或写入失败返回脱敏存储错误。回滚、
    /// rollback-only 与提交失败都不会唤醒 dispatcher。
    pub async fn append_transactional(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let mut conn = natx::mandatory_conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await?;
        drop(conn);
        register_commit_notification()?;
        Ok(())
    }

    /// 业务作用：以受信写入上下文在当前 ambient 事务内追加事件——配额与归因的租户
    /// 身份由已认证业务上下文填充,绝不从 payload 解析。
    ///
    /// 若该租户被冻结配额表列出,先在同事务内对 `outbox_tenant_quota` 做
    /// `in_flight < cap` 的条件自增预留:预留失败按稳定原因码拒绝且**不写事件行**,
    /// 业务事务回滚时预留一并回滚。未列出的租户不记账不设限。事件行携带租户列,
    /// 投递/死信裁决时由同一语句/事务释放。
    ///
    /// 参数说明：
    /// - `context`: 已认证租户的受信写入上下文。
    /// - `event`: 必须与当前业务事实原子提交的待发布事件。
    ///
    /// 返回：预留(如需)与 INSERT 都成功时完成;配额耗尽返回携带
    /// [`TENANT_QUOTA_EXCEEDED_REASON`] 的错误,缺少事务或数据库失败返回脱敏错误。
    pub async fn append_transactional_with_context(
        &self,
        context: &OutboxWriteContext,
        event: &OutboxEvent,
    ) -> Result<(), OutboxStoreError> {
        let mut conn = natx::mandatory_conn().await.map_err(map_err)?;
        if let Some(cap) = outbox_tenant_quota_of(context.tenant()) {
            // 预留先于写行:两步都持有该租户账本行锁,并发 append 在行锁上串行化,
            // 上限不可能被无锁计数穿透;拒绝时事件行从未写入,无需补偿。
            sqlx::query(
                "INSERT IGNORE INTO outbox_tenant_quota (tenant_id, in_flight) VALUES (?, 0)",
            )
            .bind(context.tenant())
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
            // 设限租户的预留要求账本已初始化(存量待投递行经受锁对账入账),否则旧行
            // 的投递/死信释放会扣掉新行名额、上限被穿透;未初始化按部署错误上报,
            // 不与"配额耗尽"共用稳定拒绝。
            let reserved = sqlx::query(
                "UPDATE outbox_tenant_quota SET in_flight = in_flight + 1 \
                 WHERE tenant_id = ? AND in_flight < ? AND initialized = 1",
            )
            .bind(context.tenant())
            .bind(cap)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
            if reserved.rows_affected() == 0 {
                let initialized: i8 = sqlx::query_scalar(
                    "SELECT initialized FROM outbox_tenant_quota WHERE tenant_id = ?",
                )
                .bind(context.tenant())
                .fetch_one(conn.as_mut())
                .await
                .map_err(map_err)?;
                if initialized == 0 {
                    return Err(OutboxStoreError::new(
                        "outbox tenant quota ledger is not initialized; run reconcile_outbox_tenant_quota after every writer runs the accounting binary",
                    ));
                }
                QUOTA_REJECTIONS.fetch_add(1, Ordering::Relaxed);
                // 稳定原因码使配额拒绝与系统故障可区分;文本不携带其它租户信息。
                return Err(OutboxStoreError::new(TENANT_QUOTA_EXCEEDED_REASON));
            }
        }
        Self::append_on_with_tenant(conn.as_mut(), event, context.tenant()).await?;
        drop(conn);
        register_commit_notification()?;
        Ok(())
    }

    /// 业务作用：读取单个租户的精确在飞事件数——只服务受鉴权管理查询,不进指标标签。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：账本记录的在飞事件数;无记录返回 0。
    pub async fn outbox_tenant_quota_usage(&self, tenant: &str) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row = sqlx::query("SELECT in_flight FROM outbox_tenant_quota WHERE tenant_id = ?")
            .bind(tenant)
            .fetch_optional(conn.as_mut())
            .await
            .map_err(map_err)?;
        match row {
            Some(row) => Ok(row.try_get::<u64, _>("in_flight").map_err(map_err)?),
            None => Ok(0),
        }
    }

    /// 业务作用：按已提交事实有界对账单个租户的在飞账本——把旧路径混写或崩溃窗口
    /// 造成的漂移收敛回真实在飞数。
    ///
    /// 必须在 ambient 事务内调用,且**先锁账本行再计数**:受信 append 的预留与投递/
    /// 死信裁决的释放都持有同一把行锁,未提交的在线事务被挡在行锁外;先计数后覆盖
    /// 会让"计数与覆盖之间提交的在线变更"被旧值覆盖,对账本身制造漂移。在飞事实 =
    /// `dispatched = 0 AND dead = 0` 的该租户行数,扫描范围由
    /// `(tenant, dispatched, dead, id)` 索引约束;不在请求路径调用,由运维显式触发。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    ///
    /// 返回：对账后的在飞事件数;事务缺失或底层失败返回错误。
    pub async fn reconcile_outbox_tenant_quota(
        &self,
        tenant: &str,
    ) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::mandatory_conn().await.map_err(map_err)?;
        sqlx::query("INSERT IGNORE INTO outbox_tenant_quota (tenant_id, in_flight) VALUES (?, 0)")
            .bind(tenant)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        // 行锁先行:此后该租户的受信 append(预留)与投递/死信裁决(释放)都在本事务
        // 提交前被阻塞,计数窗口内不可能再有账本参与方提交新事实。
        let _locked: u64 = sqlx::query_scalar(
            "SELECT in_flight FROM outbox_tenant_quota WHERE tenant_id = ? FOR UPDATE",
        )
        .bind(tenant)
        .fetch_one(conn.as_mut())
        .await
        .map_err(map_err)?;
        let actual: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_event \
             WHERE tenant = ? AND dispatched = 0 AND dead = 0",
        )
        .bind(tenant)
        .fetch_one(conn.as_mut())
        .await
        .map_err(map_err)?;
        let actual = actual.max(0) as u64;
        // 对账是初始化标记的唯一置位入口:设限租户的预留与 Ready 校验都以它为放行条件。
        sqlx::query(
            "UPDATE outbox_tenant_quota SET in_flight = ?, initialized = 1 WHERE tenant_id = ?",
        )
        .bind(actual)
        .bind(tenant)
        .execute(conn.as_mut())
        .await
        .map_err(map_err)?;
        Ok(actual)
    }

    /// 业务作用：订阅当前进程 Outbox 提交代际，使受管 dispatcher 在新事实提交后立即尝试投递。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：初值为当前代际的接收端；通知只优化本进程延迟，调用方仍须保留数据库轮询兜底。
    pub fn subscribe_committed_appends() -> watch::Receiver<u64> {
        commit_signal().subscribe()
    }

    /// 业务作用：在调用方提供的连接内写入完整 Outbox 事件，不改变连接的提交所有权。
    ///
    /// 参数说明：
    /// - `connection`：业务事务或独立提交路径持有的 MySQL 连接。
    /// - `event`：已具备稳定事件身份的待发布事件。
    ///
    /// 返回：事件行写入成功时完成；数据库失败返回脱敏错误，由外层决定提交或回滚。
    async fn append_on(
        connection: &mut MySqlConnection,
        event: &OutboxEvent,
    ) -> Result<(), OutboxStoreError> {
        // 未携带受信上下文的历史路径固定映射 system 租户:租户身份只能由已认证
        // 上下文填充,这里不读取 payload 或 header 里的任何自报身份。
        Self::append_on_with_tenant(connection, event, naoutbox_core::SYSTEM_TENANT).await
    }

    /// 业务作用：在调用方提供的连接内写入带租户归因的完整 Outbox 事件行。
    ///
    /// 参数说明：
    /// - `connection`：业务事务或独立提交路径持有的 MySQL 连接。
    /// - `event`：已具备稳定事件身份的待发布事件。
    /// - `tenant`：受信写入上下文解析出的租户;历史路径固定传 system。
    ///
    /// 返回：事件行写入成功时完成；数据库失败返回脱敏错误，由外层决定提交或回滚。
    async fn append_on_with_tenant(
        connection: &mut MySqlConnection,
        event: &OutboxEvent,
        tenant: &str,
    ) -> Result<(), OutboxStoreError> {
        // lane 归属在写入时由冻结路由稳定派生:只依赖 aggregate_type,同一聚合根
        // 二元组自始至终同 lane;未安装路由时全部落默认 lane,分片保持显式 opt-in。
        sqlx::query(
            "INSERT INTO outbox_event \
             (event_id, aggregate_type, aggregate_id, event_type, payload, traceparent, tenant, channel, dispatched) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(&event.event_id)
        .bind(&event.aggregate_type)
        .bind(&event.aggregate_id)
        .bind(&event.event_type)
        .bind(&event.payload)
        .bind(&event.traceparent)
        .bind(tenant)
        .bind(channel_of(&event.aggregate_type))
        .execute(connection)
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// 业务作用：读取仍可投递的持久事件数量，供积压指标和恢复决策使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：基于 `(dispatched, dead, id)` 覆盖索引返回非死信积压；数据库失败返回脱敏错误。
    pub async fn pending_count(&self) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row =
            sqlx::query("SELECT COUNT(*) AS n FROM outbox_event WHERE dispatched = 0 AND dead = 0")
                .fetch_one(conn.as_mut())
                .await
                .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }

    /// 业务作用：在数据库级唯一 claim 下保序发布一个有界批次，并只标记下游已确认的成功前缀。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端；成功必须表示下游已经确认接收。
    /// - `limit`：单轮最多领取的行数，限制内存与外部 I/O 占用。
    ///
    /// 返回：claim 未取得时返回空报告；否则返回发布总量、成功前缀和首个错误，存储失败返回脱敏错误。
    pub async fn dispatch_batch<P>(
        &self,
        publisher: &P,
        limit: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        let Some(mut dispatch_claim) = self.try_claim_dispatcher().await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending(dispatch_claim.connection(), limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(dispatch_claim.connection(), &pending[..report.published])
                .await?;
        }
        dispatch_claim.release().await;
        Ok(report)
    }

    /// 业务作用：在保序投递之上累计首个失败事件的预算，达到阈值后将其移入本地死信集合。
    ///
    /// 给首个失败行记 `attempts`,达到 `max_attempts` 即标死信
    /// (`dead=1`)移出投递流,下一轮从其后继续。返回本轮报告(报告口径与 [`dispatch_batch`](Self::dispatch_batch) 一致)。
    ///
    /// 有序性让步见模块文档:死信行之后的同聚合根事件会先于它落地。`max_attempts` 应 ≥ 1;计数按
    /// 「本方法观察到的失败轮次」累计,与 `dispatch_batch` 混用不会误标(那条路径不动 `attempts`)。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端。
    /// - `limit`：单轮最多领取的行数。
    /// - `max_attempts`：同一事件累计多少个失败轮次后标记死信。
    ///
    /// 返回：返回本轮发布报告；阈值为零或存储失败时返回脱敏错误。进入死信意味着显式放弃该位置的
    /// 严格顺序，调用方必须事先批准。
    pub async fn dispatch_batch_with_dlt<P>(
        &self,
        publisher: &P,
        limit: u32,
        max_attempts: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        if max_attempts == 0 {
            return Err(OutboxStoreError::new(
                "max_attempts must be greater than zero",
            ));
        }
        let Some(mut dispatch_claim) = self.try_claim_dispatcher().await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending(dispatch_claim.connection(), limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(dispatch_claim.connection(), &pending[..report.published])
                .await?;
        }
        // 首个失败行记一次尝试;达到上限即标死信(单行静态 SQL,原子)。
        // 瞬态/结果不确定失败豁免死信预算:消息保留待投递,不计 attempts 不标死——
        // 远端可能已提交,离开投递流等于放弃后续收敛。只有确定性失败进入预算裁决。
        if report
            .first_error
            .as_ref()
            .is_some_and(|error| !error.is_transient())
            && report.published < pending.len()
        {
            let failed_id = pending[report.published].id;
            Self::record_failed_attempt(dispatch_claim.connection(), failed_id, max_attempts)
                .await?;
        }
        dispatch_claim.release().await;
        Ok(report)
    }

    /// 业务作用：读取本地死信集合数量，供毒丸策略告警和人工恢复决策使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：基于 `(dead, id)` 覆盖索引返回死信总数；数据库失败返回脱敏错误。
    pub async fn dead_count(&self) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row = sqlx::query("SELECT COUNT(*) AS n FROM outbox_event WHERE dead = 1")
            .fetch_one(conn.as_mut())
            .await
            .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }

    /// 业务作用：原子累计一个未确认事件的失败轮次，并在预算耗尽时标记为本地死信。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `id`：首个失败事件的数据库主键。
    /// - `max_attempts`：死信阈值。
    ///
    /// 返回：行仍处于可投递状态时完成原子更新；数据库失败返回脱敏错误。
    async fn record_failed_attempt(
        connection: &mut MySqlConnection,
        id: i64,
        max_attempts: u32,
    ) -> Result<(), OutboxStoreError> {
        if !tenant_quotas_enabled() {
            // 未启用配额:保持升级前的单条原子 UPDATE。没有账本要释放,就不该为此
            // 引入显式事务与额外往返,也不引用配额账本。
            // 死信时刻与翻转同语句写入:保留清理只认 dead_at,缺它的死信永远无法进入
            // 清理候选——零足迹分支省的是配额账本,不能省生命周期证据。
            sqlx::query(
                "UPDATE outbox_event SET \
                 dead = IF(attempts + 1 >= ?, 1, dead), \
                 dead_at = IF(attempts + 1 >= ?, CURRENT_TIMESTAMP(6), dead_at), \
                 attempts = attempts + 1 \
                 WHERE id = ? AND dispatched = 0 AND dead = 0",
            )
            .bind(max_attempts)
            .bind(max_attempts)
            .bind(id)
            .execute(connection)
            .await
            .map_err(map_err)?;
            return Ok(());
        }
        // 启用配额:死信裁决与名额释放必须同事务——行离开可投递集合与账本回落要么
        // 一起提交、要么一起消失。claim 连接不在 ambient 事务里,这里用显式事务;
        // 中途失败时 claim 的 close-on-drop 关闭会话,MySQL 隐式回滚。
        sqlx::raw_sql("BEGIN")
            .execute(&mut *connection)
            .await
            .map_err(map_err)?;
        // FOR UPDATE 锁行读取旧 attempts:是否翻转死信由旧值决定,不依赖多表 UPDATE
        // 的赋值顺序语义;claim 已按 lane 串行化,这里的行锁是对非本 lane 写者的防御。
        let row = sqlx::query(
            "SELECT attempts, tenant FROM outbox_event \
             WHERE id = ? AND dispatched = 0 AND dead = 0 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_err)?;
        let Some(row) = row else {
            // 行已离开可投递集合(并发投递或早前死信):无事可记,提交空事务。
            sqlx::raw_sql("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(map_err)?;
            return Ok(());
        };
        let attempts: i32 = row.try_get("attempts").map_err(map_err)?;
        let tenant: String = row.try_get("tenant").map_err(map_err)?;
        let becomes_dead = attempts.saturating_add(1) >= max_attempts as i32;
        // 行锁已在本事务内持有,dispatched/dead 不可能被并发改写;仍保留守卫条件,
        // 使语句单独看也不会越过已离开可投递集合的行。
        // 翻转死信时同步记录 dead_at:死信保留期以它为唯一时间依据。
        sqlx::query(
            "UPDATE outbox_event SET attempts = attempts + 1, dead = ?, \
             dead_at = IF(?, CURRENT_TIMESTAMP(6), dead_at) \
             WHERE id = ? AND dispatched = 0 AND dead = 0",
        )
        .bind(becomes_dead)
        .bind(becomes_dead)
        .bind(id)
        .execute(&mut *connection)
        .await
        .map_err(map_err)?;
        if becomes_dead {
            // 死信行离开可投递集合:同事务释放在飞名额;无账本行的租户自然无操作,
            // `in_flight > 0` 防御旧路径混写造成的欠账回绕。
            sqlx::query(
                "UPDATE outbox_tenant_quota SET in_flight = in_flight - 1 \
                 WHERE tenant_id = ? AND in_flight > 0",
            )
            .bind(&tenant)
            .execute(&mut *connection)
            .await
            .map_err(map_err)?;
        }
        sqlx::raw_sql("COMMIT")
            .execute(&mut *connection)
            .await
            .map_err(map_err)?;
        Ok(())
    }

    /// 业务作用：按 `id` 升序领取真实待投递候选，避免死信空洞破坏局部顺序和批次上限。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `limit`：最多回表读取的事件行数。
    ///
    /// 返回：联合索引定位后回表得到的有序事件集合；数据库或解码失败返回脱敏错误。
    async fn poll_pending(
        connection: &mut MySqlConnection,
        limit: u32,
    ) -> Result<Vec<PendingRow>, OutboxStoreError> {
        let rows = sqlx::query(
            "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, traceparent, \
             tenant \
             FROM outbox_event WHERE dispatched = 0 AND dead = 0 ORDER BY id ASC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(connection)
        .await
        .map_err(map_err)?;
        Self::parse_pending(rows)
    }

    /// 业务作用：按 lane 轮询待投递行——每个 lane 独享自己的 `id` 升序前缀,互不阻塞。
    ///
    /// 参数说明：
    /// - `connection`：持有该 lane claim 的 MySQL 会话。
    /// - `channel`：lane 名。
    /// - `limit`：单轮最多领取的行数。
    ///
    /// 返回：该 lane 内按 `id` 升序的待投递批次；数据库失败返回脱敏错误。
    async fn poll_pending_channel(
        connection: &mut MySqlConnection,
        channel: &str,
        limit: u32,
    ) -> Result<Vec<PendingRow>, OutboxStoreError> {
        let rows = sqlx::query(
            "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, traceparent, \
             tenant \
             FROM outbox_event WHERE channel = ? AND dispatched = 0 AND dead = 0 \
             ORDER BY id ASC LIMIT ?",
        )
        .bind(channel)
        .bind(limit)
        .fetch_all(connection)
        .await
        .map_err(map_err)?;
        Self::parse_pending(rows)
    }

    /// 业务作用：把待投递查询结果解析为带主键的事件批次。
    ///
    /// 参数说明：
    /// - `rows`：待投递查询结果集。
    ///
    /// 返回：按查询顺序的批次；列缺失或类型漂移返回脱敏错误。
    fn parse_pending(
        rows: Vec<sqlx::mysql::MySqlRow>,
    ) -> Result<Vec<PendingRow>, OutboxStoreError> {
        let mut pending = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get("id").map_err(map_err)?;
            let event = OutboxEvent {
                event_id: row.try_get("event_id").map_err(map_err)?,
                aggregate_type: row.try_get("aggregate_type").map_err(map_err)?,
                aggregate_id: row.try_get("aggregate_id").map_err(map_err)?,
                event_type: row.try_get("event_type").map_err(map_err)?,
                payload: row.try_get("payload").map_err(map_err)?,
                traceparent: row.try_get("traceparent").map_err(map_err)?,
                // 受信租户归因唯一来源是持久列;发布端据此做租户级授权与分类,
                // 不得从 payload 或自报 header 推导。
                tenant: row.try_get("tenant").map_err(map_err)?,
            };
            pending.push(PendingRow { id, event });
        }
        Ok(pending)
    }

    /// 业务作用：只标记本轮已经得到下游确认的精确行，防止并发或死信空洞被范围更新越过。
    ///
    /// 标记与在飞配额释放在**同一条多表 UPDATE**内完成:行离开可投递集合的瞬间账本
    /// 同步回落,崩溃窗口里不会出现"已投递但仍占额"的幽灵占用。无账本行的租户
    /// (未纳入配额)由 LEFT JOIN 自然跳过;`WHERE dispatched = 0` 保证重复调用不会
    /// 二次释放。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `rows`：已经按顺序得到下游确认的成功前缀。
    ///
    /// 返回：全部精确身份完成幂等标记时成功；数据库失败返回脱敏错误。
    async fn mark_dispatched_exact(
        connection: &mut MySqlConnection,
        rows: &[PendingRow],
    ) -> Result<(), OutboxStoreError> {
        // 未启用配额时保持单表 UPDATE:不引用配额账本,非采用者的投递路径与升级前
        // 逐字节等价,也不因缺少该表而中断。
        let statement = if tenant_quotas_enabled() {
            "UPDATE outbox_event event \
             LEFT JOIN outbox_tenant_quota quota ON quota.tenant_id = event.tenant \
             SET event.dispatched = 1, event.dispatched_at = CURRENT_TIMESTAMP, \
             quota.in_flight = quota.in_flight - IF(quota.in_flight > 0, 1, 0) \
             WHERE event.dispatched = 0 AND event.dead = 0 AND event.id = ? AND event.event_id = ?"
        } else {
            "UPDATE outbox_event SET dispatched = 1, dispatched_at = CURRENT_TIMESTAMP \
             WHERE dispatched = 0 AND dead = 0 AND id = ? AND event_id = ?"
        };
        for row in rows {
            sqlx::query(statement)
                .bind(row.id)
                .bind(&row.event.event_id)
                .execute(&mut *connection)
                .await
                .map_err(map_err)?;
        }
        Ok(())
    }

    /// 业务作用：尝试取得数据库级 dispatcher claim，阻止多个进程同时推进同一表的顺序游标。
    ///
    /// dispatcher 必须运行在业务事务之外：claim、网络 publish 与标记不能占用 ambient transaction，
    /// 否则会把业务事务跨外部 I/O 长时间悬挂。取得的 pooled connection 同时承载 poll/mark，故
    /// `max_connections=1` 也不会发生“持 claim 后再等第二条连接”的自锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得锁时返回 armed guard，锁已被其它进程持有时返回 `None`；事务上下文或数据库失败
    /// 返回脱敏错误。
    async fn try_claim_dispatcher(&self) -> Result<Option<DispatchClaim>, OutboxStoreError> {
        let conn = natx::conn().await.map_err(map_err)?;
        let natx::Conn::Pool(conn) = conn else {
            return Err(OutboxStoreError::new(
                "dispatcher cannot run inside an ambient transaction",
            ));
        };
        // GET_LOCK 请求本身若在响应前取消/断线，服务端可能已经取得锁；先 armed，异常即关闭 session。
        let mut claim = DispatchClaim::new(conn);
        let claimed: Option<i64> =
            sqlx::query_scalar("SELECT GET_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE()), 256), 0)")
                .fetch_one(claim.connection())
                .await
                .map_err(map_err)?;
        if claimed == Some(1) {
            Ok(Some(claim))
        } else {
            claim.disarm();
            Ok(None)
        }
    }

    /// 业务作用：竞争单个 lane 的独占投递权威——锁名带通道后缀,与其它 lane 及未分片
    /// dispatcher 的锁互不冲突,某 lane 停摆不影响其余 lane 的领取。
    ///
    /// 参数说明：
    /// - `channel`：lane 名（须先通过 [`valid_channel_name`]）。
    ///
    /// 返回：取得锁时返回 armed guard；锁已被其它进程持有时返回 `None`；事务上下文或
    /// 数据库失败返回脱敏错误。
    async fn try_claim_channel(
        &self,
        channel: &str,
    ) -> Result<Option<DispatchClaim>, OutboxStoreError> {
        let conn = natx::conn().await.map_err(map_err)?;
        let natx::Conn::Pool(conn) = conn else {
            return Err(OutboxStoreError::new(
                "dispatcher cannot run inside an ambient transaction",
            ));
        };
        let mut claim = DispatchClaim::for_channel(conn, channel);
        let claimed: Option<i64> = sqlx::query_scalar(
            "SELECT GET_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE(), ':', ?), 256), 0)",
        )
        .bind(channel)
        .fetch_one(claim.connection())
        .await
        .map_err(map_err)?;
        if claimed == Some(1) {
            Ok(Some(claim))
        } else {
            claim.disarm();
            Ok(None)
        }
    }

    /// 业务作用：在单 lane 独占 claim 下保序发布该 lane 的一个有界批次。
    ///
    /// lane 内不变量与未分片入口完全一致:成功前缀才标记、`Block` 语义不变——改变的只是
    /// 停摆半径(单 lane)而不是投递不变量。**启用分片后同库不得再运行未分片 dispatcher**,
    /// 两种 claim 锁名不同,并行运行会对同一批行双重发布。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端。
    /// - `channel`：lane 名。
    /// - `limit`：单轮最多领取的行数。
    ///
    /// 返回：claim 未取得时返回空报告；否则返回该 lane 的发布报告；通道名非法或存储
    /// 失败返回脱敏错误。
    pub async fn dispatch_batch_channel<P>(
        &self,
        publisher: &P,
        channel: &str,
        limit: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        if !valid_channel_name(channel) {
            return Err(OutboxStoreError::new("invalid outbox channel name"));
        }
        let Some(mut claim) = self.try_claim_channel(channel).await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending_channel(claim.connection(), channel, limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(claim.connection(), &pending[..report.published]).await?;
        }
        claim.release().await;
        Ok(report)
    }

    /// 业务作用：单 lane 的毒丸死信投递变体——首失败行计预算,达到上限移入死信,
    /// 死信策略的作用域同样收窄到本 lane。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端。
    /// - `channel`：lane 名。
    /// - `limit`：单轮最多领取的行数。
    /// - `max_attempts`：同一事件累计失败轮次上限。
    ///
    /// 返回：该 lane 的发布报告；阈值为零、通道名非法或存储失败返回脱敏错误。
    pub async fn dispatch_batch_channel_with_dlt<P>(
        &self,
        publisher: &P,
        channel: &str,
        limit: u32,
        max_attempts: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        if max_attempts == 0 {
            return Err(OutboxStoreError::new(
                "max_attempts must be greater than zero",
            ));
        }
        if !valid_channel_name(channel) {
            return Err(OutboxStoreError::new("invalid outbox channel name"));
        }
        let Some(mut claim) = self.try_claim_channel(channel).await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending_channel(claim.connection(), channel, limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(claim.connection(), &pending[..report.published]).await?;
        }
        // 瞬态/结果不确定失败豁免死信预算:消息保留待投递,不计 attempts 不标死——
        // 远端可能已提交,离开投递流等于放弃后续收敛。只有确定性失败进入预算裁决。
        if report
            .first_error
            .as_ref()
            .is_some_and(|error| !error.is_transient())
            && report.published < pending.len()
        {
            let failed_id = pending[report.published].id;
            Self::record_failed_attempt(claim.connection(), failed_id, max_attempts).await?;
        }
        claim.release().await;
        Ok(report)
    }

    /// 业务作用：读取单个 lane 仍可投递的事件数量，供按 lane 的积压观测。
    ///
    /// 参数说明：
    /// - `channel`：lane 名。
    ///
    /// 返回：基于 `(channel, dispatched, dead, id)` 索引的该 lane 非死信积压；
    /// 通道名非法或数据库失败返回脱敏错误。
    pub async fn pending_count_channel(&self, channel: &str) -> Result<u64, OutboxStoreError> {
        if !valid_channel_name(channel) {
            return Err(OutboxStoreError::new("invalid outbox channel name"));
        }
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM outbox_event \
             WHERE channel = ? AND dispatched = 0 AND dead = 0",
        )
        .bind(channel)
        .fetch_one(conn.as_mut())
        .await
        .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }
}

/// 业务作用：取得进程唯一的提交代际发布端，供写侧和受管 dispatcher 共享唤醒事实。
///
/// 参数说明: 无。
///
/// 返回：惰性创建且进程内稳定的 watch 发布端。
fn commit_signal() -> &'static watch::Sender<u64> {
    COMMIT_SIGNAL.get_or_init(|| watch::channel(0).0)
}

/// 业务作用：在数据库提交已经确定后推进代际，缩短持久化事件进入 dispatcher 的等待时间。
///
/// 参数说明: 无。
///
/// 返回：无；接收端可合并连续代际，真实待投递集合始终以数据库为准。
fn notify_committed_append() {
    commit_signal().send_modify(|generation| {
        *generation = generation.wrapping_add(1);
    });
}

/// 业务作用：把 Outbox 唤醒挂到 ambient transaction 的提交确认之后，防止未提交事件提前触发投递。
///
/// 参数说明: 无。
///
/// 返回：成功登记时返回 `Ok`；事务上下文意外丢失时返回脱敏错误并让调用方回滚本次写入。
fn register_commit_notification() -> Result<(), OutboxStoreError> {
    natx::after_commit(|| async {
        notify_committed_append();
    })
    .map_err(map_err)
}

/// 业务作用：把任意底层错误映射为脱敏的 [`OutboxStoreError`]，防止 SQL、凭据或 payload 外泄。
///
/// 参数说明：
/// - `_error`：只用于丢弃敏感细节的底层错误。
///
/// 返回：固定数据库失败分类。
pub(crate) fn map_err<E>(_error: E) -> OutboxStoreError {
    OutboxStoreError::new("database error")
}
