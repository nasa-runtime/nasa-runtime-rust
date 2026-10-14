//! 单一可写 MySQL 主节点的共享进度；故障切换必须由外部保证旧主失去写权限。

use super::{
    increment, validate_task, wire, CheckpointError as Error, CheckpointLease,
    CheckpointResult as Result, CheckpointStore, ReplayCheckpoint, ReplayPosition,
};
use sha2::{Digest, Sha256};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlSslMode},
    ConnectOptions, Connection, MySqlConnection, Row,
};
use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

const TABLE: &str = "nasa_kafka_checkpoint";
const DDL: &str = "CREATE TABLE IF NOT EXISTS nasa_kafka_checkpoint (namespace VARBINARY(128) NOT NULL, task_id VARBINARY(128) NOT NULL, generation BIGINT NOT NULL, revision BIGINT NOT NULL, body MEDIUMBLOB NOT NULL, PRIMARY KEY(namespace,task_id)) ENGINE=InnoDB";

/// 使用专用物理会话与命名锁的共享 checkpoint。同步 API 由有界独立线程桥接 SQL，调用方仍须安排阻塞预算。
/// SQL 权限检查不能原子撤销 Kafka 或其它系统中的在途副作用；多主写入不在合同范围内。
#[derive(Clone)]
pub struct MySqlCheckpointStore {
    config: Arc<Config>,
    active: Arc<AtomicUsize>,
}

/// 冻结端点和资源边界，不提供可能输出连接凭据的 Debug 实现。
struct Config {
    options: MySqlConnectOptions,
    database: String,
    namespace: String,
    timeout: Duration,
    capacity: usize,
}

impl MySqlCheckpointStore {
    /// 业务作用：冻结单主连接与资源边界，阻止旧 lease 重连后复用权限。
    /// 参数说明：url 为 mysql:// URL，仅接受 ssl-mode、ssl-ca、ssl-cert、ssl-key 查询项；TLS 默认 VERIFY_IDENTITY，明文须显式 DISABLED；namespace 为隔离名；timeout 为 100ms 至 60s 的单次预算；capacity 为 1 至 1024 个实际会话额度。
    /// 返回：未建连的存储；非法端点、未知参数、重复参数及不安全 TLS 降级模式拒绝。
    pub fn new(url: &str, namespace: &str, timeout: Duration, capacity: usize) -> Result<Self> {
        validate_task(namespace)?;
        let parsed = url::Url::parse(url).map_err(|_| Error::InvalidConfiguration)?;
        let database = parsed
            .path()
            .strip_prefix('/')
            .ok_or(Error::InvalidConfiguration)?;
        if parsed.scheme() != "mysql"
            || parsed.host_str().is_none()
            || parsed.fragment().is_some()
            || parsed.port() == Some(0)
            || database.is_empty()
            || database.len() > 64
            || !database
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'-'))
            || !(Duration::from_millis(100)..=Duration::from_secs(60)).contains(&timeout)
            || !(1..=1024).contains(&capacity)
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut keys = HashSet::new();
        let mut mode = MySqlSslMode::VerifyIdentity;
        for (key, value) in parsed.query_pairs() {
            if !keys.insert(key.to_string())
                || !matches!(key.as_ref(), "ssl-mode" | "ssl-ca" | "ssl-cert" | "ssl-key")
            {
                return Err(Error::InvalidConfiguration);
            }
            if key == "ssl-mode" {
                mode = match value.as_ref() {
                    "VERIFY_IDENTITY" => MySqlSslMode::VerifyIdentity,
                    "DISABLED" => MySqlSslMode::Disabled,
                    _ => return Err(Error::InvalidConfiguration),
                };
            }
        }
        let options = MySqlConnectOptions::from_str(url)
            .map_err(|_| Error::InvalidConfiguration)?
            .ssl_mode(mode)
            .statement_cache_capacity(0)
            .disable_statement_logging();
        Ok(Self {
            config: Arc::new(Config {
                options,
                database: database.to_owned(),
                namespace: namespace.to_owned(),
                timeout,
                capacity,
            }),
            active: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 业务作用：显式建立固定 InnoDB 表并核对持久化合同，普通任务不隐式运行 DDL。
    /// 参数说明：无。返回：表结构和持久设置匹配才成功；DDL 响应未知时可再次显式调用确认。
    pub fn initialize(&self) -> Result<()> {
        let (mut session, _) = self.start(Start::Initialize)?;
        session.close()
    }

    /// 业务作用：为建连与实际会话占用同一额度，线程退出以前不归还责任。
    /// 参数说明：operation 为初始化、读取或独占 claim。
    /// 返回：启动结果和唯一会话句柄；无容量立即拒绝，调用超时不撤销可能已经持久化的写入。
    fn start(&self, operation: Start) -> Result<(Session, Option<ReplayCheckpoint>)> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.config.capacity).then_some(n + 1)
            })
            .map_err(|_| Error::Capacity)?;
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let guard = ExitGuard {
            active: self.active.clone(),
            exit: exit.clone(),
        };
        let config = self.config.clone();
        let timeout_error = if matches!(operation, Start::Load(_)) {
            Error::Io
        } else {
            Error::OutcomeUnknown
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        let (reply, ready) = mpsc::sync_channel(1);
        let deadline = Instant::now() + config.timeout;
        std::thread::Builder::new()
            .name("nafka-checkpoint-mysql".into())
            .spawn(move || {
                // 守卫先构造、最后析构，只有连接和运行时均退出以后才公开完成并释放额度。
                let _guard = guard;
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = reply.send(Err(Error::Io));
                        return;
                    }
                };
                worker(&runtime, &config, operation, receiver, reply, deadline);
            })
            .map_err(|_| Error::Io)?;
        let mut session = Session {
            sender: Some(sender),
            exit,
            timeout: self.config.timeout,
        };
        match ready.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(Ok(snapshot)) => Ok((session, snapshot)),
            Ok(Err(error)) => {
                let _ = session.close();
                Err(error)
            }
            Err(_) => {
                session.sender.take();
                Err(timeout_error)
            }
        }
    }
}

impl CheckpointStore for MySqlCheckpointStore {
    /// 业务作用：只读取得完整共享进度，不授予锁或业务推进权。
    /// 参数说明：task_id 为精确任务名。返回：可信快照或不存在；损坏、超限和读取失败拒绝。
    fn load(&self, task_id: &str) -> Result<Option<ReplayCheckpoint>> {
        validate_task(task_id)?;
        let (mut session, snapshot) = self.start(Start::Load(task_id.to_owned()))?;
        session.close()?;
        Ok(snapshot)
    }

    /// 业务作用：以独占 SQL 会话持久化新的任务代次，完成以后才交付 lease。
    /// 参数说明：initial 为所有权、序号和进度均处于初始位置的冻结声明。
    /// 返回：完整持久化的唯一责任；已有所有者、合同改变或证据不足时拒绝。
    fn claim(&self, initial: &ReplayCheckpoint) -> Result<Box<dyn CheckpointLease>> {
        initial.validate()?;
        if initial.generation != 0
            || initial.revision != 0
            || initial
                .positions
                .iter()
                .any(|p| p.next_offset != p.start_offset)
        {
            return Err(Error::InvalidConfiguration);
        }
        let (session, snapshot) = self.start(Start::Claim(initial.clone()))?;
        Ok(Box::new(Lease {
            session,
            current: snapshot.ok_or(Error::Io)?,
            fenced: false,
            closed: false,
        }))
    }
}

/// 启动操作拥有完整输入，不从调用线程借用可能提前退出的内存。
enum Start {
    Initialize,
    Load(String),
    Claim(ReplayCheckpoint),
}
/// 每条命令带独立回复和绝对截止点；只有 lease 的串行调用方可提交。
struct Command {
    next: Option<ReplayCheckpoint>,
    deadline: Instant,
    reply: SyncSender<Result<ReplayCheckpoint>>,
}
/// 退出证据在连接与运行时退出后发布，即使调用方已放弃等待也保留资源额度。
struct ExitGuard {
    active: Arc<AtomicUsize>,
    exit: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for ExitGuard {
    /// 业务作用：在实际线程资源退出后归还容量并唤醒关闭等待者。
    /// 参数说明：无。返回：只发布资源退出，不推断未知 SQL 是否曾经提交。
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        *self
            .exit
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.exit.1.notify_all();
    }
}

/// 有界消息入口与实际退出证据，不持有可自动重连的连接池。
struct Session {
    sender: Option<SyncSender<Command>>,
    exit: Arc<(Mutex<bool>, Condvar)>,
    timeout: Duration,
}

impl Session {
    /// 业务作用：串行请求一次复验或条件写入，等待超时后永久断开旧入口。
    /// 参数说明：next 为待推进快照，空值表示只复验。
    /// 返回：服务端确认的快照；写入等待未知不会被当作未执行。
    fn request(&mut self, next: Option<ReplayCheckpoint>) -> Result<ReplayCheckpoint> {
        let error = if next.is_some() {
            Error::OutcomeUnknown
        } else {
            Error::Io
        };
        let (reply, receive) = mpsc::sync_channel(1);
        let deadline = Instant::now() + self.timeout;
        let sender = self.sender.as_ref().ok_or(Error::Fenced)?;
        if sender
            .try_send(Command {
                next,
                deadline,
                reply,
            })
            .is_err()
        {
            self.sender.take();
            return Err(Error::Fenced);
        }
        match receive.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result,
            Err(_) => {
                self.sender.take();
                Err(error)
            }
        }
    }

    /// 业务作用：禁止新命令并等待持锁连接真正退出，重复关闭可以继续等待原责任。
    /// 参数说明：无。返回：线程及本地 SQL 资源均退出才成功；超时保留独立退出证据。
    fn close(&mut self) -> Result<()> {
        self.sender.take();
        let state = self
            .exit
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .exit
            .1
            .wait_timeout_while(state, self.timeout, |exited| !*exited)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state {
            Ok(())
        } else {
            Err(Error::Io)
        }
    }
}

/// lease 的历史快照与会话权威分离，未知持久化之后不得复用旧序号。
struct Lease {
    session: Session,
    current: ReplayCheckpoint,
    fenced: bool,
    closed: bool,
}

impl CheckpointLease for Lease {
    /// 业务作用：提供最后确认的历史进度，不访问 SQL 或重新授予权限。
    /// 参数说明：无。返回：独立不可变事实副本。
    fn checkpoint(&self) -> ReplayCheckpoint {
        self.current.clone()
    }

    /// 业务作用：在新业务派发前验证当前会话、锁所有者及完整持久快照。
    /// 参数说明：无。返回：全部证据成立才成功，任何存储失败永久保护旧 lease。
    fn verify(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        if self.fenced {
            return Err(Error::Fenced);
        }
        match self.session.request(None) {
            Ok(snapshot) if snapshot == self.current => Ok(()),
            Ok(_) => {
                self.fenced = true;
                Err(Error::Fenced)
            }
            Err(error) => {
                self.fenced = true;
                Err(error)
            }
        }
    }

    /// 业务作用：目标确认之后以完整证据条件推进，写入确认前不发布内存进度。
    /// 参数说明：expected_revision 为预期序号，positions 为完整且不回退的进度。
    /// 返回：真实确认的新快照；未知结果关闭旧权限，输入拒绝不写库。
    fn advance(
        &mut self,
        expected_revision: u64,
        positions: Vec<ReplayPosition>,
    ) -> Result<ReplayCheckpoint> {
        if self.closed {
            return Err(Error::Closed);
        }
        if self.fenced {
            return Err(Error::Fenced);
        }
        if expected_revision != self.current.revision {
            return Err(Error::StaleRevision);
        }
        let next = ReplayCheckpoint::new(
            self.current.task_id.clone(),
            self.current.contract_fingerprint.clone(),
            self.current.generation,
            increment(self.current.revision)?,
            positions,
        )?;
        if !self.current.same_contract(&next) {
            return Err(Error::ContractMismatch);
        }
        if self
            .current
            .positions
            .iter()
            .zip(&next.positions)
            .any(|(old, new)| new.next_offset < old.next_offset)
        {
            return Err(Error::InvalidConfiguration);
        }
        match self.session.request(Some(next)) {
            Ok(snapshot) => {
                self.current = snapshot;
                Ok(self.current.clone())
            }
            Err(error) => {
                self.fenced = true;
                Err(error)
            }
        }
    }

    /// 业务作用：先永久关闭业务推进，再等待专用会话退出并归还命名锁。
    /// 参数说明：无。返回：实际本地退出后成功；服务端失联会话仍由服务端超时或运维结束。
    fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.session.close()
    }
}

/// 业务作用：在独立有界线程上持有唯一物理 SQL 连接，避免同步 API 嵌套调用 Tokio 运行时。
/// 参数说明：runtime 为线程私有运行时，config 为冻结配置，start 为首个操作，commands 为串行入口，ready 为启动回复，deadline 为首次截止点。
/// 返回：所有命令与真实本地连接退出后结束，命令超时不会触发自动重连。
fn worker(
    runtime: &tokio::runtime::Runtime,
    config: &Config,
    start: Start,
    commands: Receiver<Command>,
    ready: SyncSender<Result<Option<ReplayCheckpoint>>>,
    deadline: Instant,
) {
    let mut connection = match runtime.block_on(async {
        tokio::time::timeout_at(deadline.into(), config.options.connect()).await
    }) {
        Ok(Ok(connection)) => connection,
        _ => {
            let _ = ready.send(Err(Error::Io));
            return;
        }
    };
    let is_claim = matches!(start, Start::Claim(_));
    let startup = runtime.block_on(async {
        tokio::time::timeout_at(deadline.into(), async {
            validate_server(&mut connection, config).await?;
            if matches!(start, Start::Initialize) {
                sqlx::query(DDL)
                    .execute(&mut connection)
                    .await
                    .map_err(|_| Error::OutcomeUnknown)?;
            }
            validate_schema(&mut connection, config).await?;
            match &start {
                Start::Initialize => Ok(None),
                Start::Load(task) => read(&mut connection, config, task).await,
                Start::Claim(initial) => claim(&mut connection, config, initial).await.map(Some),
            }
        })
        .await
        .map_err(|_| {
            if matches!(start, Start::Load(_)) {
                Error::Io
            } else {
                Error::OutcomeUnknown
            }
        })?
    });
    let mut current = startup.as_ref().ok().and_then(Clone::clone);
    let succeeded = startup.is_ok();
    let delivered = ready.send(startup).is_ok();
    let accepted = succeeded && delivered;
    if let (true, Some(snapshot)) = (accepted && is_claim, current.as_mut()) {
        let lock = lock_name(config, &snapshot.task_id);
        let connection_id: Result<u64> = runtime.block_on(async {
            tokio::time::timeout(
                config.timeout,
                sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS UNSIGNED)")
                    .fetch_one(&mut connection),
            )
            .await
            .map_err(|_| Error::Io)?
            .map_err(|_| Error::Io)
        });
        if let Ok(connection_id) = connection_id {
            while let Ok(command) = commands.recv() {
                let write = command.next.is_some();
                let result = runtime.block_on(async {
                    tokio::time::timeout_at(command.deadline.into(), async {
                        verify(&mut connection, config, &lock, connection_id, snapshot).await?;
                        if let Some(next) = command.next {
                            persist(
                                &mut connection,
                                config,
                                &lock,
                                connection_id,
                                Some(snapshot),
                                &next,
                            )
                            .await?;
                            // 只有完整写入响应确认，工作线程才公开新内存事实。
                            *snapshot = next;
                        }
                        Ok(snapshot.clone())
                    })
                    .await
                    .map_err(|_| {
                        if write {
                            Error::OutcomeUnknown
                        } else {
                            Error::Io
                        }
                    })?
                });
                let failed = result.is_err();
                if command.reply.send(result).is_err() || failed {
                    break;
                }
            }
        }
    }
    // 即使等待者离开，连接仍由此线程明确关闭；关闭预算耗尽则丢弃实际 socket，随后才释放容量。
    runtime.block_on(async {
        let _ = tokio::time::timeout(config.timeout, connection.close()).await;
    });
}

/// 业务作用：拒绝只读节点、非 MySQL 服务与不足的持久化设置。
/// 参数说明：connection 为专用会话，config 为冻结数据库。
/// 返回：单主自动提交及日志持久合同成立才允许任务读写。
async fn validate_server(connection: &mut MySqlConnection, config: &Config) -> Result<()> {
    let row = sqlx::query("SELECT DATABASE() AS db, VERSION() AS version, CAST(@@global.read_only AS SIGNED) AS ro, CAST(@@global.super_read_only AS SIGNED) AS sro, CAST(@@global.innodb_flush_log_at_trx_commit AS SIGNED) AS flush_log, CAST(@@global.log_bin AS SIGNED) AS log_bin, CAST(@@global.sync_binlog AS SIGNED) AS sync_binlog, @@session.binlog_format AS binlog_format, CAST(@@session.autocommit AS SIGNED) AS autocommit")
        .fetch_one(connection).await.map_err(|_| Error::Io)?;
    let version: String = row.try_get("version").map_err(|_| Error::Io)?;
    let valid = row.try_get::<String, _>("db").map_err(|_| Error::Io)? == config.database
        && !version.to_ascii_lowercase().contains("mariadb")
        && version
            .split('.')
            .next()
            .and_then(|v| v.parse::<u32>().ok())
            .is_some_and(|v| v >= 8)
        && row.try_get::<i64, _>("ro").map_err(|_| Error::Io)? == 0
        && row.try_get::<i64, _>("sro").map_err(|_| Error::Io)? == 0
        && row.try_get::<i64, _>("flush_log").map_err(|_| Error::Io)? == 1
        && row.try_get::<i64, _>("autocommit").map_err(|_| Error::Io)? == 1
        && (row.try_get::<i64, _>("log_bin").map_err(|_| Error::Io)? == 0
            || (row
                .try_get::<i64, _>("sync_binlog")
                .map_err(|_| Error::Io)?
                == 1
                && row
                    .try_get::<String, _>("binlog_format")
                    .map_err(|_| Error::Io)?
                    == "ROW"));
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidConfiguration)
    }
}

/// 业务作用：拒绝改变完整 CAS 行为的表结构、索引、触发器或存储引擎。
/// 参数说明：connection 为专用会话，config 为固定数据库。
/// 返回：与共同固定表合同逐项匹配时成功，缺表或变更明确拒绝。
async fn validate_schema(connection: &mut MySqlConnection, config: &Config) -> Result<()> {
    let engines: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
    )
    .bind(&config.database)
    .bind(TABLE)
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Error::Io)?;
    if engines != vec![(Some("InnoDB".into()),)] {
        return Err(Error::InvalidConfiguration);
    }
    let columns: Vec<(String, String, String, String)> = sqlx::query_as("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION")
        .bind(&config.database).bind(TABLE).fetch_all(&mut *connection).await.map_err(|_| Error::Io)?;
    let expected = [
        ("namespace", "varbinary(128)"),
        ("task_id", "varbinary(128)"),
        ("generation", "bigint"),
        ("revision", "bigint"),
        ("body", "mediumblob"),
    ];
    if columns.len() != expected.len()
        || columns
            .iter()
            .zip(expected)
            .any(|((name, kind, nullable, extra), (n, k))| {
                name != n || kind != k || nullable != "NO" || !extra.is_empty()
            })
    {
        return Err(Error::InvalidConfiguration);
    }
    let keys: Vec<(String, String, Option<u64>)> = sqlx::query_as("SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX")
        .bind(&config.database).bind(TABLE).fetch_all(&mut *connection).await.map_err(|_| Error::Io)?;
    if keys
        != vec![
            ("PRIMARY".into(), "namespace".into(), None),
            ("PRIMARY".into(), "task_id".into(), None),
        ]
    {
        return Err(Error::InvalidConfiguration);
    }
    let triggers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?")
        .bind(&config.database).bind(TABLE).fetch_one(connection).await.map_err(|_| Error::Io)?;
    if triggers != 0 {
        return Err(Error::InvalidConfiguration);
    }
    Ok(())
}

/// 业务作用：限定返回正文大小并核对行元数据，避免损坏行参与恢复。
/// 参数说明：connection 为专用会话，config 为隔离配置，task 为精确任务 ID。
/// 返回：通过完整性验证的快照或不存在，非法元数据及正文拒绝。
async fn read(
    connection: &mut MySqlConnection,
    config: &Config,
    task: &str,
) -> Result<Option<ReplayCheckpoint>> {
    let row = sqlx::query("SELECT generation,revision,CAST(OCTET_LENGTH(body) AS UNSIGNED) AS size,CASE WHEN OCTET_LENGTH(body)<=2097152 THEN body ELSE NULL END AS body FROM nasa_kafka_checkpoint WHERE namespace=? AND task_id=?")
        .bind(config.namespace.as_bytes()).bind(task.as_bytes()).fetch_optional(connection).await.map_err(|_| Error::Io)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let size: u64 = row.try_get("size").map_err(|_| Error::Corrupt)?;
    if size == 0 || size > wire::MAX_BYTES as u64 {
        return Err(Error::Corrupt);
    }
    let body: Vec<u8> = row.try_get("body").map_err(|_| Error::Corrupt)?;
    let generation: i64 = row.try_get("generation").map_err(|_| Error::Corrupt)?;
    let revision: i64 = row.try_get("revision").map_err(|_| Error::Corrupt)?;
    let checkpoint = wire::decode(&body)?;
    if body.len() as u64 != size
        || generation <= 0
        || revision < 0
        || checkpoint.task_id != task
        || checkpoint.generation != generation as u64
        || checkpoint.revision != revision as u64
    {
        return Err(Error::Corrupt);
    }
    Ok(Some(checkpoint))
}

/// 业务作用：取得即时命名锁并持久化新代次，旧进度与合同不能在接管时被覆盖。
/// 参数说明：connection 为专用物理会话，config 为隔离配置，initial 为已验证初始合同。
/// 返回：新代次持久化成立的快照，竞争不等待，任何写入未知拒绝发布权限。
async fn claim(
    connection: &mut MySqlConnection,
    config: &Config,
    initial: &ReplayCheckpoint,
) -> Result<ReplayCheckpoint> {
    let lock = lock_name(config, &initial.task_id);
    // GET_LOCK 为会话权限，必须与后续读取和条件写入使用同一物理连接。
    let (acquired, connection_id): (Option<i64>, u64) =
        sqlx::query_as("SELECT CAST(GET_LOCK(?,0) AS SIGNED),CAST(CONNECTION_ID() AS UNSIGNED)")
            .bind(&lock)
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| Error::Io)?;
    match acquired {
        Some(1) => (),
        Some(0) => return Err(Error::Busy),
        _ => return Err(Error::Io),
    }
    let previous = read(connection, config, &initial.task_id).await?;
    let next = if let Some(previous) = &previous {
        if !initial.same_contract(previous) {
            return Err(Error::ContractMismatch);
        }
        ReplayCheckpoint::new(
            previous.task_id.clone(),
            previous.contract_fingerprint.clone(),
            increment(previous.generation)?,
            increment(previous.revision)?,
            previous.positions.clone(),
        )?
    } else {
        ReplayCheckpoint::new(
            initial.task_id.clone(),
            initial.contract_fingerprint.clone(),
            1,
            0,
            initial.positions.clone(),
        )?
    };
    // claim 也使用完整条件写入，新权限在持久化响应确认前不可见。
    persist(
        connection,
        config,
        &lock,
        connection_id,
        previous.as_ref(),
        &next,
    )
    .await?;
    Ok(next)
}

/// 业务作用：验证当前会话身份、命名锁与完整持久证据，历史快照不能代替当前权限。
/// 参数说明：connection/config 为专用端点，lock/connection_id 为原权威，current 为最后确认进度。
/// 返回：全部证据仍成立才成功；查询失败或改变均阻止下一次业务副作用。
async fn verify(
    connection: &mut MySqlConnection,
    config: &Config,
    lock: &str,
    connection_id: u64,
    current: &ReplayCheckpoint,
) -> Result<()> {
    validate_server(connection, config).await?;
    let (actual, owner): (u64, Option<u64>) = sqlx::query_as(
        "SELECT CAST(CONNECTION_ID() AS UNSIGNED),CAST(IS_USED_LOCK(?) AS UNSIGNED)",
    )
    .bind(lock)
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Error::Io)?;
    if actual != connection_id || owner != Some(connection_id) {
        return Err(Error::Fenced);
    }
    if read(connection, config, &current.task_id).await?.as_ref() != Some(current) {
        return Err(Error::Fenced);
    }
    Ok(())
}

/// 业务作用：在同一 SQL 中约束会话权限及完整旧正文，只修改一个精确任务。
/// 参数说明：connection/config 为存储会话，lock/connection_id 为原权威，previous 为预期快照或首次空值，next 为下一快照。
/// 返回：恰好一行确认后成功，零行失权，执行响应不确定为 OutcomeUnknown。
async fn persist(
    connection: &mut MySqlConnection,
    config: &Config,
    lock: &str,
    connection_id: u64,
    previous: Option<&ReplayCheckpoint>,
    next: &ReplayCheckpoint,
) -> Result<()> {
    let bytes = wire::encode(next)?;
    let result = if let Some(previous) = previous {
        sqlx::query("UPDATE nasa_kafka_checkpoint SET generation=?,revision=?,body=? WHERE namespace=? AND task_id=? AND generation=? AND revision=? AND body=? AND CONNECTION_ID()=? AND IS_USED_LOCK(?)=CONNECTION_ID()")
            .bind(next.generation as i64).bind(next.revision as i64).bind(bytes)
            .bind(config.namespace.as_bytes()).bind(next.task_id.as_bytes()).bind(previous.generation as i64)
            .bind(previous.revision as i64).bind(wire::encode(previous)?).bind(connection_id).bind(lock)
            .execute(connection).await
    } else {
        sqlx::query("INSERT INTO nasa_kafka_checkpoint(namespace,task_id,generation,revision,body) SELECT ?,?,?,?,? WHERE CONNECTION_ID()=? AND IS_USED_LOCK(?)=CONNECTION_ID()")
            .bind(config.namespace.as_bytes()).bind(next.task_id.as_bytes()).bind(next.generation as i64)
            .bind(next.revision as i64).bind(bytes).bind(connection_id).bind(lock).execute(connection).await
    }.map_err(|_| Error::OutcomeUnknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::Fenced);
    }
    Ok(())
}

/// 业务作用：为数据库、表、命名空间及任务产生双端一致且无歧义的锁身份。
/// 参数说明：config 为冻结数据库与隔离名，task 为已验证任务 ID。
/// 返回：64 字符 SHA-256 小写摘要，不在诊断中暴露业务名称。
fn lock_name(config: &Config, task: &str) -> String {
    let mut bytes = b"NSKMYL01".to_vec();
    for part in [&config.database, TABLE, &config.namespace, task] {
        bytes.extend_from_slice(&(part.len() as u16).to_be_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
