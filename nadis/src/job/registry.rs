//! 执行器注册表：登记本节点的 Worker 能力与存活截止点，续期心跳，停机注销，并回收过期执行器。
//!
//! 执行器身份是持久线协议：`nodeIdentity = applicationName:instanceIdentity`，`executorId =
//! nodeIdentity:startupId`。同一 Worker 合同（schemaId + wireCodecs）不能按节点启动顺序覆盖，冲突即封闭
//! 新快照。`runtime`、`implementationDigest` 与构建信息只用于观测，不参与兼容判定，因此本实现以 `rust`
//! 标记运行时而不破坏与其它实现共享同一 registry 的能力。

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::identifiers::{snapshot_digest, worker_key};
use crate::job::keyspace::JobKeyspace;
use crate::job::model::JobExecutorState;
use crate::job::names::require_name;
use crate::job::script::{eval, eval_retry_safe, value_to_string, JobScript};

/// 登记能力的重路径脚本。
static EXECUTOR_REGISTER: JobScript =
    JobScript::new(include_str!("lua/executor_register_capabilities.lua"));
/// 续期执行器与能力的心跳脚本。
static EXECUTOR_HEARTBEAT: JobScript = JobScript::new(include_str!("lua/executor_heartbeat.lua"));
/// 优雅停机注销脚本。
static EXECUTOR_UNREGISTER: JobScript = JobScript::new(include_str!("lua/executor_unregister.lua"));
/// 撤销单个 Worker 能力的脚本。
static EXECUTOR_REMOVE_CAPABILITY: JobScript =
    JobScript::new(include_str!("lua/executor_remove_capability.lua"));
/// 回收过期执行器的注册表 GC 脚本。
static REGISTRY_GC: JobScript = JobScript::new(include_str!("lua/registry_gc.lua"));
/// 冻结兼容能力快照的脚本。
static FANOUT_SNAPSHOT: JobScript = JobScript::new(include_str!("lua/fanout_snapshot.lua"));
/// 累积跨 Fanout 节点失联证据的脚本。
static EXECUTOR_RECORD_FANOUT_EVIDENCE: JobScript =
    JobScript::new(include_str!("lua/executor_record_fanout_evidence.lua"));

/// 本进程执行器身份；节点身份与启动代次共同确定 registry 中的唯一执行器记录。
#[derive(Debug, Clone)]
pub struct ExecutorIdentity {
    application_name: String,
    node_identity: String,
    executor_id: String,
    startup_id: String,
}

impl ExecutorIdentity {
    /// 业务作用：以应用名、稳定实例身份与本次启动代次构造执行器身份；任一部分非法即拒绝，避免非法身份进入线协议。
    ///
    /// 参数说明：
    /// - `application_name`: 稳定应用名。
    /// - `instance_identity`: 稳定实例身份，跨重启不变。
    /// - `startup_id`: 本次进程启动的唯一标识。
    ///
    /// 返回：三部分均通过名称合同时返回身份；任一非法返回配置错误。
    pub fn new(application_name: &str, instance_identity: &str, startup_id: &str) -> Result<Self> {
        let application_name = require_name(application_name, "applicationName")?;
        let instance_identity = require_name(instance_identity, "instanceIdentity")?;
        let startup_id = require_name(startup_id, "startupId")?;
        let node_identity = format!("{application_name}:{instance_identity}");
        let executor_id = format!("{node_identity}:{startup_id}");
        Ok(Self {
            application_name,
            node_identity,
            executor_id,
            startup_id,
        })
    }

    /// 业务作用：从 Job 配置与本次启动代次构造执行器身份；独立开发入口可使用环境主机名作为受限降级。
    ///
    /// 参数说明：
    /// - `config`: Job 配置，提供应用名与实例身份。
    /// - `startup_id`: 本次进程启动的唯一标识。
    ///
    /// 返回：显式身份或可用主机身份合法时返回；两者均缺失或非法时返回配置错误。受管入口在调用前始终要求显式稳定身份。
    pub fn from_config(config: &JobConfig, startup_id: &str) -> Result<Self> {
        let configured = config.instance_identity.trim();
        if !configured.is_empty() {
            return Self::new(&config.application_name, configured, startup_id);
        }
        // 核心独立入口允许开发环境使用显式进程环境中的主机身份；受管 napp 在进入核心前仍强制配置稳定实例名。
        let fallback = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .map_err(|_| {
                NasaRedisError::from(crate::job::JobError::Config(
                    "job.instance_identity 为空且开发环境没有 HOSTNAME/COMPUTERNAME".to_owned(),
                ))
            })?;
        tracing::warn!(
            "独立 RedisJob runtime 使用主机名作为 instance_identity；生产部署应显式配置跨重启稳定身份"
        );
        Self::new(&config.application_name, fallback.trim(), startup_id)
    }

    /// 业务作用：返回稳定应用名。参数说明: 无。返回：应用名。
    pub fn application_name(&self) -> &str {
        &self.application_name
    }
    /// 业务作用：返回稳定节点身份。参数说明: 无。返回：`applicationName:instanceIdentity`。
    pub fn node_identity(&self) -> &str {
        &self.node_identity
    }
    /// 业务作用：返回本进程执行器身份。参数说明: 无。返回：`nodeIdentity:startupId`。
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }
    /// 业务作用：返回本次启动代次。参数说明: 无。返回：startupId。
    pub fn startup_id(&self) -> &str {
        &self.startup_id
    }
}

/// 能力登记的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterCapabilityOutcome {
    /// 登记成功：本节点存活截止、心跳修订号与权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 本节点存活截止毫秒；心跳前有效。
        expire_at: i64,
        /// 单调心跳修订号，用于 Fanout 失联证据复验。
        heartbeat_revision: i64,
    },
    /// workerKey 与其它 Worker 名冲突，拒绝混入同一能力池。
    WorkerKeyConflict,
    /// 同一 Worker 合同的 schemaId/wireCodecs 不一致，已封闭新快照。
    ContractMismatch,
}

/// 心跳续期的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    /// 续期成功：新的存活截止、心跳修订号与权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 顺延后的存活截止毫秒。
        expire_at: i64,
        /// 单调心跳修订号。
        heartbeat_revision: i64,
    },
    /// 执行器主记录已过期或被回收，需要重新登记能力。
    NotFound,
}

/// 注销的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnregisterOutcome {
    /// 注销成功（或重复注销的幂等成功）。
    Ok,
    /// 执行器记录不存在，按幂等成功处理。
    NotFound,
    /// 执行器记录已被同身份新进程覆盖，拒绝撤销他人记录。
    StaleExecutor,
}

/// 快照中的一个执行器成员；`runtime`/`implementation_digest` 仅观测，不参与兼容判定。
///
/// 序列化字段名与顺序（camelCase、声明序）与既有实现的记录组件逐字节一致，`snapshotPayload` 跨实现可解码。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorMember {
    /// 稳定节点身份。
    pub node_identity: String,
    /// 执行器身份（含启动代次）。
    pub executor_id: String,
    /// 本次启动代次。
    pub startup_id: String,
    /// 应用名。
    pub application_name: String,
    /// 运行时标记（观测用）。
    pub runtime: String,
    /// 实现摘要（观测用）。
    pub implementation_digest: String,
    /// 心跳修订号；Fanout 失联证据据此复验代次。
    pub heartbeat_revision: i64,
}

/// 冻结的兼容能力快照；同一 fanoutId 只绑定一个 `snapshot_id`，据此收敛跨节点重试。
///
/// 字段顺序与 camelCase 名与既有实现的记录序列化逐字节一致：`snapshotId`、`workerName`、`selectedAt`、
/// `members`、`snapshotDigest`。`wire_json` 产出即 Fanout 根 `snapshotPayload` 的 Base64 之前的原始字节，
/// 保证 Java 创建、其它实现恢复进行中 Fanout 时可解码。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSnapshot {
    /// 128 位快照标识（摘要前 32 hex）。
    pub snapshot_id: String,
    /// Worker 能力名。
    pub worker_name: String,
    /// 脚本选择快照的权威毫秒时刻。
    pub selected_at: i64,
    /// 按 `node_identity` 排序的成员集合。
    pub members: Vec<ExecutorMember>,
    /// 完整 256 位规范成员摘要。
    pub snapshot_digest: String,
}

impl ClusterSnapshot {
    /// 业务作用：产出与既有实现逐字节一致的快照 JSON，作为 Fanout 根 `snapshotPayload` 的原始字节（外层再 Base64）。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：紧凑无空白、字段按声明序、camelCase 命名的 JSON 字节；恒可序列化，失败视为内部不变量破坏。
    pub fn wire_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("cluster snapshot 序列化不应失败")
    }
}

/// 冻结能力快照的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// 快照成功，返回冻结的成员集合与标识。
    Ok(ClusterSnapshot),
    /// Worker 合同冲突或 schema/codec 不匹配，拒绝快照。
    ContractMismatch,
}

/// 记录跨 Fanout 节点失联证据的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordEvidenceOutcome {
    /// 已记录本根证据，尚未达门槛，返回累计证据数。
    Recorded {
        /// 当前累计的不同根证据数。
        count: i64,
    },
    /// 证据达门槛，节点已标记 Fanout 不就绪，返回证据数与冷却截止。
    MarkedUnready {
        /// 触发降级的证据数。
        count: i64,
        /// 不就绪冷却截止毫秒。
        until: i64,
    },
    /// 快照观测的启动代次已变化，拒绝旧证据。
    StaleStartup,
    /// 快照观测的心跳修订号已变化，拒绝旧证据。
    StaleHeartbeat,
    /// 目标节点当前无存活执行器，不记录证据。
    NotActive,
}

/// 一次注册表回收的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryGc {
    /// 本轮实际删除的执行器数量。
    pub deleted: i64,
    /// 本轮新进入 GC 候选（首次过期）的执行器数量。
    pub newly_expired: i64,
    /// 脚本观测到的权威当前毫秒。
    pub redis_now: i64,
}

/// 单个执行器的注册表客户端；持有连接、键模型、配置与身份，并跟踪本节点已登记的 Worker 能力集合。
pub struct ExecutorRegistry {
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
    identity: ExecutorIdentity,
    workers: Mutex<BTreeSet<String>>,
    known_workers: Mutex<BTreeSet<String>>,
}

impl ExecutorRegistry {
    /// 业务作用：绑定连接、键模型、配置与执行器身份，创建注册表客户端。
    ///
    /// 参数说明：
    /// - `client`: 承载注册表连接的 RedisClient。
    /// - `keyspace`: 冻结的 Job 键模型。
    /// - `config`: 已校验的 Job 配置，提供容量与失效时长。
    /// - `identity`: 本进程执行器身份。
    ///
    /// 返回：可登记能力、续期心跳与注销的注册表客户端。
    pub fn new(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        identity: ExecutorIdentity,
    ) -> Self {
        Self {
            client,
            keyspace,
            config,
            identity,
            workers: Mutex::new(BTreeSet::new()),
            known_workers: Mutex::new(BTreeSet::new()),
        }
    }

    /// 业务作用：返回本节点执行器身份，供 Dispatcher 与 Fanout 快照复用。参数说明: 无。返回：执行器身份引用。
    pub fn identity(&self) -> &ExecutorIdentity {
        &self.identity
    }

    /// 业务作用：登记执行器存活事实；只有 `FANOUT_ONLY` 定义同时发布 Worker 能力与合同并加入心跳集合。
    ///
    /// 参数说明：
    /// - `definition`: 本地任务定义，提供 Worker 名、契约修订号、schema 与线编码。
    /// - `state`: 本执行器当前状态（Active/Draining）。
    ///
    /// 返回：登记成功返回存活截止与心跳修订号；Fanout workerKey 冲突或合同不一致返回对应结局，普通定义不会进入 Fanout 快照。
    pub async fn register(
        &self,
        definition: &JobDefinition,
        state: JobExecutorState,
    ) -> Result<RegisterCapabilityOutcome> {
        let worker_name = definition.worker_name();
        let fanout_capable = definition.trigger() == crate::job::model::JobTrigger::FanoutOnly;
        let codecs = definition.wire_codecs();
        // runtime 标记与 implementationDigest 只用于观测，本实现用 "rust"，不参与兼容判定。
        let implementation_digest = worker_key(&format!(
            "{}:rust:{}",
            self.identity.application_name(),
            definition.name()
        ));
        // capabilityDigest == canonicalDigest：Worker 名、契约修订、schema 与线编码的规范摘要，跨实现必须一致。
        let capability_digest = worker_key(&format!(
            "{worker_name}:{}:{}:{codecs}",
            definition.contract_revision(),
            definition.schema_id()
        ));
        if fanout_capable {
            // 在写出前保存全量注销坐标；登记提交后回包丢失时，启动补偿仍能撤销该能力索引。
            self.known_workers
                .lock()
                .expect("registry known workers lock")
                .insert(worker_name.to_owned());
        }
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace
                .executor(self.identity.executor_id())
                .into_bytes(),
            self.keyspace.capability(worker_name).into_bytes(),
            self.keyspace.capability_meta(worker_name).into_bytes(),
            self.keyspace
                .contract(worker_name, definition.contract_revision())
                .into_bytes(),
            self.keyspace.worker_key_binding(worker_name).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            self.identity.executor_id().as_bytes().to_vec(),
            self.identity.node_identity().as_bytes().to_vec(),
            self.identity.startup_id().as_bytes().to_vec(),
            self.identity.application_name().as_bytes().to_vec(),
            b"rust".to_vec(),
            state.wire_name().as_bytes().to_vec(),
            worker_name.as_bytes().to_vec(),
            definition.contract_revision().to_string().into_bytes(),
            definition.schema_id().as_bytes().to_vec(),
            codecs.as_bytes().to_vec(),
            implementation_digest.into_bytes(),
            self.config.executor_capacity.to_string().into_bytes(),
            self.config.executor_expire_ms.to_string().into_bytes(),
            capability_digest.clone().into_bytes(),
            capability_digest.into_bytes(),
            if fanout_capable {
                b"1"
            } else {
                b"0"
            }
            .to_vec(),
        ];
        let raw = eval(&self.client, &EXECUTOR_REGISTER, &keys, &argv).await?;
        let outcome = interpret_register_capability(&raw)?;
        if fanout_capable && matches!(outcome, RegisterCapabilityOutcome::Ok { .. }) {
            self.workers
                .lock()
                .expect("registry workers lock")
                .insert(worker_name.to_owned());
        }
        Ok(outcome)
    }

    /// 业务作用：走轻量路径续期执行器及其全部已登记能力，并发布当前容量与 Fanout 就绪状态。
    ///
    /// 参数说明：
    /// - `state`: 本执行器当前状态；`Draining` 不再进入新能力快照。
    /// - `inflight`: 当前在途 Handler 数量，供容量观测。
    ///
    /// 返回：续期成功返回新存活截止与心跳修订号；主记录不存在返回 `NotFound`，此时必须重新登记。
    pub async fn heartbeat(
        &self,
        state: JobExecutorState,
        inflight: u32,
    ) -> Result<HeartbeatOutcome> {
        self.heartbeat_inner(state, inflight, None).await
    }

    /// 业务作用：以稳定逻辑请求与已确认 revision 发送可安全重发的执行器心跳。
    ///
    /// 参数说明：
    /// - `state`: 本执行器当前状态；`Draining` 不再进入新能力快照。
    /// - `inflight`: 当前在途 Handler 数量，供容量观测。
    /// - `request_id`: 同一逻辑心跳的稳定标识，传输重发时不得变化。
    /// - `expected_revision`: 调用方最后一次确认的心跳修订号。
    ///
    /// 返回：首次执行或同请求重放均返回同一续租结局；服务端记录已过期或 revision 不一致时拒绝续租。
    pub(crate) async fn heartbeat_idempotent(
        &self,
        state: JobExecutorState,
        inflight: u32,
        request_id: &str,
        expected_revision: i64,
    ) -> Result<HeartbeatOutcome> {
        self.heartbeat_inner(state, inflight, Some((request_id, expected_revision)))
            .await
    }

    /// 业务作用：组装执行器心跳键与参数，并按是否携带 fencing 证据选择结局分类。
    ///
    /// 参数说明：
    /// - `state`: 本执行器当前状态。
    /// - `inflight`: 当前在途 Handler 数量。
    /// - `idempotency`: 受监督运行时提供的逻辑请求 ID 与已确认 revision；低层单次调用不提供。
    ///
    /// 返回：脚本的封闭心跳结局；受监督调用保留可重发的 Redis 错误，低层调用在结局未知时阻止透明重放。
    async fn heartbeat_inner(
        &self,
        state: JobExecutorState,
        inflight: u32,
        idempotency: Option<(&str, i64)>,
    ) -> Result<HeartbeatOutcome> {
        let sorted: Vec<String> = self
            .workers
            .lock()
            .expect("registry workers lock")
            .iter()
            .cloned()
            .collect();
        let mut keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace
                .executor(self.identity.executor_id())
                .into_bytes(),
            self.keyspace
                .fanout_evidence(self.identity.node_identity())
                .into_bytes(),
        ];
        for worker in &sorted {
            keys.push(self.keyspace.capability(worker).into_bytes());
        }
        // Fanout 就绪仅在 Active 时请求；Draining 节点不应进入新快照。
        let fanout_ready = matches!(state, JobExecutorState::Active);
        let mut argv: Vec<Vec<u8>> = vec![
            self.identity.executor_id().as_bytes().to_vec(),
            state.wire_name().as_bytes().to_vec(),
            self.config.executor_expire_ms.to_string().into_bytes(),
            inflight.to_string().into_bytes(),
            if fanout_ready {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            },
        ];
        if let Some((request_id, expected_revision)) = idempotency {
            argv.push(request_id.as_bytes().to_vec());
            argv.push(expected_revision.to_string().into_bytes());
        }
        // 受监督心跳用同一请求 ID 重发，服务端只允许一次 revision 推进；低层单次调用没有该证据，
        // 写出后失联时必须维持结局未知，不能把新调用误当成同一逻辑请求。
        let raw = if idempotency.is_some() {
            eval_retry_safe(&self.client, &EXECUTOR_HEARTBEAT, &keys, &argv).await?
        } else {
            eval(&self.client, &EXECUTOR_HEARTBEAT, &keys, &argv).await?
        };
        interpret_heartbeat(&raw)
    }

    /// 业务作用：优雅停机时注销本执行器，并从全部 Worker 能力反向索引移除元数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：注销成功或重复注销返回 `Ok`；记录不存在返回 `NotFound`；被同身份新进程覆盖返回 `StaleExecutor`。
    pub async fn unregister(&self) -> Result<UnregisterOutcome> {
        let sorted: Vec<String> = self
            .known_workers
            .lock()
            .expect("registry known workers lock")
            .iter()
            .cloned()
            .collect();
        let mut keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace
                .executor(self.identity.executor_id())
                .into_bytes(),
            self.keyspace.registry_gc().into_bytes(),
        ];
        // 能力索引与成员元数据成对排列，脚本据此原子撤销每个能力的反向索引。
        for worker in &sorted {
            keys.push(self.keyspace.capability(worker).into_bytes());
            keys.push(self.keyspace.capability_meta(worker).into_bytes());
        }
        let raw = eval(
            &self.client,
            &EXECUTOR_UNREGISTER,
            &keys,
            &[self.identity.executor_id().as_bytes().to_vec()],
        )
        .await?;
        interpret_unregister(&raw)
    }

    /// 业务作用：撤销本执行器的单个 Worker 能力，使定义删除后新 Fanout 快照不再选择本节点。
    ///
    /// 参数说明：`worker_name` 为本地引用已归零的 Worker 能力名。
    ///
    /// 返回：本地心跳集合先永久移除该能力，Redis 成员与元数据原子撤销后成功；脚本或传输失败返回错误，
    /// 后续心跳仍不会重新发布旧能力，调用方可用相同 Worker 名安全重试。
    pub async fn remove_capability(&self, worker_name: &str) -> Result<()> {
        // 先从心跳集合移除，确保 Redis 撤销失败时也不会在下一次续期把已删除能力重新写回。
        self.workers
            .lock()
            .expect("registry workers lock")
            .remove(worker_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.capability(worker_name).into_bytes(),
            self.keyspace.capability_meta(worker_name).into_bytes(),
            self.keyspace
                .executor(self.identity.executor_id())
                .into_bytes(),
        ];
        let raw = eval(
            &self.client,
            &EXECUTOR_REMOVE_CAPABILITY,
            &keys,
            &[
                self.identity.executor_id().as_bytes().to_vec(),
                worker_name.as_bytes().to_vec(),
            ],
        )
        .await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        if code == "OK" {
            self.known_workers
                .lock()
                .expect("registry known workers lock")
                .remove(worker_name);
            Ok(())
        } else {
            Err(protocol("executor_remove_capability 返回未知码"))
        }
    }

    /// 业务作用：以两阶段宽限回收过期执行器主记录、能力索引与成员元数据，期间心跳恢复的成员不删除。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本轮删除数、新进入候选数与权威时刻；结构不符合脚本合同时 fail-closed。
    pub async fn registry_gc(&self) -> Result<RegistryGc> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace.registry_gc().into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            self.config.registry_gc_grace_ms.to_string().into_bytes(),
            self.config.scan_batch_size.to_string().into_bytes(),
            self.keyspace.registry_prefix().into_bytes(),
        ];
        let raw = eval(&self.client, &REGISTRY_GC, &keys, &argv).await?;
        interpret_registry_gc(&raw)
    }

    /// 业务作用：累积不同 Fanout 根对同一稳定节点启动代次的失联证据，达门槛后暂停其 Fanout 能力。
    ///
    /// 参数说明：
    /// - `node_identity`: 目标稳定节点身份。
    /// - `startup_id`/`heartbeat_revision`: 快照观测到的启动代次与心跳修订号，用于拒绝过期证据。
    /// - `fanout_id`: 产生本次证据的 Fanout 标识；同一 fanoutId 只计一次。
    ///
    /// 返回：达门槛返回 `MarkedUnready` 与冷却截止；未达返回 `Recorded`；启动代次/心跳变化或节点不活跃返回对应结局。
    pub async fn record_fanout_evidence(
        &self,
        node_identity: &str,
        startup_id: &str,
        heartbeat_revision: i64,
        fanout_id: &str,
    ) -> Result<RecordEvidenceOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace.fanout_evidence(node_identity).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            node_identity.as_bytes().to_vec(),
            startup_id.as_bytes().to_vec(),
            heartbeat_revision.to_string().into_bytes(),
            fanout_id.as_bytes().to_vec(),
            self.config
                .node_unready_evidence_count
                .to_string()
                .into_bytes(),
            self.config.executor_expire_ms.to_string().into_bytes(),
            self.config.registry_gc_grace_ms.to_string().into_bytes(),
            self.keyspace.registry_prefix().into_bytes(),
        ];
        let raw = eval(&self.client, &EXECUTOR_RECORD_FANOUT_EVIDENCE, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STALE_STARTUP" => RecordEvidenceOutcome::StaleStartup,
            "STALE_HEARTBEAT" => RecordEvidenceOutcome::StaleHeartbeat,
            "NOT_ACTIVE" => RecordEvidenceOutcome::NotActive,
            "RECORDED" => RecordEvidenceOutcome::Recorded {
                count: parse_i64_value(
                    raw.get(1)
                        .ok_or_else(|| protocol("record_evidence 缺少 count"))?,
                    "count",
                )?,
            },
            "MARKED_UNREADY" => RecordEvidenceOutcome::MarkedUnready {
                count: parse_i64_value(
                    raw.get(1)
                        .ok_or_else(|| protocol("record_evidence 缺少 count"))?,
                    "count",
                )?,
                until: parse_i64_value(
                    raw.get(2)
                        .ok_or_else(|| protocol("record_evidence 缺少 until"))?,
                    "until",
                )?,
            },
            _ => return Err(protocol("executor_record_fanout_evidence 返回未知码")),
        })
    }

    /// 业务作用：在 registry slot 冻结一份当前存活、就绪且合同精确兼容的执行器快照，并派生稳定 `snapshot_id`。
    ///
    /// 参数说明：
    /// - `worker_name`: 目标 Worker 能力名。
    /// - `contract_revision`: 契约修订号。
    /// - `schema_id`: schema 标识，必须与合同精确一致。
    /// - `codecs`: 请求的线编码名，必须在合同声明的编码集合内。
    ///
    /// 返回：兼容时返回冻结成员集合与快照标识；合同冲突或 schema/codec 不匹配返回 `ContractMismatch`。
    /// `snapshot_id` 由 Worker 名、选择时刻与按 `node_identity` 排序的成员规范串派生，跨实现字节稳定。
    pub async fn snapshot(
        &self,
        worker_name: &str,
        contract_revision: i64,
        schema_id: &str,
        codecs: &str,
    ) -> Result<SnapshotOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.executors().into_bytes(),
            self.keyspace.capability(worker_name).into_bytes(),
            self.keyspace.capability_meta(worker_name).into_bytes(),
            self.keyspace
                .contract(worker_name, contract_revision)
                .into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            worker_name.as_bytes().to_vec(),
            contract_revision.to_string().into_bytes(),
            schema_id.as_bytes().to_vec(),
            codecs.as_bytes().to_vec(),
            self.config.fanout_max_members.to_string().into_bytes(),
            self.keyspace.registry_prefix().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_SNAPSHOT, &keys, &argv).await?;
        interpret_snapshot(
            &raw,
            self.keyspace.qualifier(),
            self.keyspace.namespace(),
            worker_name,
        )
    }
}

/// 业务作用：把 fanout_snapshot 原始返回解释为冻结快照，并按既有规则派生稳定 `snapshot_id`。
///
/// 参数说明：
/// - `raw`: 脚本返回 `{OK, selectedAt, (7 字段/成员)*}` 或 `CONTRACT_MISMATCH`。
/// - `worker_name`: 参与摘要派生的 Worker 名。
///
/// 返回：兼容时返回排序成员与派生标识；`CONTRACT_MISMATCH` 返回对应结局；成员字段不齐或非数值 fail-closed。
fn interpret_snapshot(
    raw: &[redis::Value],
    qualifier: &str,
    namespace: &str,
    worker_name: &str,
) -> Result<SnapshotOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    if code == "CONTRACT_MISMATCH" {
        return Ok(SnapshotOutcome::ContractMismatch);
    }
    if code != "OK" {
        return Err(protocol("fanout_snapshot 返回未知码"));
    }
    let selected_at = parse_i64_value(
        raw.get(1)
            .ok_or_else(|| protocol("fanout_snapshot 缺少 selectedAt"))?,
        "selectedAt",
    )?;
    let body = &raw[2.min(raw.len())..];
    if !body.len().is_multiple_of(7) {
        return Err(protocol("fanout_snapshot 成员字段不完整"));
    }
    let mut members = Vec::with_capacity(body.len() / 7);
    for chunk in body.as_chunks::<7>().0 {
        members.push(ExecutorMember {
            node_identity: value_to_string(&chunk[0]),
            executor_id: value_to_string(&chunk[1]),
            startup_id: value_to_string(&chunk[2]),
            application_name: value_to_string(&chunk[3]),
            runtime: value_to_string(&chunk[4]),
            implementation_digest: value_to_string(&chunk[5]),
            heartbeat_revision: parse_i64_value(&chunk[6], "heartbeatRevision")?,
        });
    }
    // 成员按稳定节点身份排序，保证同一批成员在任意实现上派生同一 snapshot_id。
    members.sort_by(|a, b| a.node_identity.cmp(&b.node_identity));
    let canonical = members
        .iter()
        .flat_map(|member| {
            [
                member.node_identity.clone(),
                member.executor_id.clone(),
                member.heartbeat_revision.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let digest = snapshot_digest(qualifier, namespace, worker_name, selected_at, &canonical);
    let snapshot_id = digest[..32].to_owned();
    Ok(SnapshotOutcome::Ok(ClusterSnapshot {
        snapshot_id,
        worker_name: worker_name.to_owned(),
        selected_at,
        members,
        snapshot_digest: digest,
    }))
}

/// 业务作用：把 executor_register 原始返回解释为封闭结局；OK 缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回存活截止与心跳修订号；已知拒绝码返回对应结局；未知码返回协议错误。
fn interpret_register_capability(raw: &[redis::Value]) -> Result<RegisterCapabilityOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "WORKER_KEY_CONFLICT" => Ok(RegisterCapabilityOutcome::WorkerKeyConflict),
        "CONTRACT_MISMATCH" => Ok(RegisterCapabilityOutcome::ContractMismatch),
        "OK" => {
            let (redis_now, expire_at, heartbeat_revision) = parse_liveness(raw)?;
            Ok(RegisterCapabilityOutcome::Ok {
                redis_now,
                expire_at,
                heartbeat_revision,
            })
        }
        _ => Err(protocol("executor_register 返回未知码")),
    }
}

/// 业务作用：把 executor_heartbeat 原始返回解释为封闭结局；权威不一致、OK 缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回存活截止与心跳修订号；NOT_FOUND 返回对应结局；STALE_AUTHORITY 返回失权错误；未知码返回协议错误。
fn interpret_heartbeat(raw: &[redis::Value]) -> Result<HeartbeatOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "NOT_FOUND" => Ok(HeartbeatOutcome::NotFound),
        "OK" => {
            let (redis_now, expire_at, heartbeat_revision) = parse_liveness(raw)?;
            Ok(HeartbeatOutcome::Ok {
                redis_now,
                expire_at,
                heartbeat_revision,
            })
        }
        "STALE_AUTHORITY" => Err(crate::job::JobError::StaleOwner(
            "executor_heartbeat 的 heartbeatRevision 已被其它权威推进".to_owned(),
        )
        .into()),
        _ => Err(protocol("executor_heartbeat 返回未知码")),
    }
}

/// 业务作用：把 executor_unregister 原始返回解释为封闭结局；未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：已知返回码返回对应结局；未知码返回协议错误。
fn interpret_unregister(raw: &[redis::Value]) -> Result<UnregisterOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    Ok(match code.as_str() {
        "OK" => UnregisterOutcome::Ok,
        "NOT_FOUND" => UnregisterOutcome::NotFound,
        "STALE_EXECUTOR" => UnregisterOutcome::StaleExecutor,
        _ => return Err(protocol("executor_unregister 返回未知码")),
    })
}

/// 业务作用：把 registry_gc 的 `{deletedCount, newlyExpiredCount, redisNow}` 解释为回收结果；结构不符时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回三元组。
///
/// 返回：删除数、新候选数与权威时刻；缺字段或非数值返回协议错误。
fn interpret_registry_gc(raw: &[redis::Value]) -> Result<RegistryGc> {
    if raw.len() < 3 {
        return Err(protocol("registry_gc 返回结构非法"));
    }
    Ok(RegistryGc {
        deleted: parse_i64_value(&raw[0], "registry_gc deleted")?,
        newly_expired: parse_i64_value(&raw[1], "registry_gc newlyExpired")?,
        redis_now: parse_i64_value(&raw[2], "registry_gc redisNow")?,
    })
}

/// 业务作用：解析 register/heartbeat 共用的 `{OK, redisNow, expireAt, heartbeatRevision}` 尾部。
///
/// 参数说明：
/// - `raw`: 以 OK 开头的返回数组。
///
/// 返回：权威时刻、存活截止与心跳修订号；缺字段或非数值返回协议错误。
fn parse_liveness(raw: &[redis::Value]) -> Result<(i64, i64, i64)> {
    Ok((
        parse_i64_value(
            raw.get(1)
                .ok_or_else(|| protocol("存活返回缺少 redisNow"))?,
            "redisNow",
        )?,
        parse_i64_value(
            raw.get(2)
                .ok_or_else(|| protocol("存活返回缺少 expireAt"))?,
            "expireAt",
        )?,
        parse_i64_value(
            raw.get(3)
                .ok_or_else(|| protocol("存活返回缺少 heartbeatRevision"))?,
            "heartbeatRevision",
        )?,
    ))
}

/// 业务作用：把脚本返回的一个数值字段解释为 i64；非数值 fail-closed。
///
/// 参数说明：
/// - `value`: 字段原始值。
/// - `field`: 字段名，用于错误定位。
///
/// 返回：合法数值；非数值返回协议错误。
fn parse_i64_value(value: &redis::Value, field: &str) -> Result<i64> {
    value_to_string(value)
        .parse::<i64>()
        .map_err(|_| protocol(&format!("registry {field} 字段非法")))
}

/// 业务作用：构造 Job 协议错误。参数说明：`message` 摘要。返回：协议错误。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}
