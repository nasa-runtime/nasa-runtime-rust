//! Fanout 协调：从一个正在执行的根 Run 出发，把跨 slot Fanout intent 落库、冻结快照、批量建 shard、提交并投递。
//!
//! 提交按固定次序推进：先在根 Run 上持久 FANOUT_CREATING intent，再在 Fanout 桶建根、写 shard、提交，最后回填
//! 根 Run 为 WAITING_CHILDREN 并首次投递。任一步骤未取得预期结局即中止，已建立的持久入口由对账/看门狗收敛。
//! 根定义负责建立批次，目标必须是独立的 `FANOUT_ONLY` Worker 定义；两者通过冻结 Worker 合同连接，
//! 普通调度器不会领取目标能力的 shard。

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::fanout::{
    FanoutAddShardsOutcome, FanoutBeginOutcome, FanoutCommitOutcome, FanoutContract,
    FanoutDeliverOutcome, FanoutRepository,
};
use crate::job::handler::{JobExecution, JobExecutionAuthority};
use crate::job::identifiers::fanout_id;
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::JobMetrics;
use crate::job::model::{JobFanoutFailurePolicy, JobTrigger, JobWireCodec};
use crate::job::payload::JobPayload;
use crate::job::registry::{ClusterSnapshot, ExecutorRegistry, SnapshotOutcome};
use crate::job::repository::{FinishFanoutRootOutcome, JobRepository, PrepareFanoutRootOutcome};

/// Fanout 提交成功的结局：批次标识、分片总数与首次投递数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutSubmitOutcome {
    /// 本批 Fanout 标识（与根 attempt 一一对应）。
    pub fanout_id: String,
    /// 分片总数。
    pub shard_total: usize,
    /// 首次投递写入 inbox 的分片数。
    pub delivered: i64,
}

/// 一个正在执行的根 Run 的执行权上下文，用于把完成权威从普通 Handler 移交对账流程。
#[derive(Debug, Clone)]
pub struct FanoutRootContext<'a> {
    /// 根任务定义，提供根 Run 状态迁移与回执参数。
    pub definition: &'a JobDefinition,
    /// 本批冻结的目标 Worker 合同。
    pub contract: &'a FanoutContract,
    /// 根 Run 标识。
    pub root_run_id: &'a str,
    /// 根 Run 当前 owner（本执行器）。
    pub root_owner: &'a str,
    /// 根 Run 当前 attempt；批次标识协议要求为 `1..=i32::MAX`，提交前验证，不能截断。
    pub root_attempt: i64,
    /// 根 Run 当前 attempt 的 fencing token。
    pub root_attempt_token: i64,
}

/// Fanout 协调器；组合根 Run 状态迁移与 Fanout 桶数据模型完成一次批次提交与投递。
#[derive(Clone)]
pub struct FanoutCoordinator {
    /// 普通根执行的状态迁移仓库，负责持久移交完成权威。
    repository: JobRepository,
    /// 批次根、分片及投递的持久仓库。
    fanout: FanoutRepository,
    /// 发起批次的执行器身份，写入分片供回执关联。
    origin_executor_id: String,
    /// 受管场景的共享指标容器，独立构造时为空。
    metrics: Option<Arc<JobMetrics>>,
}

/// 普通 Handler 可见的 Fanout 创建服务；持有冻结定义表、能力注册表与根状态协调器。
#[derive(Clone)]
pub(crate) struct FanoutService {
    /// 当前数据源的能力注册表，提供兼容执行器的冻结快照。
    registry: Arc<ExecutorRegistry>,
    /// 负责持久提交顺序及完成权威移交的协调器。
    coordinator: FanoutCoordinator,
    /// 当前运行实例已经冻结的根任务与 Worker 定义。
    definitions: Arc<HashMap<String, JobDefinition>>,
    /// 已验证的分片容量和字节上限配置。
    config: Arc<JobConfig>,
    /// 当前执行根任务的节点身份，进入持久所有者校验。
    executor_id: String,
}

impl std::fmt::Debug for FanoutService {
    /// 业务作用：只展示无敏感、无 payload 的稳定服务类别，避免执行上下文调试输出连接或业务参数。
    ///
    /// 参数说明：
    /// - `formatter`：调试格式化目标，只接收固定服务类别名。
    ///
    /// 返回：写入固定类别名的格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FanoutService")
            .finish_non_exhaustive()
    }
}

impl FanoutService {
    /// 业务作用：把当前 source 的冻结定义、registry 与协调器组合成 Handler 受控入口。
    ///
    /// 参数说明：
    /// - `registry`：当前数据源运行实例的执行器能力注册表。
    /// - `coordinator`：同一实例的根执行与批次持久协调器。
    /// - `definitions`：同一实例冻结的普通根任务和 Worker 定义表。
    /// - `config`：同一实例已经验证的容量和时间配置。
    /// - `executor_id`：当前执行器身份，须与根执行的持久所有者一致。
    ///
    /// 返回：可从普通 JobContext 派生一次性 FanoutBuilder 的共享服务。
    pub(crate) fn new(
        registry: Arc<ExecutorRegistry>,
        coordinator: FanoutCoordinator,
        definitions: Arc<HashMap<String, JobDefinition>>,
        config: Arc<JobConfig>,
        executor_id: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            coordinator,
            definitions,
            config,
            executor_id: executor_id.into(),
        })
    }

    /// 业务作用：创建绑定当前根 attempt 与目标 Worker 的一次性构建器，Fanout shard 禁止递归创建。
    ///
    /// 参数说明：
    /// - `root`：当前普通执行的上下文，须仍具有本地执行权威。
    /// - `worker_name`：要使用的 Worker 能力名，创建时检查非空，提交时查找冻结定义。
    ///
    /// 返回：普通且仍持权的根返回 builder；递归 Fanout、失权或空 Worker 返回错误。
    pub(crate) fn builder(
        self: &Arc<Self>,
        root: &JobExecution,
        worker_name: impl Into<String>,
    ) -> Result<FanoutBuilder> {
        root.checkpoint()?;
        // 分片不得递归创建批次，避免完成责任扩展到当前根无法治理的层级。
        if root.fanout_context().is_some() {
            return Err(config("FANOUT_ONLY Worker 不能递归创建 Fanout".to_owned()));
        }
        let worker_name = worker_name.into();
        if worker_name.trim().is_empty() {
            return Err(config("Fanout worker_name 不能为空".to_owned()));
        }
        Ok(FanoutBuilder {
            service: self.clone(),
            root: root.clone(),
            worker_name,
            contract_revision: None,
            schema_id: None,
            wire_codec: None,
            failure_policy: JobFanoutFailurePolicy::ReassignOnFailure,
            input: FanoutInput::Missing,
        })
    }
}

/// Fanout 分片输入；直接编码与按成员数延迟分片互斥。
enum FanoutInput {
    /// 尚未声明分片输入，提交时必须拒绝。
    Missing,
    /// 已按目标成员顺序编码的载荷，提交时复验数量和合同。
    Shards(Vec<JobPayload>),
    /// 取得成员数后执行一次的分片算法，返回各成员的编码字节。
    Partition(Box<dyn FnOnce(usize) -> Result<Vec<Vec<u8>>> + Send>),
}

/// 业务侧一次性 Fanout 构建器；只收集合同与分片，`dispatch` 才冻结能力快照并转移根完成权威。
pub struct FanoutBuilder {
    /// 当前数据源提供的受控创建能力。
    service: Arc<FanoutService>,
    /// 绑定创建行为的普通根执行及本地权威门禁。
    root: JobExecution,
    /// 需要从冻结定义和注册表查找的目标能力名。
    worker_name: String,
    /// 本批指定的 Worker 合同修订号，未设置时沿用冻结定义。
    contract_revision: Option<i64>,
    /// 本批指定的载荷 Schema，未设置时沿用冻结定义。
    schema_id: Option<String>,
    /// 本批唯一线编码，未设置时选择 Worker 声明的第一个编码。
    wire_codec: Option<JobWireCodec>,
    /// 持久分片在目标失联或执行失败时采用的策略。
    failure_policy: JobFanoutFailurePolicy,
    /// 直接载荷或延迟分片算法，后一次设置替换前一次输入。
    input: FanoutInput,
}

impl FanoutBuilder {
    /// 业务作用：覆盖目标 Worker 契约修订号。
    ///
    /// 参数说明：
    /// - `revision`：本批要求的 Worker 合同修订号，必须大于零。
    ///
    /// 返回：合法时返回当前 builder；非正数返回配置错误。
    pub fn contract_revision(mut self, revision: i64) -> Result<Self> {
        if revision <= 0 {
            return Err(config("Fanout contract_revision 必须大于 0".to_owned()));
        }
        self.contract_revision = Some(revision);
        Ok(self)
    }

    /// 业务作用：覆盖本批 payload Schema 标识。
    ///
    /// 参数说明：
    /// - `schema_id`：本批载荷的稳定 Schema 标识，拒绝空串及全空白，保留原始非空文本。
    ///
    /// 返回：合法时返回当前 builder；空值返回配置错误。
    pub fn schema(mut self, schema_id: impl Into<String>) -> Result<Self> {
        let schema_id = schema_id.into();
        if schema_id.trim().is_empty() {
            return Err(config("Fanout schema 不能为空"));
        }
        self.schema_id = Some(schema_id);
        Ok(self)
    }

    /// 业务作用：选择本批唯一线编码，快照与每个 shard 均按该编码精确复验。
    ///
    /// 参数说明：
    /// - `codec`：本批唯一线编码，提交时须由目标 Worker 声明为兼容编码。
    ///
    /// 返回：更新后的 builder。
    pub fn codec(mut self, codec: JobWireCodec) -> Self {
        self.wire_codec = Some(codec);
        self
    }

    /// 业务作用：选择目标失联或执行失败时的持久收敛策略。
    ///
    /// 参数说明：
    /// - `policy`：写入批次合同的封闭失败策略，用于持久恢复和分片收敛。
    ///
    /// 返回：更新后的 builder。
    pub fn failure_policy(mut self, policy: JobFanoutFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// 业务作用：直接提供按能力成员顺序编码的分片，dispatch 会复验数量、合同与整批字节上限。
    ///
    /// 参数说明：
    /// - `payloads`：已经编码的有序分片集合，提交时须与冻结成员数、Schema 和线编码一致。
    ///
    /// 返回：更新后的 builder；此前延迟分片输入被替换。
    pub fn shards(mut self, payloads: impl IntoIterator<Item = JobPayload>) -> Self {
        self.input = FanoutInput::Shards(payloads.into_iter().collect());
        self
    }

    /// 业务作用：保存 JSON 业务输入与分片算法，待兼容成员数冻结后生成同样数量的 payload。
    ///
    /// 参数说明：
    /// - `items`：移入构建器的业务输入，在取得成员快照后交给分片算法。
    /// - `partitioner`：接收输入和成员数、返回按成员顺序排列的分片；分片数量必须等于成员数。
    ///
    /// 返回：保存算法的构建器，此时不执行序列化；算法和编码在 `dispatch` 内执行，编码失败不会建立持久 intent。
    pub fn partition<T, F>(mut self, items: Vec<T>, partitioner: F) -> Self
    where
        T: serde::Serialize + Send + 'static,
        F: FnOnce(Vec<T>, usize) -> Vec<Vec<T>> + Send + 'static,
    {
        self.input = FanoutInput::Partition(Box::new(move |member_count| {
            partitioner(items, member_count)
                .into_iter()
                .map(|partition| {
                    serde_json::to_vec(&partition)
                        .map_err(|error| config(format!("Fanout JSON 分片序列化失败: {error}")))
                })
                .collect()
        }));
        self
    }

    /// 业务作用：冻结兼容执行器快照，完成全部本地校验后提交确定性 Fanout intent 与持久 inbox。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交成功返回 fanoutId、分片数与首次投递数；无兼容节点、合同不符或状态迁移失败返回错误。
    pub async fn dispatch(self) -> Result<FanoutSubmitOutcome> {
        self.root.checkpoint()?;
        let worker = self
            .service
            .definitions
            .values()
            .find(|definition| definition.worker_name() == self.worker_name)
            .ok_or_else(|| {
                config(format!(
                    "Fanout 未找到本地 Worker 定义: {}",
                    self.worker_name
                ))
            })?;
        if worker.trigger() != JobTrigger::FanoutOnly {
            return Err(config(format!(
                "Fanout 目标必须声明为 FANOUT_ONLY: {}",
                self.worker_name
            )));
        }
        let root_definition = self
            .service
            .definitions
            .get(&self.root.job_name)
            .ok_or_else(|| config("Fanout 根定义未冻结"))?;
        let contract_revision = self.contract_revision.unwrap_or(worker.contract_revision());
        let schema_id = self
            .schema_id
            .unwrap_or_else(|| worker.schema_id().to_owned());
        let wire_codec = self.wire_codec.unwrap_or_else(|| worker.codecs()[0]);
        if !worker.codecs().contains(&wire_codec) {
            return Err(config("Fanout 选择的 codec 未由 Worker 声明".to_owned()));
        }
        let snapshot = match self
            .service
            .registry
            .snapshot(
                &self.worker_name,
                contract_revision,
                &schema_id,
                wire_codec.wire_name(),
            )
            .await?
        {
            SnapshotOutcome::Ok(snapshot) if !snapshot.members.is_empty() => snapshot,
            SnapshotOutcome::Ok(_) => {
                return Err(crate::job::JobError::NoCapableExecutor(
                    "没有已发布 fanoutReady 的兼容成员".to_owned(),
                )
                .into());
            }
            SnapshotOutcome::ContractMismatch => {
                return Err(crate::job::JobError::ContractMismatch(
                    "Worker 合同与 registry 不一致".to_owned(),
                )
                .into());
            }
        };
        let payloads = match self.input {
            FanoutInput::Missing => {
                return Err(config("Fanout 未配置 shards 或 partition".to_owned()));
            }
            FanoutInput::Shards(payloads) => {
                let mut bytes = Vec::with_capacity(payloads.len());
                for payload in payloads {
                    if payload.schema_id() != schema_id || payload.codec() != wire_codec {
                        return Err(config(
                            "Fanout shard 的 schema 或 codec 与本批合同不一致".to_owned(),
                        ));
                    }
                    bytes.push(payload.bytes().to_vec());
                }
                bytes
            }
            FanoutInput::Partition(materialize) => {
                if wire_codec != JobWireCodec::Json {
                    return Err(config("非 JSON Fanout 必须使用预编码 shards".to_owned()));
                }
                materialize(snapshot.members.len())?
            }
        };
        if payloads.len() != snapshot.members.len() {
            return Err(config("Fanout shard 数必须与冻结成员数相同".to_owned()));
        }
        let mut total = 0_u64;
        for payload in &payloads {
            if payload.len() > self.service.config.max_parameter_bytes as usize {
                return Err(config(
                    "Fanout 单 shard 超过 max_parameter_bytes".to_owned(),
                ));
            }
            total = total
                .checked_add(payload.len() as u64)
                .ok_or_else(|| config("Fanout payload 总量溢出"))?;
        }
        if total > self.service.config.fanout_max_total_parameter_bytes as u64 {
            return Err(config("Fanout payload 总量超过配置上限".to_owned()));
        }
        let contract = FanoutContract {
            worker_name: self.worker_name,
            contract_revision,
            schema_id,
            wire_codec,
            failure_policy: self.failure_policy,
        };
        let root = FanoutRootContext {
            definition: root_definition,
            contract: &contract,
            root_run_id: &self.root.run_id,
            root_owner: &self.service.executor_id,
            root_attempt: self.root.attempt,
            root_attempt_token: self.root.attempt_token,
        };
        self.service
            .coordinator
            .submit_with_authority(&root, &snapshot, &payloads, &self.root.authority)
            .await
    }
}

impl FanoutCoordinator {
    /// 业务作用：绑定连接、键模型、配置与发起执行器身份，创建 Fanout 协调器。
    ///
    /// 参数说明：
    /// - `client`：当前数据源的 Redis 连接客户端。
    /// - `keyspace`：已冻结的任务命名空间和分片键布局。
    /// - `config`：与键布局一致、已通过容量及时间门禁的运行配置。
    /// - `origin_executor_id`: 发起本次创建的执行器身份，写入各 shard 供回执定位。
    ///
    /// 返回：可执行提交与投递的协调器。
    pub fn new(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        origin_executor_id: impl Into<String>,
    ) -> Self {
        let repository = JobRepository::new(client.clone(), keyspace.clone(), config.clone());
        let fanout = FanoutRepository::new(client, keyspace, config);
        Self {
            repository,
            fanout,
            origin_executor_id: origin_executor_id.into(),
            metrics: None,
        }
    }

    /// 业务作用：为受管运行时创建共享同一 source 指标容器的 Fanout 协调器。
    ///
    /// 参数说明：
    /// - `client`：当前数据源运行实例的 Redis 客户端。
    /// - `keyspace`：同一实例已经冻结的命名空间和分片布局。
    /// - `config`：同一实例已经验证的运行配置。
    /// - `origin_executor_id`：发起批次的执行器身份，写入分片用于回执关联。
    /// - `metrics`：同一实例共享的低基数指标容器。
    ///
    /// 返回：提交 shard 时同步发布本地低基数观测的协调器。
    pub(crate) fn new_with_metrics(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        origin_executor_id: impl Into<String>,
        metrics: Arc<JobMetrics>,
    ) -> Self {
        let mut coordinator = Self::new(client, keyspace, config, origin_executor_id);
        coordinator.metrics = Some(metrics);
        coordinator
    }

    /// 业务作用：从正在执行的根 Run 提交一次 Fanout：落库 intent → 建根 → 写 shard → 提交 → 回填根 → 首次投递。
    ///
    /// 参数说明：
    /// - `root`: 根 Run 执行权上下文。
    /// - `snapshot`: 冻结的兼容能力快照，成员按序作为各 shard 目标。
    /// - `shard_payloads`: 每个 shard 的业务参数字节，长度必须等于快照成员数。
    ///
    /// 返回：提交并投递成功返回批次标识、分片总数与投递数；任一步骤未取得预期结局返回协议错误，
    /// 已建立的持久入口留待对账/看门狗收敛。
    pub async fn submit(
        &self,
        root: &FanoutRootContext<'_>,
        snapshot: &ClusterSnapshot,
        shard_payloads: &[Vec<u8>],
    ) -> Result<FanoutSubmitOutcome> {
        self.submit_inner(root, snapshot, shard_payloads, None)
            .await
    }

    /// 业务作用：从 Handler 显式 Fanout API 提交批次，并在根 Run 已持久转入 Fanout intent 后关闭本地完成权威。
    ///
    /// 参数说明：
    /// - `root`：当前普通根执行的定义、持久所有者和 attempt 权威。
    /// - `snapshot`：已经冻结的兼容成员快照，其顺序决定分片目标。
    /// - `shard_payloads`：按快照顺序排列的编码载荷，数量必须与成员数相同。
    /// - `authority`: 当前 Handler attempt 的本地权威门禁。
    ///
    /// 返回：成功返回批次结局；确定拒绝且未移交时保留 Handler 权威，intent 已建立或准备结果未知时撤销本地权威并返回后续结局。
    pub(crate) async fn submit_with_authority(
        &self,
        root: &FanoutRootContext<'_>,
        snapshot: &ClusterSnapshot,
        shard_payloads: &[Vec<u8>],
        authority: &Arc<JobExecutionAuthority>,
    ) -> Result<FanoutSubmitOutcome> {
        self.submit_inner(root, snapshot, shard_payloads, Some(authority))
            .await
    }

    /// 业务作用：执行统一 Fanout 提交状态机，并可在持久 intent 成功后同步撤销调用方本地权威。
    ///
    /// 参数说明：
    /// - `root`：根执行的持久定义、所有者、attempt 和 fencing token。
    /// - `snapshot`：冻结的兼容成员快照，后续重试须对应同一批次。
    /// - `shard_payloads`：按成员顺序提供的编码载荷，数量须与快照一致。
    /// - `authority`：Handler 入口提供的本地完成门禁，低层恢复调用不提供。
    ///
    /// 返回：提交并投递成功返回批次结局；非法 attempt 在持久准备前拒绝，其它拒绝或未知结局保留已建立的恢复入口。
    async fn submit_inner(
        &self,
        root: &FanoutRootContext<'_>,
        snapshot: &ClusterSnapshot,
        shard_payloads: &[Vec<u8>],
        authority: Option<&Arc<JobExecutionAuthority>>,
    ) -> Result<FanoutSubmitOutcome> {
        if shard_payloads.len() != snapshot.members.len() {
            return Err(protocol("submit 载荷数必须与快照成员数一致"));
        }
        // 标识协议使用正的 32 位 attempt，截断会使不同执行尝试共用批次，必须在移交根权威前拒绝。
        let root_attempt = i32::try_from(root.root_attempt)
            .map_err(|_| protocol("Fanout root_attempt 超出批次标识协议范围"))?;
        if root_attempt <= 0 {
            return Err(protocol("Fanout root_attempt 必须大于 0"));
        }
        let shard_total = shard_payloads.len() as i64;
        let fanout = fanout_id(root.root_run_id, root_attempt);

        // 先把根 Run 从 RUNNING 转入 FANOUT_CREATING，移交完成权威；未取得该 intent 则不建立任何 Fanout 结构。
        let prepare = self
            .repository
            .prepare_fanout_root(
                root.definition,
                root.root_run_id,
                root.root_owner,
                root.root_attempt_token,
                &fanout,
                &snapshot.snapshot_id,
                shard_total,
            )
            .await;
        let prepare = match prepare {
            Ok(outcome) => outcome,
            Err(error) => {
                // prepare 的传输或协议结局未知时，Redis 可能已经接管根；保守失权避免业务重试产生第二组外部副作用。
                if let Some(authority) = authority {
                    authority.transfer_to_fanout();
                }
                return Err(error);
            }
        };
        match prepare {
            PrepareFanoutRootOutcome::Prepared {
                ..
            }
            | PrepareFanoutRootOutcome::Adopted {
                ..
            } => {}
            PrepareFanoutRootOutcome::JobDeleted => {
                // 完成权威尚未转移，普通完成出口会把当前 attempt 按删除 fence 收敛为删除终态。
                return Err(crate::job::JobError::ExecutionStopped(
                    "Fanout 根定义已删除，拒绝建立新 intent".to_owned(),
                )
                .into());
            }
            other => return Err(protocol(&format!("prepare_fanout_root 拒绝: {other:?}"))),
        }
        // Redis 已把根从 RUNNING 转入 FANOUT_CREATING；此刻起完成权威属于持久对账流程，本地 attempt 不得再续期或提交业务结果。
        if let Some(authority) = authority {
            authority.transfer_to_fanout();
        }

        match self
            .fanout
            .begin(
                &fanout,
                root.root_run_id,
                root.root_attempt,
                snapshot,
                root.definition,
                root.contract,
                shard_total,
            )
            .await?
        {
            FanoutBeginOutcome::Ok {
                ..
            }
            | FanoutBeginOutcome::Adopted => {}
            other => return Err(protocol(&format!("fanout_begin 拒绝: {other:?}"))),
        }

        let added = match self
            .fanout
            .add_shards(
                &fanout,
                snapshot,
                root.contract,
                root.root_run_id,
                &self.origin_executor_id,
                shard_payloads,
            )
            .await?
        {
            FanoutAddShardsOutcome::Ok {
                added,
                ..
            } => added,
            other => return Err(protocol(&format!("fanout_add_shards 拒绝: {other:?}"))),
        };
        if let Some(metrics) = &self.metrics {
            metrics.add("redis_job_fanout_shard_total", added);
        }

        match self.fanout.commit(&fanout).await? {
            FanoutCommitOutcome::Ok {
                ..
            }
            | FanoutCommitOutcome::Adopted => {}
            other => return Err(protocol(&format!("fanout_commit 拒绝: {other:?}"))),
        }

        // 桶提交后才回填根为 WAITING_CHILDREN；本地完成权威已在准备阶段移交，不能在此重新开启。
        match self
            .repository
            .finish_fanout_root(
                root.definition,
                root.root_run_id,
                &fanout,
                root.root_attempt,
                "COMMITTED",
                "",
                "",
            )
            .await?
        {
            FinishFanoutRootOutcome::Ok {
                ..
            }
            | FinishFanoutRootOutcome::Adopted => {}
            FinishFanoutRootOutcome::JobDeleted {
                ..
            } => {
                // 桶已经提交，必须先发布 CANCELLING；只停止首次 deliver 不能阻止持久看门狗继续补投。
                let root = self
                    .fanout
                    .read_root(&fanout)
                    .await?
                    .ok_or_else(|| protocol("删除 fence 命中后缺少已提交 Fanout 根"))?;
                let _ = self.fanout.cancel_batch(&root, "JOB_DELETED").await?;
                return Err(crate::job::JobError::ExecutionStopped(
                    "Fanout 根定义已删除，已关闭桶内新执行权".to_owned(),
                )
                .into());
            }
            other => return Err(protocol(&format!("finish_fanout_root 拒绝: {other:?}"))),
        }

        // 根记录已持久建立，首次投递失败不撤销结构，由 receipt 看门狗重发。
        let delivered = match self
            .fanout
            .deliver_batch(&fanout, snapshot, root.definition)
            .await?
        {
            FanoutDeliverOutcome::Ok {
                delivered,
                ..
            } => delivered,
            FanoutDeliverOutcome::NotCommitted => {
                return Err(protocol("fanout_deliver_batch 拒绝: NotCommitted"))
            }
        };

        Ok(FanoutSubmitOutcome {
            fanout_id: fanout,
            shard_total: shard_payloads.len(),
            delivered,
        })
    }
}

/// 业务作用：将不符合 Fanout 持久状态机预期的结局归为任务协议错误。
///
/// 参数说明：
/// - `message`：当前状态迁移被拒绝或结果结构不符的原因摘要。
///
/// 返回：携带摘要的任务协议错误，不撤销已经完成的持久写入。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}

/// 业务作用：构造 Fanout 本地声明或容量门禁的结构化配置错误。
///
/// 参数说明：
/// - `message`：不包含载荷正文的声明或容量拒绝摘要。
///
/// 返回：携带摘要的任务配置错误，交给调用方停止本次创建。
fn config(message: impl Into<String>) -> NasaRedisError {
    crate::job::JobError::Config(message.into()).into()
}
