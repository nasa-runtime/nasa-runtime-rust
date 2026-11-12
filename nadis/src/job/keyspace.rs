//! Job 键模型：调度分片、Fanout 桶与注册表 key 的稳定派生。
//!
//! 同一 `(qualifier, namespace)` 内的 shard 数与 fanout 桶数是持久布局：调度、Fanout 与注册表
//! 分别使用 `{qualifier:namespace:shard}`、`{qualifier:namespace:fanout:bucket}`、
//! `{qualifier:namespace:registry}` hash tag，保证各自原子边界内的 key 落在同一 Redis slot。
//! 不同边界之间通过持久状态对账，不提供跨 slot 原子提交。分片选择用 FNV-1a 32 位
//! 哈希取 floorMod，是跨节点稳定协议，不能改算法，否则同一任务会被不同节点路由到不同分片。

use crate::error::Result;
use crate::job::identifiers::worker_key;
use crate::job::names::require_name;

/// FNV-1a 32 位偏移基。
const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
/// FNV-1a 32 位质数。
const FNV_PRIME: u32 = 0x0100_0193;

/// Job 的键与分片派生器；构造时冻结 namespace、shard 数与 fanout 桶数。
#[derive(Debug, Clone)]
pub struct JobKeyspace {
    /// 数据源的规范身份，参与全部键前缀并禁止冒号以保持 source 边界。
    qualifier: String,
    /// 已校验的任务命名空间，分片布局变更时应使用新的命名空间。
    namespace: String,
    /// 调度分片总数，冻结后所有节点使用同一值。
    shard_count: u32,
    /// Fanout 桶总数，决定批次状态与通知频道的稳定归属。
    fanout_bucket_count: u32,
}

impl JobKeyspace {
    /// 业务作用：构造键模型；namespace 校验合法且分片/桶数为正时冻结持久布局。
    ///
    /// 参数说明：
    /// - `qualifier`：语言无关的 source id，校验后禁止冒号，避免前缀边界歧义。
    /// - `namespace`：任务命名空间，校验并移除首尾空白后冻结。
    /// - `shard_count`：调度分片总数，必须大于零。
    /// - `fanout_bucket_count`：Fanout 桶总数，必须大于零。
    ///
    /// 返回：布局合法时返回键模型；名称非法或分片/桶数为零时返回配置错误。
    pub fn new(
        qualifier: &str,
        namespace: &str,
        shard_count: u32,
        fanout_bucket_count: u32,
    ) -> Result<Self> {
        let qualifier = require_name(qualifier, "qualifier")?;
        if qualifier.contains(':') {
            return Err(
                crate::job::JobError::Config("job qualifier 不得包含 ':'".to_owned()).into(),
            );
        }
        let namespace = require_name(namespace, "namespace")?;
        if shard_count == 0 {
            return Err(
                crate::job::JobError::Config("job shard_count 必须大于 0".to_owned()).into(),
            );
        }
        if fanout_bucket_count == 0 {
            return Err(crate::job::JobError::Config(
                "job fanout_bucket_count 必须大于 0".to_owned(),
            )
            .into());
        }
        Ok(Self {
            qualifier,
            namespace,
            shard_count,
            fanout_bucket_count,
        })
    }

    /// 业务作用：返回冻结的语言无关 source id。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：canonical qualifier，默认 source 为 `primary`。
    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }

    /// 业务作用：返回冻结的命名空间。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：命名空间文本。
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 业务作用：返回冻结的调度分片数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分片数。
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// 业务作用：返回冻结的 Fanout 桶数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：桶数。
    pub fn fanout_bucket_count(&self) -> u32 {
        self.fanout_bucket_count
    }

    /// 业务作用：按任务名选择固定调度分片；同一任务的全部 Run 与索引必须落在同一分片同一 slot。
    ///
    /// 参数说明：
    /// - `job_name`: 任务名。
    ///
    /// 返回：`0..shard_count` 的分片下标。
    pub fn schedule_shard(&self, job_name: &str) -> u32 {
        floor_mod(stable_hash(job_name), self.shard_count)
    }

    /// 业务作用：按 Fanout 标识选择固定桶；同一 Fanout 的 root、shard、inbox 必须落在同一桶同一 slot。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：`0..fanout_bucket_count` 的桶下标。
    pub fn fanout_bucket(&self, fanout_id: &str) -> u32 {
        floor_mod(stable_hash(fanout_id), self.fanout_bucket_count)
    }

    /// 业务作用：返回某分片的同 slot 前缀；分片内所有 key 共用它保证原子跨 key 迁移，同时作为脚本
    /// 权威分片前缀传入，避免 Lua 端从具体键名反推命名空间与分片边界。
    ///
    /// 参数说明：
    /// - `shard`: 分片下标。
    ///
    /// 返回：带 `{qualifier:namespace:shard}` hash tag 的分片键前缀。
    pub(crate) fn shard_prefix(&self, shard: u32) -> String {
        format!(
            "rjob:{{{}:{}:{}}}:",
            self.qualifier,
            self.namespace,
            padded_index(shard)
        )
    }

    /// 业务作用：返回注册表前缀；执行器、能力与 GC 等跨分片单例状态共用它，也作为权威前缀传入
    /// registry 相关脚本，避免 Lua 端从具体键名反推命名空间。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：带 `{qualifier:namespace:registry}` hash tag 的注册表键前缀。
    pub(crate) fn registry_prefix(&self) -> String {
        format!("rjob:{{{}:{}:registry}}:", self.qualifier, self.namespace)
    }

    /// 业务作用：返回承载不可变 Job 布局指纹的 registry-slot marker key。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 source 的 layout marker key。
    pub fn layout_marker(&self) -> String {
        format!("{}layout", self.registry_prefix())
    }

    /// 业务作用：返回 Rust 节点共享状态参数的 companion marker key；与跨语言基础布局 marker 同 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只约束当前 Rust 节点的运行时布局 key，不冒充跨语言部署证明。
    pub fn runtime_layout_marker(&self) -> String {
        format!("{}layout-runtime", self.registry_prefix())
    }

    /// 业务作用：返回某 Fanout 桶的同 slot 前缀。
    ///
    /// 参数说明：
    /// - `bucket`：调用方已选定的桶下标，应位于冻结的桶范围内。
    ///
    /// 返回：带 source、命名空间和补零桶下标的前缀；本函数不重新校验下标。
    fn fanout_bucket_prefix(&self, bucket: u32) -> String {
        format!(
            "rjob:{{{}:{}:fanout:{}}}:",
            self.qualifier,
            self.namespace,
            padded_index(bucket)
        )
    }

    /// 业务作用：返回某 Fanout 标识所属桶的同 slot 前缀。
    ///
    /// 参数说明：
    /// - `fanout_id`：已冻结的批次标识，按稳定哈希选择桶。
    ///
    /// 返回：该批次共享的桶前缀；批次自身的标识仍须由具体记录键追加。
    fn fanout_prefix(&self, fanout_id: &str) -> String {
        self.fanout_bucket_prefix(self.fanout_bucket(fanout_id))
    }

    /// 业务作用：定位到期调度索引，让扫描与任务重排使用同一分片状态。
    ///
    /// 参数说明：
    /// - `shard`：任务经稳定哈希确定的调度分片下标。
    ///
    /// 返回：该分片的 schedule ZSET 键，仅派生键名而不创建索引。
    pub fn schedule(&self, shard: u32) -> String {
        format!("{}schedule", self.shard_prefix(shard))
    }
    /// 业务作用：定位分片内的任务定义索引，供定义登记与枚举使用。
    ///
    /// 参数说明：
    /// - `shard`：任务定义所属的调度分片下标。
    ///
    /// 返回：与该分片定义记录同 slot 的 jobs 键，不读取登记状态。
    pub fn jobs(&self, shard: u32) -> String {
        format!("{}jobs", self.shard_prefix(shard))
    }
    /// 业务作用：定位命名空间在单个分片上的控制记录，供暂停与恢复门禁读取。
    ///
    /// 参数说明：
    /// - `shard`：需要应用命名空间控制的调度分片下标。
    ///
    /// 返回：该分片的 control HASH 键；不同分片的控制状态不因此成为一个原子事务。
    pub fn control(&self, shard: u32) -> String {
        format!("{}control", self.shard_prefix(shard))
    }
    /// 业务作用：定位普通 Run 的可见性截止索引，供延迟投递和消息恢复使用。
    ///
    /// 参数说明：
    /// - `shard`：Run 所属任务的调度分片下标。
    ///
    /// 返回：与 Run 记录同 slot 的 visible ZSET 键，不改变投递资格。
    pub fn visible(&self, shard: u32) -> String {
        format!("{}visible", self.shard_prefix(shard))
    }
    /// 业务作用：定位普通 Run 的租约截止索引，供领取、续期及过期恢复共享。
    ///
    /// 参数说明：
    /// - `shard`：Run 所属任务的调度分片下标。
    ///
    /// 返回：该分片的 leases ZSET 键，取得键名不代表持有租约。
    pub fn leases(&self, shard: u32) -> String {
        format!("{}leases", self.shard_prefix(shard))
    }
    /// 业务作用：定位等待 Fanout 创建完成的根索引，供创建中断后按截止时间恢复。
    ///
    /// 参数说明：
    /// - `shard`：普通根 Run 所属的调度分片下标。
    ///
    /// 返回：与根 Run 同 slot 的 waiting ZSET 键，不查询创建是否已完成。
    pub fn waiting(&self, shard: u32) -> String {
        format!("{}waiting", self.shard_prefix(shard))
    }
    /// 业务作用：定位按任务记录串行占用的单活槽，使领取和释放处于同一原子边界。
    ///
    /// 参数说明：
    /// - `shard`：串行任务所属的调度分片下标。
    ///
    /// 返回：该分片的 running HASH 键，仅供后续状态机访问，不授予执行权。
    pub fn running(&self, shard: u32) -> String {
        format!("{}running", self.shard_prefix(shard))
    }
    /// 业务作用：定位普通任务的 fencing 计数，供领取脚本分配单调执行令牌。
    ///
    /// 参数说明：
    /// - `shard`：任务所属的调度分片下标。
    ///
    /// 返回：与任务和 Run 同 slot 的 fences HASH 键；构造键名不增加计数。
    pub fn fences(&self, shard: u32) -> String {
        format!("{}fences", self.shard_prefix(shard))
    }
    /// 业务作用：定位普通 Run 的完成事件流，使终态与完成证据可在同一脚本提交。
    ///
    /// 参数说明：
    /// - `shard`：Run 所属任务的调度分片下标。
    ///
    /// 返回：该分片的 completion Stream 键，不追加事件或确认消费。
    pub fn completion(&self, shard: u32) -> String {
        format!("{}completion", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片内已删除且仍有存量 waitq 待收敛的任务名集合。
    ///
    /// 参数说明：
    /// - `shard`：已删除任务所属的调度分片下标。
    ///
    /// 返回：与任务 waitq、Run 和 completion 同 slot 的 reaping SET key。
    pub fn reaping(&self, shard: u32) -> String {
        format!("{}reaping", self.shard_prefix(shard))
    }
    /// 业务作用：定位指定任务的持久定义记录，供合同校验和定义状态迁移使用。
    ///
    /// 参数说明：
    /// - `shard`：必须与该任务的稳定调度分片一致的下标。
    /// - `job_name`：已校验且在命名空间内稳定的任务名。
    ///
    /// 返回：分片前缀下的 job HASH 键；不重新校验名称或分片对应关系。
    pub fn job(&self, shard: u32, job_name: &str) -> String {
        format!("{}job:{job_name}", self.shard_prefix(shard))
    }
    /// 业务作用：定位普通 Run 的持久执行记录，保证同一触发身份访问同一状态。
    ///
    /// 参数说明：
    /// - `shard`：Run 所属任务的调度分片下标。
    /// - `run_id`：按触发协议派生的稳定 Run 标识。
    ///
    /// 返回：该分片的 run HASH 键，不读取或改变执行状态。
    pub fn run(&self, shard: u32, run_id: &str) -> String {
        format!("{}run:{run_id}", self.shard_prefix(shard))
    }
    /// 业务作用：按 Worker 完整摘要选择普通任务投递流，使共享能力的执行器消费同一通道。
    ///
    /// 参数说明：
    /// - `shard`：待投递 Run 所属任务的调度分片下标。
    /// - `worker_name`：已经登记的 Worker 能力名，参与完整摘要计算。
    ///
    /// 返回：与该分片 Run 同 slot 的 dispatch Stream 键，不发布消息。
    pub fn dispatch(&self, shard: u32, worker_name: &str) -> String {
        format!(
            "{}dispatch:{}",
            self.shard_prefix(shard),
            worker_key(worker_name)
        )
    }
    /// 业务作用：定位任务的串行等待队列，使占用槽释放后可继续推进阻塞的 Run。
    ///
    /// 参数说明：
    /// - `shard`：任务的稳定调度分片下标。
    /// - `job_name`：需要独立排队的已校验任务名。
    ///
    /// 返回：与任务单活槽同 slot 的 waitq 键，不改变排队顺序或出队状态。
    pub fn waitq(&self, shard: u32, job_name: &str) -> String {
        format!("{}waitq:{job_name}", self.shard_prefix(shard))
    }

    /// 业务作用：定位当前 source 和命名空间的执行器集合，供注册表枚举及过期回收使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：registry slot 内的 executors 键，不把集合成员身份视为存活证明。
    pub fn executors(&self) -> String {
        format!("{}executors", self.registry_prefix())
    }
    /// 业务作用：定位一次执行器启动对应的注册状态，隔离同一稳定节点的不同启动实例。
    ///
    /// 参数说明：
    /// - `executor_id`：本次启动的执行器标识，不能用稳定节点身份替代。
    ///
    /// 返回：registry slot 内的 executor 记录键，不核验心跳或租约。
    pub fn executor(&self, executor_id: &str) -> String {
        format!("{}executor:{executor_id}", self.registry_prefix())
    }
    /// 业务作用：定位声明某项 Worker 能力的执行器集合，供兼容候选选择使用。
    ///
    /// 参数说明：
    /// - `worker_name`：已校验的 Worker 能力名。
    ///
    /// 返回：registry slot 内的 capability 键；成员是否兼容及存活仍由注册表核验。
    pub fn capability(&self, worker_name: &str) -> String {
        format!("{}capability:{worker_name}", self.registry_prefix())
    }
    /// 业务作用：定位 Worker 能力的注册元数据，供合同登记和清理共享。
    ///
    /// 参数说明：
    /// - `worker_name`：已校验的 Worker 能力名。
    ///
    /// 返回：与能力集合同 slot 的 capability-meta 键，不读取已登记的合同。
    pub fn capability_meta(&self, worker_name: &str) -> String {
        format!("{}capability-meta:{worker_name}", self.registry_prefix())
    }
    /// 业务作用：定位 Worker 的特定合同修订，避免能力滚动变化时把不同合同混为一份记录。
    ///
    /// 参数说明：
    /// - `worker_name`：已校验的 Worker 能力名。
    /// - `revision`：调用方确定的合同修订号，按十进制参与键身份。
    ///
    /// 返回：registry slot 内的 contract 键，不核验该修订是否已经登记。
    pub fn contract(&self, worker_name: &str, revision: i64) -> String {
        format!(
            "{}contract:{worker_name}:{revision}",
            self.registry_prefix()
        )
    }
    /// 业务作用：定位 Worker 完整摘要到能力名的绑定记录，供注册时核验路由摘要归属。
    ///
    /// 参数说明：
    /// - `worker_name`：参与完整摘要计算的已校验能力名。
    ///
    /// 返回：registry slot 内以摘要为后缀的 worker-key 键，不写入绑定。
    pub fn worker_key_binding(&self, worker_name: &str) -> String {
        format!(
            "{}worker-key:{}",
            self.registry_prefix(),
            worker_key(worker_name)
        )
    }
    /// 业务作用：定位稳定节点的 Fanout 注册证据，供定向分配核对节点身份与启动状态。
    ///
    /// 参数说明：
    /// - `node_identity`：跨重启保持稳定的节点身份，区别于单次启动的执行器标识。
    ///
    /// 返回：registry slot 内的 fanout-evidence 键，键名本身不是存活或接收证明。
    pub fn fanout_evidence(&self, node_identity: &str) -> String {
        format!("{}fanout-evidence:{node_identity}", self.registry_prefix())
    }
    /// 业务作用：定位注册表的延迟回收索引，使过期发现与最终删除遵守同一宽限窗口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：registry slot 内的 gc 键，不触发成员删除。
    pub fn registry_gc(&self) -> String {
        format!("{}gc", self.registry_prefix())
    }

    /// 业务作用：定位 Fanout 批次根状态，供分片操作核验提交与取消状态。
    ///
    /// 参数说明：
    /// - `fanout_id`：已冻结的批次标识，同时决定桶归属和记录后缀。
    ///
    /// 返回：批次所在桶的 root 记录键；与普通调度根 Run 位于不同原子边界。
    pub fn fanout_root(&self, fanout_id: &str) -> String {
        format!("{}root:{fanout_id}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位不随目标重新分配变化的 Fanout 分片记录，保持状态与幂等身份连续。
    ///
    /// 参数说明：
    /// - `fanout_id`：已冻结的批次标识，决定桶归属。
    /// - `seq`：批次内稳定分片序号，重发时保留原值。
    ///
    /// 返回：与批次 root 同 slot 的 shard 键，不校验序号是否已经创建。
    pub fn fanout_shard(&self, fanout_id: &str, seq: i64) -> String {
        format!("{}shard:{fanout_id}:{seq}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位目标节点在指定 Fanout 桶内的定向接收流，供多个批次共享投递。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于确定桶归属的批次标识，不直接作为 inbox 后缀。
    /// - `node_identity`：跨重启稳定的目标节点身份。
    ///
    /// 返回：该桶内以节点身份区分的 inbox Stream 键，不确认目标已接收消息。
    pub fn fanout_inbox(&self, fanout_id: &str, node_identity: &str) -> String {
        format!("{}inbox:{node_identity}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位 Fanout 所属桶的接收截止索引，供未收到持久回执的投递恢复使用。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：桶内全部批次共享的 receipts ZSET 键，不是单一批次的独占索引。
    pub fn fanout_receipts(&self, fanout_id: &str) -> String {
        format!("{}receipts", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位 Fanout 桶内的待执行唤醒索引，供已接收但尚未取得执行槽的分片推进。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：桶内共享的 ready ZSET 键，不据此认定某分片已获执行权。
    pub fn fanout_ready(&self, fanout_id: &str) -> String {
        format!("{}ready", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位 Fanout 桶内的执行租约截止索引，供续期和过期恢复共享。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：桶内共享的 lease ZSET 键，实际权威仍需核验 owner、token 和 assignment。
    pub fn fanout_leases(&self, fanout_id: &str) -> String {
        format!("{}lease", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位 Fanout 桶内的完成事件流，使分片终态与完成证据在同一 slot 提交。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：桶内共享的 completion Stream 键，不追加或确认完成事件。
    pub fn fanout_completion(&self, fanout_id: &str) -> String {
        format!("{}completion", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：按固定桶返回 Fanout Completion Stream，供后台在没有新完成写入时继续执行保留裁剪。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标，无需持有某个批次标识。
    ///
    /// 返回：与该桶根、shard 与看门狗索引同 slot 的 completion key。
    pub fn fanout_completion_at(&self, bucket: u32) -> String {
        format!("{}completion", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：定位 Fanout 桶内的非终态根索引，供看门狗发现需要继续推进的批次。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：该桶的 roots ZSET 键，不读取根状态或触发恢复。
    pub fn fanout_roots(&self, fanout_id: &str) -> String {
        format!("{}roots", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位 Fanout 桶内的清理截止索引，使批次终态按保留窗口回收。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    ///
    /// 返回：桶内共享的 gc ZSET 键，不删除 root、shard 或 payload。
    pub fn fanout_gc(&self, fanout_id: &str) -> String {
        format!("{}gc", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位稳定节点在 Fanout 桶内的定向唤醒频道，通知只推动持久记录的读取。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    /// - `node_identity`：跨重启稳定的目标节点身份。
    ///
    /// 返回：该桶内的 notify 频道；发布通知不等同于持久投递或接收确认。
    pub fn fanout_notify_channel(&self, fanout_id: &str, node_identity: &str) -> String {
        format!("{}notify:{node_identity}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：定位发起执行器在 Fanout 桶内的回执通知频道，供提交者及时读取持久结果。
    ///
    /// 参数说明：
    /// - `fanout_id`：用于选择固定桶的批次标识。
    /// - `executor_id`：发起者本次启动的执行器身份，隔离不同启动实例。
    ///
    /// 返回：该桶内的 receipt 频道；缺少通知不能证明持久接收未发生。
    pub fn fanout_receipt_channel(&self, fanout_id: &str, executor_id: &str) -> String {
        format!("{}receipt:{executor_id}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：按固定桶生成本节点的定向通知频道，允许运行时在任何 Fanout 建立前完成全部订阅门禁。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标，供启动时枚举全部频道。
    /// - `node_identity`：跨重启稳定的目标节点身份。
    ///
    /// 返回：与该桶全部 Fanout 共享 hash tag 的通知频道。
    pub fn fanout_notify_channel_at(&self, bucket: u32, node_identity: &str) -> String {
        format!(
            "{}notify:{node_identity}",
            self.fanout_bucket_prefix(bucket)
        )
    }
    /// 业务作用：按固定桶生成发起执行器回执频道，使提交者可在创建任何 Fanout 前建立订阅。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标，供提交前预先订阅。
    /// - `executor_id`：提交者本次启动的执行器身份。
    ///
    /// 返回：与该桶全部 Fanout 共享 hash tag 的回执频道。
    pub fn fanout_receipt_channel_at(&self, bucket: u32, executor_id: &str) -> String {
        format!("{}receipt:{executor_id}", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：生成 Fanout shard 跨重发稳定的业务幂等键，防止不同 source 的相同批次互相去重。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `seq`: shard 稳定序号。
    ///
    /// 返回：包含 source 与命名空间边界的 `qualifier:namespace:fanoutId:seq` 文本；跨目标重分配保持不变。
    pub fn execution_key(&self, fanout_id: &str, seq: i64) -> String {
        format!("{}:{}:{fanout_id}:{seq}", self.qualifier, self.namespace)
    }
    /// 业务作用：供监视器按固定桶扫描非终态根，无需预先知道批次标识。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标。
    ///
    /// 返回：该桶的 roots ZSET 键，不读取或推进批次状态。
    pub fn fanout_roots_at(&self, bucket: u32) -> String {
        format!("{}roots", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：供监视器按固定桶发现接收回执超时的分片。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标。
    ///
    /// 返回：该桶共享的 receipts ZSET 键，是否超时仍由 Redis 权威时间裁决。
    pub fn fanout_receipts_at(&self, bucket: u32) -> String {
        format!("{}receipts", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：供监视器按固定桶发现需要再次唤醒的已接收分片。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标。
    ///
    /// 返回：该桶的 ready ZSET 键，不授予执行槽或改变 assignment。
    pub fn fanout_ready_at(&self, bucket: u32) -> String {
        format!("{}ready", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：供监视器按固定桶定位需要复验租约的运行中分片。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标。
    ///
    /// 返回：该桶的 lease ZSET 键，恢复时仍须复验分片的当前持久状态。
    pub fn fanout_leases_at(&self, bucket: u32) -> String {
        format!("{}lease", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：供清理任务按固定桶发现达到保留截止的 Fanout 批次。
    ///
    /// 参数说明：
    /// - `bucket`：已处于冻结桶范围内的下标。
    ///
    /// 返回：该桶的 gc ZSET 键，实际删除仍须满足终态、对账和游标条件。
    pub fn fanout_gc_at(&self, bucket: u32) -> String {
        format!("{}gc", self.fanout_bucket_prefix(bucket))
    }
}

/// 业务作用：计算跨节点稳定的 FNV-1a 32 位哈希；分片与桶路由据此保持一致。
///
/// 参数说明：
/// - `value`: 参与路由的文本（任务名或 Fanout 标识）。
///
/// 返回：FNV-1a 32 位哈希（wrapping 语义，与 32 位整数溢出一致）。
fn stable_hash(value: &str) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in value.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// 业务作用：对有符号 32 位哈希取 floorMod，得到非负分片下标。
///
/// 参数说明：
/// - `hash`: FNV-1a 32 位哈希。
/// - `count`: 分片或桶总数，调用方保证大于零。
///
/// 返回：`0..count` 的下标；哈希保留有符号解释，除数保持正数，支持整个非零 `u32` 布局范围。
fn floor_mod(hash: u32, count: u32) -> u32 {
    // 布局数量不能被窄化成负除数；扩大计算宽度也避免最小哈希除以负一时溢出。
    i64::from(hash as i32).rem_euclid(i64::from(count)) as u32
}

/// 业务作用：把分片或桶下标编码为至少两位十进制；单位数补前导零。
///
/// 参数说明：
/// - `value`: 分片或桶下标。
///
/// 返回：至少两位的十进制文本。
fn padded_index(value: u32) -> String {
    if value < 10 {
        format!("0{value}")
    } else {
        value.to_string()
    }
}
