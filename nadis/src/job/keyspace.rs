//! Job 键模型：调度分片、Fanout 桶与注册表 key 的稳定派生。
//!
//! 同一 `(qualifier, namespace)` 内的 shard 数与 fanout 桶数是持久布局：所有 key 用
//! `{qualifier:namespace:shard}` 形式的 hash tag
//! 保证同分片内多 key 落在同一 Redis slot，Lua 状态迁移才能原子跨这些 key 提交。分片选择用 FNV-1a 32 位
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
    qualifier: String,
    namespace: String,
    shard_count: u32,
    fanout_bucket_count: u32,
}

impl JobKeyspace {
    /// 业务作用：构造键模型；namespace 校验合法且分片/桶数为正时冻结持久布局。
    ///
    /// 参数说明：
    /// - `qualifier`/`namespace`: 语言无关 source id 与命名空间。
    /// - `shard_count`/`fanout_bucket_count`: 调度分片数与 Fanout 桶数，必须大于零。
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
    /// 返回：带 `{namespace:registry}` hash tag 的注册表键前缀。
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
    fn fanout_bucket_prefix(&self, bucket: u32) -> String {
        format!(
            "rjob:{{{}:{}:fanout:{}}}:",
            self.qualifier,
            self.namespace,
            padded_index(bucket)
        )
    }

    /// 业务作用：返回某 Fanout 标识所属桶的同 slot 前缀。
    fn fanout_prefix(&self, fanout_id: &str) -> String {
        self.fanout_bucket_prefix(self.fanout_bucket(fanout_id))
    }

    /// 业务作用：返回分片的到期调度 ZSET key。参数说明：`shard` 分片下标。返回：schedule key。
    pub fn schedule(&self, shard: u32) -> String {
        format!("{}schedule", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的任务定义索引 key。参数说明：`shard` 分片下标。返回：jobs key。
    pub fn jobs(&self, shard: u32) -> String {
        format!("{}jobs", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的控制通道 key。参数说明：`shard` 分片下标。返回：control key。
    pub fn control(&self, shard: u32) -> String {
        format!("{}control", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的可见性提升索引 key。参数说明：`shard` 分片下标。返回：visible key。
    pub fn visible(&self, shard: u32) -> String {
        format!("{}visible", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的租约索引 key。参数说明：`shard` 分片下标。返回：leases key。
    pub fn leases(&self, shard: u32) -> String {
        format!("{}leases", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的待创建等待索引 key。参数说明：`shard` 分片下标。返回：waiting key。
    pub fn waiting(&self, shard: u32) -> String {
        format!("{}waiting", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的串行运行单活槽 key。参数说明：`shard` 分片下标。返回：running key。
    pub fn running(&self, shard: u32) -> String {
        format!("{}running", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的 fencing token 分配 key。参数说明：`shard` 分片下标。返回：fences key。
    pub fn fences(&self, shard: u32) -> String {
        format!("{}fences", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片的完成事件 Stream key。参数说明：`shard` 分片下标。返回：completion key。
    pub fn completion(&self, shard: u32) -> String {
        format!("{}completion", self.shard_prefix(shard))
    }
    /// 业务作用：返回分片内已删除且仍有存量 waitq 待收敛的任务名集合。
    ///
    /// 参数说明：`shard` 为调度分片下标。
    ///
    /// 返回：与任务 waitq、Run 和 completion 同 slot 的 reaping SET key。
    pub fn reaping(&self, shard: u32) -> String {
        format!("{}reaping", self.shard_prefix(shard))
    }
    /// 业务作用：返回某任务定义 hash key。参数说明：`shard` 分片、`job_name` 任务名。返回：job key。
    pub fn job(&self, shard: u32, job_name: &str) -> String {
        format!("{}job:{job_name}", self.shard_prefix(shard))
    }
    /// 业务作用：返回某 Run 记录 hash key。参数说明：`shard` 分片、`run_id` Run 标识。返回：run key。
    pub fn run(&self, shard: u32, run_id: &str) -> String {
        format!("{}run:{run_id}", self.shard_prefix(shard))
    }
    /// 业务作用：返回按 Worker 能力路由的 Dispatch Stream key。参数说明：`shard` 分片、`worker_name` 能力名。返回：dispatch key。
    pub fn dispatch(&self, shard: u32, worker_name: &str) -> String {
        format!(
            "{}dispatch:{}",
            self.shard_prefix(shard),
            worker_key(worker_name)
        )
    }
    /// 业务作用：返回某任务的串行等待队列 key。参数说明：`shard` 分片、`job_name` 任务名。返回：waitq key。
    pub fn waitq(&self, shard: u32, job_name: &str) -> String {
        format!("{}waitq:{job_name}", self.shard_prefix(shard))
    }

    /// 业务作用：返回执行器集合 key。参数说明: 无。返回：executors key。
    pub fn executors(&self) -> String {
        format!("{}executors", self.registry_prefix())
    }
    /// 业务作用：返回某执行器状态 key。参数说明：`executor_id` 执行器标识。返回：executor key。
    pub fn executor(&self, executor_id: &str) -> String {
        format!("{}executor:{executor_id}", self.registry_prefix())
    }
    /// 业务作用：返回某能力的执行器集合 key。参数说明：`worker_name` 能力名。返回：capability key。
    pub fn capability(&self, worker_name: &str) -> String {
        format!("{}capability:{worker_name}", self.registry_prefix())
    }
    /// 业务作用：返回某能力的元数据 key。参数说明：`worker_name` 能力名。返回：capability-meta key。
    pub fn capability_meta(&self, worker_name: &str) -> String {
        format!("{}capability-meta:{worker_name}", self.registry_prefix())
    }
    /// 业务作用：返回某能力某修订的契约 key。参数说明：`worker_name` 能力名、`revision` 修订号。返回：contract key。
    pub fn contract(&self, worker_name: &str, revision: i64) -> String {
        format!(
            "{}contract:{worker_name}:{revision}",
            self.registry_prefix()
        )
    }
    /// 业务作用：返回某能力的 worker-key 绑定 key。参数说明：`worker_name` 能力名。返回：worker-key 绑定 key。
    pub fn worker_key_binding(&self, worker_name: &str) -> String {
        format!(
            "{}worker-key:{}",
            self.registry_prefix(),
            worker_key(worker_name)
        )
    }
    /// 业务作用：返回某节点的 Fanout 证据 key。参数说明：`node_identity` 节点身份。返回：fanout-evidence key。
    pub fn fanout_evidence(&self, node_identity: &str) -> String {
        format!("{}fanout-evidence:{node_identity}", self.registry_prefix())
    }
    /// 业务作用：返回注册表 GC key。参数说明: 无。返回：registry gc key。
    pub fn registry_gc(&self) -> String {
        format!("{}gc", self.registry_prefix())
    }

    /// 业务作用：返回某 Fanout 的 root 记录 key。参数说明：`fanout_id` Fanout 标识。返回：root key。
    pub fn fanout_root(&self, fanout_id: &str) -> String {
        format!("{}root:{fanout_id}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout shard 记录 key。参数说明：`fanout_id` Fanout 标识、`seq` 分片序号。返回：shard key。
    pub fn fanout_shard(&self, fanout_id: &str, seq: i64) -> String {
        format!("{}shard:{fanout_id}:{seq}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某节点在某 Fanout 的定向 inbox key。参数说明：`fanout_id` 标识、`node_identity` 节点身份。返回：inbox key。
    pub fn fanout_inbox(&self, fanout_id: &str, node_identity: &str) -> String {
        format!("{}inbox:{node_identity}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout 的接收回执索引 key。参数说明：`fanout_id` 标识。返回：receipts key。
    pub fn fanout_receipts(&self, fanout_id: &str) -> String {
        format!("{}receipts", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout 的 ready 唤醒索引 key。参数说明：`fanout_id` 标识。返回：ready key。
    pub fn fanout_ready(&self, fanout_id: &str) -> String {
        format!("{}ready", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout 的租约索引 key。参数说明：`fanout_id` 标识。返回：lease key。
    pub fn fanout_leases(&self, fanout_id: &str) -> String {
        format!("{}lease", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout 桶的完成事件 Stream key。参数说明：`fanout_id` 标识。返回：completion key。
    pub fn fanout_completion(&self, fanout_id: &str) -> String {
        format!("{}completion", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：按固定桶返回 Fanout Completion Stream，供后台在没有新完成写入时继续执行保留裁剪。
    ///
    /// 参数说明：`bucket` 为固定 Fanout 桶下标。
    ///
    /// 返回：与该桶根、shard 与看门狗索引同 slot 的 completion key。
    pub fn fanout_completion_at(&self, bucket: u32) -> String {
        format!("{}completion", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：返回某 Fanout 桶的非终态看门狗 ZSET key。参数说明：`fanout_id` 标识。返回：roots 看门狗 key。
    pub fn fanout_roots(&self, fanout_id: &str) -> String {
        format!("{}roots", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回某 Fanout 桶的清理 ZSET key。参数说明：`fanout_id` 标识。返回：gc key。
    pub fn fanout_gc(&self, fanout_id: &str) -> String {
        format!("{}gc", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回稳定节点的 Fanout 定向通知频道。参数说明：`fanout_id` 标识、`node_identity` 节点身份。返回：notify 频道。
    pub fn fanout_notify_channel(&self, fanout_id: &str, node_identity: &str) -> String {
        format!("{}notify:{node_identity}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：返回发起执行器的 Fanout 回执频道。参数说明：`fanout_id` 标识、`executor_id` 执行器身份。返回：receipt 频道。
    pub fn fanout_receipt_channel(&self, fanout_id: &str, executor_id: &str) -> String {
        format!("{}receipt:{executor_id}", self.fanout_prefix(fanout_id))
    }
    /// 业务作用：按固定桶生成本节点的定向通知频道，允许运行时在任何 Fanout 建立前完成全部订阅门禁。
    ///
    /// 参数说明：`bucket` 为固定桶下标，`node_identity` 为跨重启稳定节点身份。
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
    /// 参数说明：`bucket` 为固定桶下标，`executor_id` 为本次启动执行器身份。
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
    /// 返回：`qualifier:namespace:fanoutId:seq`。
    pub fn execution_key(&self, fanout_id: &str, seq: i64) -> String {
        format!("{}:{}:{fanout_id}:{seq}", self.qualifier, self.namespace)
    }
    /// 业务作用：按桶下标返回该桶的非终态看门狗 ZSET；监视器按桶而非按 fanoutId 扫描时用它。参数说明：`bucket` 桶下标。返回：roots 看门狗 key。
    pub fn fanout_roots_at(&self, bucket: u32) -> String {
        format!("{}roots", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：按桶下标返回该桶的 receipt 截止 ZSET。参数说明：`bucket` 桶下标。返回：receipts key。
    pub fn fanout_receipts_at(&self, bucket: u32) -> String {
        format!("{}receipts", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：按桶下标返回该桶的 ready 截止 ZSET。参数说明：`bucket` 桶下标。返回：ready key。
    pub fn fanout_ready_at(&self, bucket: u32) -> String {
        format!("{}ready", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：按桶下标返回该桶的租约截止 ZSET。参数说明：`bucket` 桶下标。返回：lease key。
    pub fn fanout_leases_at(&self, bucket: u32) -> String {
        format!("{}lease", self.fanout_bucket_prefix(bucket))
    }
    /// 业务作用：按桶下标返回该桶的清理 ZSET。参数说明：`bucket` 桶下标。返回：gc key。
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
/// 返回：`0..count` 的下标；对负数按向下取整取模，与既有分片路由一致。
fn floor_mod(hash: u32, count: u32) -> u32 {
    // 哈希按 32 位有符号解释再 floorMod，负数结果补正为非负，保证与既有节点选择同一分片。
    (hash as i32).rem_euclid(count as i32) as u32
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
