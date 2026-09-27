//! 调度扫描：读取到期定义，按误触发策略展开有界 CAS 链，再由批量脚本原子创建 Run 并推进逻辑时刻。
//!
//! 扫描只提供候选；每项仍在 Redis 内复验定义修订、旧 score 与服务端时间。`CATCH_UP` 只保留窗口内最近的
//! 有限时刻，更早积压用跳过项推进，避免恢复后形成无界 Run 风暴。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::JobFireResult;
use crate::job::model::{JobMisfire, JobScheduleType, JobTrigger};
use crate::job::repository::{FireDueBatchItem, JobRepository};
use crate::job::trigger::{first_fire_at_after, next_fire_at};

/// Cron 从很久以前推进到补偿窗口时的绝对步数上限；超过表示配置需要迁移调度基线，不能阻塞运行时循环。
const CRON_ADVANCE_LIMIT: usize = 1_000_000;

/// 一轮调度扫描的本地观测；触发结果来自脚本封闭返回码，不引入运行期任意 label。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobScanOutcome {
    /// 实际建立或采用的 Run 数。
    pub fired: usize,
    /// 本轮候选中最大的 Redis 权威调度延迟。
    pub schedule_lag_ms: i64,
    /// 各封闭触发结局的本轮次数。
    pub fire_results: BTreeMap<JobFireResult, u64>,
}

/// 单节点调度扫描器；持有仓库、配置与本地定义注册表。
pub struct JobScanner {
    repository: JobRepository,
    config: Arc<JobConfig>,
    definitions: HashMap<String, JobDefinition>,
}

impl JobScanner {
    /// 业务作用：绑定连接、键模型与配置，创建调度扫描器。
    ///
    /// 参数说明：
    /// - `client`: 承载扫描连接的 RedisClient。
    /// - `keyspace`: 冻结的 Job 键模型。
    /// - `config`: 已校验的 Job 配置。
    ///
    /// 返回：可登记定义并轮询触发的扫描器。
    pub fn new(client: Arc<RedisClient>, keyspace: JobKeyspace, config: Arc<JobConfig>) -> Self {
        let repository = JobRepository::new(client, keyspace, config.clone());
        Self {
            repository,
            config,
            definitions: HashMap::new(),
        }
    }

    /// 业务作用：登记一个本地定义，使扫描到该任务时可用触发引擎计算下一逻辑时刻。
    ///
    /// 参数说明：
    /// - `definition`: 任务定义。
    ///
    /// 返回：无返回值；同名任务重复登记覆盖既有项。
    pub fn register_definition(&mut self, definition: JobDefinition) {
        self.definitions
            .insert(definition.name().to_owned(), definition);
    }

    /// 业务作用：扫描一个分片内到期定义，按误触发策略生成有限链并批量 CAS 提交。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片。
    ///
    /// 返回：本轮实际创建或幂等采用的 Run 数；候选并发变化时有界重算，耗尽后返回零并留待下一轮。
    pub async fn scan_once(&self, shard: u32) -> Result<usize> {
        Ok(self.scan_once_observed(shard).await?.fired)
    }

    /// 业务作用：扫描一个分片并保留调度延迟与每类脚本结局，供受管运行时发布完整低基数观测。
    ///
    /// 参数说明：`shard` 为目标调度分片。
    ///
    /// 返回：全部有界重算期间实际产生的 Run 数、最大延迟和封闭结果计数。
    pub(crate) async fn scan_once_observed(&self, shard: u32) -> Result<JobScanOutcome> {
        let mut outcome = JobScanOutcome {
            fired: 0,
            schedule_lag_ms: 0,
            fire_results: BTreeMap::new(),
        };
        for _ in 0..self.config.need_recompute_max_retries {
            let scan = self
                .repository
                .scan_schedule_due(shard, self.config.scan_batch_size)
                .await?;
            if !scan.namespace_enabled {
                // 暂停期间保留 schedule 的权威逻辑时刻，不生成必然被脚本拒绝的触发请求。
                return Ok(outcome);
            }
            let mut requests = Vec::new();
            for entry in &scan.entries {
                let Some(definition) = self.definitions.get(&entry.job_name) else {
                    continue;
                };
                if definition.trigger() == JobTrigger::FanoutOnly
                    || definition.definition_revision() != entry.definition_revision
                {
                    continue;
                }
                outcome.schedule_lag_ms = outcome
                    .schedule_lag_ms
                    .max(scan.redis_now.saturating_sub(entry.logical_fire_at).max(0));
                requests.extend(self.fire_requests(
                    definition,
                    entry.logical_fire_at,
                    scan.redis_now,
                )?);
            }

            let mut need_recompute = false;
            for batch in requests.chunks(self.config.scan_batch_size as usize) {
                let results = self.repository.fire_due_batch(shard, batch).await?;
                for result in results {
                    let fire_result = JobFireResult::from_script_code(&result.code)
                        .ok_or_else(|| config_error("批量触发返回未知状态码"))?;
                    *outcome.fire_results.entry(fire_result).or_default() += 1;
                    match fire_result {
                        JobFireResult::Fired | JobFireResult::Adopted => {
                            outcome.fired = outcome.fired.saturating_add(1);
                        }
                        JobFireResult::NeedRecompute => need_recompute = true,
                        _ => {}
                    }
                }
                if need_recompute {
                    break;
                }
            }
            if !need_recompute {
                return Ok(outcome);
            }
        }
        Ok(outcome)
    }

    /// 业务作用：按定义的误触发策略把单个到期 score 展开为可连续 CAS 的有限请求链。
    ///
    /// 参数说明：
    /// - `definition`: 目标定义。
    /// - `expected`: 当前权威逻辑时刻。
    /// - `redis_now`: 扫描脚本观测到的 Redis 时间。
    ///
    /// 返回：包含跳过项与执行项的有序链；计算溢出或无法在预算内推进时返回配置错误。
    fn fire_requests<'a>(
        &self,
        definition: &'a JobDefinition,
        expected: i64,
        redis_now: i64,
    ) -> Result<Vec<FireDueBatchItem<'a>>> {
        let missed =
            redis_now.saturating_sub(expected) > self.config.schedule_rtt_allowance_ms as i64;
        if definition.schedule_type() == JobScheduleType::FixedDelay {
            if missed && definition.misfire() == JobMisfire::DoNothing {
                return Ok(vec![item(
                    definition,
                    expected,
                    self.next_after_now(definition, expected, redis_now)?,
                    true,
                    true,
                    true,
                )]);
            }
            return Ok(vec![item(definition, expected, 0, false, missed, false)]);
        }
        if !missed {
            return Ok(vec![item(
                definition,
                expected,
                next_fire_at(definition, expected)?,
                false,
                false,
                true,
            )]);
        }
        match definition.misfire() {
            JobMisfire::DoNothing => Ok(vec![item(
                definition,
                expected,
                self.next_after_now(definition, expected, redis_now)?,
                true,
                true,
                true,
            )]),
            JobMisfire::FireOnceNow => Ok(vec![item(
                definition,
                expected,
                self.next_after_now(definition, expected, redis_now)?,
                false,
                true,
                true,
            )]),
            JobMisfire::CatchUp => self.catch_up_requests(definition, expected, redis_now),
        }
    }

    /// 业务作用：只保留补偿窗口内最近的有限逻辑时刻，并用跳过项跨过更早积压。
    ///
    /// 参数说明：
    /// - `definition`: 目标定义。
    /// - `expected`: 当前权威逻辑时刻。
    /// - `redis_now`: 当前 Redis 时间。
    ///
    /// 返回：先跳过旧积压、再补偿最近时刻的有序请求链。
    fn catch_up_requests<'a>(
        &self,
        definition: &'a JobDefinition,
        expected: i64,
        redis_now: i64,
    ) -> Result<Vec<FireDueBatchItem<'a>>> {
        let cutoff = redis_now
            .saturating_sub(self.config.max_catch_up_window_ms as i64)
            .max(0);
        let first = self.first_at_or_after(definition, expected, cutoff)?;
        let limit = self.config.max_catch_up_runs as usize;
        let mut selected = VecDeque::with_capacity(limit);
        let cursor =
            if definition.schedule_type() == JobScheduleType::FixedRate && first <= redis_now {
                let interval = definition.interval_ms() as i64;
                let total = (redis_now - first) / interval + 1;
                let retained = total.min(self.config.max_catch_up_runs as i64);
                let selected_first = checked_add_mul(first, total - retained, interval)?;
                for index in 0..retained {
                    selected.push_back(checked_add_mul(selected_first, index, interval)?);
                }
                checked_add_mul(first, total, interval)?
            } else {
                let mut next = first;
                let mut advanced = 0;
                while next <= redis_now {
                    if selected.len() == limit {
                        selected.pop_front();
                    }
                    selected.push_back(next);
                    next = next_fire_at(definition, next)?;
                    advanced += 1;
                    if advanced > CRON_ADVANCE_LIMIT {
                        return Err(config_error("CATCH_UP 推进次数超过安全上限"));
                    }
                }
                next
            };

        if selected.is_empty() {
            return Ok(vec![item(definition, expected, cursor, true, true, true)]);
        }
        let mut requests = Vec::with_capacity(selected.len() + 1);
        let first_selected = *selected.front().expect("selected 已确认非空");
        if first_selected != expected {
            requests.push(item(
                definition,
                expected,
                first_selected,
                true,
                true,
                false,
            ));
        }
        let times: Vec<i64> = selected.into_iter().collect();
        for (index, current) in times.iter().copied().enumerate() {
            let next = times.get(index + 1).copied().unwrap_or(cursor);
            requests.push(item(
                definition,
                current,
                next,
                false,
                true,
                index + 1 == times.len(),
            ));
        }
        Ok(requests)
    }

    /// 业务作用：推进到严格晚于当前 Redis 时间的下一逻辑时刻，供不追赶策略一次跨过历史积压。
    ///
    /// 参数说明：
    /// - `definition`: 目标定义。
    /// - `logical_fire_at`: 当前逻辑时刻。
    /// - `redis_now`: 当前 Redis 时间。
    ///
    /// 返回：严格晚于 `redis_now` 的下一时刻；预算内无法推进时返回配置错误。
    fn next_after_now(
        &self,
        definition: &JobDefinition,
        logical_fire_at: i64,
        redis_now: i64,
    ) -> Result<i64> {
        first_fire_at_after(definition, logical_fire_at, redis_now)
    }

    /// 业务作用：定位不早于补偿窗口下界的第一个逻辑时刻，固定速率使用算术跳跃避免毫秒级长循环。
    ///
    /// 参数说明：
    /// - `definition`: 目标定义。
    /// - `expected`: 当前权威逻辑时刻。
    /// - `cutoff`: 补偿窗口下界。
    ///
    /// 返回：第一个不早于下界的逻辑时刻；溢出或 Cron 推进超限时返回错误。
    fn first_at_or_after(
        &self,
        definition: &JobDefinition,
        expected: i64,
        cutoff: i64,
    ) -> Result<i64> {
        if expected >= cutoff {
            return Ok(expected);
        }
        if definition.schedule_type() == JobScheduleType::FixedRate {
            let interval = definition.interval_ms() as i64;
            let distance = cutoff - expected;
            let steps = distance
                .checked_add(interval - 1)
                .ok_or_else(|| config_error("固定速率补偿距离溢出"))?
                / interval;
            return checked_add_mul(expected, steps, interval);
        }
        let mut cursor = expected;
        for _ in 0..CRON_ADVANCE_LIMIT {
            if cursor >= cutoff {
                return Ok(cursor);
            }
            cursor = next_fire_at(definition, cursor)?;
        }
        Err(config_error("Cron 无法在推进预算内进入补偿窗口"))
    }
}

/// 业务作用：构造一项批量触发请求，集中保持标志位顺序与 Lua 合同一致。
///
/// 参数说明：`definition`、逻辑时刻、下一时刻及三个策略标志组成一次 CAS 项。
///
/// 返回：可直接交给 `fire_due_batch` 的借用项。
fn item(
    definition: &JobDefinition,
    logical_fire_at: i64,
    proposed_next_fire_at: i64,
    skip_run: bool,
    misfire: bool,
    must_end_in_future: bool,
) -> FireDueBatchItem<'_> {
    FireDueBatchItem {
        definition,
        logical_fire_at,
        proposed_next_fire_at,
        skip_run,
        misfire,
        must_end_in_future,
    }
}

/// 业务作用：完成固定速率时刻的受检乘加，避免补偿链在整数回绕后写入错误 score。
///
/// 参数说明：`base + count * interval` 的三个有符号整数分量。
///
/// 返回：未溢出的毫秒时刻；乘法或加法溢出返回配置错误。
fn checked_add_mul(base: i64, count: i64, interval: i64) -> Result<i64> {
    let delta = count
        .checked_mul(interval)
        .ok_or_else(|| config_error("固定速率补偿乘法溢出"))?;
    base.checked_add(delta)
        .ok_or_else(|| config_error("固定速率补偿时刻溢出"))
}

/// 业务作用：构造调度计算配置错误，阻止非法逻辑时刻进入 Redis 共享索引。
///
/// 参数说明：
/// - `message`: 不含业务载荷的稳定摘要。
///
/// 返回：统一配置错误。
fn config_error(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(format!("scanner {message}")).into()
}
