//! 带不可变版本的 definition 注册表与启动预检。
//!
//! definition 一经发布内容不可变（§11.5.4）：同 `(workflow, version)` 注册不同摘要的
//! 内容会被拒绝。启动预检校验全部非终态实例引用的 `(workflow, version, digest)`
//! 可用且一致——缺失或漂移时拒绝 Ready，绝不用新内容驱动旧实例。

use std::collections::BTreeMap;

use nasaga_core::{DefinitionVersion, ResolutionMode, SagaId, WorkflowDefinition, WorkflowName};
use nasaga_mysql::MySqlSagaStore;

/// 业务作用：持有本进程可驱动的全部 workflow definition，按 `(名称, 定义版本)` 索引。
///
/// 注册表只在启动阶段构建（`register` 全部完成后再交给 Orchestrator），运行期只读，
/// 因此无内部锁；定义演进并存表示同名 workflow 注册多个版本。
#[derive(Debug, Default)]
pub struct DefinitionRegistry {
    definitions: BTreeMap<(String, u32), WorkflowDefinition>,
}

impl DefinitionRegistry {
    /// 业务作用：创建空注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含任何 definition 的注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：注册一个已通过构造预检的 definition，同一版本重复注册按摘要裁决。
    ///
    /// 参数说明：
    /// - `definition`: 待注册的流程定义。
    ///
    /// 返回：解决模式已具备 hosted 闭环且新增或摘要一致时返回 `Ok`；Callback/Manual
    /// 或同 `(workflow, version)` 摘要不一致返回错误，不允许带不完整合同启动。
    pub fn register(&mut self, definition: WorkflowDefinition) -> anyhow::Result<()> {
        validate_runtime_resolution_modes(&definition)?;
        let key = (
            definition.name().as_str().to_string(),
            definition.version().get(),
        );
        if let Some(existing) = self.definitions.get(&key) {
            // definition 不可变:同一版本出现两份不同内容说明发布纪律被破坏,
            // 静默用后者覆盖会让运行中实例被新内容驱动。
            if existing.digest() != definition.digest() {
                anyhow::bail!(
                    "definition `{}` v{} already registered with a different digest",
                    key.0,
                    key.1
                );
            }
            return Ok(());
        }
        self.definitions.insert(key, definition);
        Ok(())
    }

    /// 业务作用：按名称与定义版本查找 definition，供推进裁决使用实例固定的内容。
    ///
    /// 参数说明：
    /// - `workflow`: workflow 名称。
    /// - `version`: definition 版本。
    ///
    /// 返回：存在返回定义引用；不存在返回 `None`。
    pub fn get(
        &self,
        workflow: &WorkflowName,
        version: DefinitionVersion,
    ) -> Option<&WorkflowDefinition> {
        self.definitions
            .get(&(workflow.as_str().to_string(), version.get()))
    }

    /// 业务作用：启动预检——校验全部非终态实例引用的 definition 可用且摘要一致。
    ///
    /// 缺失或摘要漂移时必须拒绝 Ready 而不是带病启动：用新内容驱动旧实例会让补偿
    /// 顺序、身份派生与冻结计划全部失真。
    ///
    /// 参数说明：
    /// - `store`: Saga store。
    /// - `scan_limit`: 单次扫描的非终态实例上限。
    ///
    /// 返回：全部一致返回 `Ok`；任一实例缺 definition 或摘要不一致返回聚合错误
    /// （只含 workflow/version 计数，不回显业务键）。
    pub async fn verify_non_terminal(
        &self,
        store: &MySqlSagaStore,
        scan_limit: u32,
    ) -> anyhow::Result<()> {
        let mut missing = 0usize;
        let mut drifted = 0usize;
        let mut cursor: Option<SagaId> = None;
        loop {
            let instances = store
                .list_non_terminal_after(cursor.as_ref(), scan_limit)
                .await?;
            if instances.is_empty() {
                break;
            }
            for instance in &instances {
                match self.get(&instance.workflow, instance.definition_version) {
                    None => missing += 1,
                    Some(definition) if definition.digest() != instance.definition_digest => {
                        drifted += 1
                    }
                    Some(_) => {}
                }
            }
            // keyset cursor 来自本页最后一行；下一页严格使用 `saga_id > cursor`，
            // 既不会重复统计，也不会因 OFFSET 上游插入/删除而跳行。
            cursor = instances.last().map(|instance| instance.saga_id.clone());
            if instances.len() < scan_limit as usize {
                break;
            }
        }
        if missing > 0 || drifted > 0 {
            anyhow::bail!(
                "definition precheck failed: {missing} non-terminal instance(s) reference \
                 unregistered definitions, {drifted} reference drifted digests; refusing Ready"
            );
        }
        Ok(())
    }
}

/// 业务作用：拒绝当前 Orchestrator 尚无可靠接入闭环的 Callback/Manual 解决模式。
///
/// runtime 目前只会为 `Poll` 原子登记 attempt、发布 resolve Outbox 并布置 timeout；若接收
/// Callback/Manual definition，结果没有已登记 command 锚点，实例只能在预算耗尽后转人工。
/// 与其运行时悬挂，必须在注册阶段 fail-fast。
///
/// 参数说明：
/// - `definition`: 待加入运行时注册表的已校验 definition。
///
/// 返回：所有允许 Unknown 的步骤都使用 `Poll` 时返回 `Ok`；否则返回启动配置错误。
fn validate_runtime_resolution_modes(definition: &WorkflowDefinition) -> anyhow::Result<()> {
    if let Some(step) = definition.steps().iter().find(|step| {
        step.resolution().allow_unknown() && step.resolution().mode() != Some(ResolutionMode::Poll)
    }) {
        anyhow::bail!(
            "definition `{}` v{} step `{}` uses an unsupported Saga runtime resolution mode; only poll is hosted",
            definition.name().as_str(),
            definition.version().get(),
            step.name().as_str()
        );
    }
    Ok(())
}
