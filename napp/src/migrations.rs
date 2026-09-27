//! 嵌入式迁移计划在 DB Prepare 前统一冻结，适用于 Service 和 Batch。

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, Migrator};

/// 无外部副作用的迁移工厂；只返回 datasource 身份和嵌入式 SQL。
pub type MigrationPlanFactory = fn() -> Vec<(String, Migrator)>;

/// 业务通过 linkme 登记的静态计划，执行顺序由 datasource 门禁负责。
#[linkme::distributed_slice]
pub static MIGRATION_PLANS: [MigrationPlanFactory];

/// 业务作用：合并静态计划与 Service 动态计划，在迁移 SQL 执行前拒绝重复来源。
/// 参数说明：`runtime` 为 UserHook 登记的迁移集合。
/// 返回：至多 128 个唯一 datasource 计划；重复、空名称或容量超限时拒绝。
pub(crate) fn collect(
    mut runtime: Vec<(String, Migrator)>,
) -> ApplicationResult<Vec<(String, Migrator)>> {
    // 动态计划不依赖静态工厂存在；先检查自身容量，保证任何来源组合都在执行迁移 SQL 前受限。
    if runtime.len() > 128 || MIGRATION_PLANS.len() > 128 {
        return Err(error());
    }
    for factory in MIGRATION_PLANS {
        runtime.extend(factory());
        if runtime.len() > 128 {
            return Err(error());
        }
    }
    let mut names = std::collections::HashSet::new();
    for (name, _) in &runtime {
        if name.is_empty() || name.len() > 128 || name.trim() != name || !names.insert(name) {
            return Err(error());
        }
    }
    Ok(runtime)
}

/// 业务作用：为迁移计划门禁返回不含 SQL 的固定原因。
/// 参数说明：无。
/// 返回：Prepare 错误，工作负载不得开始。
fn error() -> ApplicationError {
    ApplicationError::new(
        ComponentId::Db,
        ApplicationPhase::Prepare,
        "migration plans contain invalid, duplicate or excessive datasource names",
    )
}
