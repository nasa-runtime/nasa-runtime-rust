//! 数据库 migration 的后端中立状态裁决。
//!
//! 本 crate 只接收不含 SQL 正文的 migration 描述与数据库状态，输出稳定差异和错误分类。连接、锁、
//! catalog 查询和 SQL 执行由具体数据库 adapter 负责。

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};

use serde::Deserialize;

/// 有界 advisory lock 等待允许的最大毫秒数；`0` 保留无显式截止时间合同。
pub const MAX_MIGRATION_LOCK_TIMEOUT_MS: u64 = 365 * 24 * 60 * 60 * 1000;

/// migration 门禁模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MigrationMode {
    /// 跳过校验和应用。
    Disabled,
    /// 只读校验嵌入声明与数据库状态一致。
    #[default]
    Validate,
    /// 校验已有记录并应用未决 migration。
    Apply,
}

/// 后端共用的 migration 门禁配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MigrationSettings {
    /// 门禁模式；默认 `validate`。
    pub mode: MigrationMode,
    /// 获取专有连接和 advisory lock 的端到端等待上限毫秒。
    pub lock_timeout_ms: u64,
    /// 是否允许绕过 dirty 状态；当前固定拒绝。
    pub allow_dirty: bool,
}

impl Default for MigrationSettings {
    /// 业务作用: 使用只读校验、30 秒锁预算并拒绝绕过 dirty 状态的生产保守配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 可直接用于启动期只读门禁的默认配置。
    fn default() -> Self {
        Self {
            mode: MigrationMode::default(),
            lock_timeout_ms: 30_000,
            allow_dirty: false,
        }
    }
}

impl MigrationSettings {
    /// 业务作用: 在建立连接前校验 migration 安全旋钮，阻止无法证明安全的续跑和不可表示的等待预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 配置满足门禁时成功；dirty override 或超出硬上限的等待预算返回稳定分类。
    pub fn validate(&self) -> Result<(), MigrationError> {
        if self.allow_dirty {
            return Err(MigrationError::DirtyOverrideUnsupported);
        }
        if self.lock_timeout_ms > MAX_MIGRATION_LOCK_TIMEOUT_MS {
            return Err(MigrationError::InvalidLockTimeout(self.lock_timeout_ms));
        }
        Ok(())
    }
}

/// 不含 SQL 正文的嵌入式 migration 描述。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedMigration {
    /// 业务 migration 版本。
    pub version: i64,
    /// SQL 内容的稳定 checksum。
    pub checksum: Vec<u8>,
    /// 是否存在对应的 down migration。
    pub reversible: bool,
    /// 是否应在普通数据库事务中执行。
    pub transactional: bool,
}

/// 数据库 catalog 中的一条 migration 执行记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    /// 已登记的 migration 版本。
    pub version: i64,
    /// 数据库记录的 checksum。
    pub checksum: Vec<u8>,
    /// 执行是否完整成功；失败记录代表 dirty 状态。
    pub success: bool,
}

/// 已应用状态与嵌入声明比较后的稳定结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationComparison {
    /// 尚未应用的嵌入版本，按升序排列。
    pub pending: Vec<i64>,
    /// 数据库存在但嵌入声明缺失的版本，按升序排列。
    pub extra: Vec<i64>,
    /// 已应用但 checksum 与嵌入声明不同的首个版本。
    pub checksum_drift: Option<i64>,
    /// 首个未成功完成的数据库记录。
    pub dirty: Option<i64>,
}

impl MigrationComparison {
    /// 业务作用: 判断嵌入声明与数据库状态是否完全一致。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 不存在 pending、extra、checksum drift 和 dirty 时返回 `true`。
    pub fn is_clean(&self) -> bool {
        self.pending.is_empty()
            && self.extra.is_empty()
            && self.checksum_drift.is_none()
            && self.dirty.is_none()
    }

    /// 业务作用: 按稳定优先级把差异收敛为公开门禁错误。
    ///
    /// # 参数
    /// - `ignore_extra`: 是否接受数据库中存在但嵌入声明缺失的历史版本。
    ///
    /// 返回: dirty、checksum drift、extra、pending 依次裁决；没有阻断时成功。
    pub fn ensure_valid(&self, ignore_extra: bool) -> Result<(), MigrationError> {
        self.ensure_applicable(ignore_extra)?;
        if !self.pending.is_empty() {
            return Err(MigrationError::Pending(self.pending.clone()));
        }
        Ok(())
    }

    /// 业务作用: 校验已有数据库记录是否允许继续应用未决 migration。
    ///
    /// # 参数
    /// - `ignore_extra`: 是否接受数据库中存在但嵌入声明缺失的历史版本。
    ///
    /// 返回: dirty、checksum drift 或不允许的 extra 会阻断；pending 留给 adapter 执行。
    pub fn ensure_applicable(&self, ignore_extra: bool) -> Result<(), MigrationError> {
        if let Some(version) = self.dirty {
            return Err(MigrationError::Dirty(version));
        }
        if let Some(version) = self.checksum_drift {
            return Err(MigrationError::ChecksumMismatch(version));
        }
        if !ignore_extra && !self.extra.is_empty() {
            return Err(MigrationError::Extra(self.extra.clone()));
        }
        Ok(())
    }
}

/// migration 门禁成功摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// 实际执行的模式。
    pub mode: MigrationMode,
    /// 嵌入的 up migration 总数。
    pub embedded: usize,
    /// 本次实际应用的 migration 数量。
    pub applied: usize,
}

/// migration 门禁稳定失败分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationError {
    /// 存在尚未应用的嵌入版本。
    Pending(Vec<i64>),
    /// 数据库存在但嵌入声明缺失的历史版本。
    Extra(Vec<i64>),
    /// 已应用版本的 checksum 与嵌入声明不一致。
    ChecksumMismatch(i64),
    /// migration 表存在失败记录。
    Dirty(i64),
    /// 未在配置预算内取得 advisory lock。
    LockTimeout(u64),
    /// `allow_dirty=true` 不具备可通用证明的安全语义。
    DirtyOverrideUnsupported,
    /// 锁等待超出框架可安全表示的 deadline。
    InvalidLockTimeout(u64),
    /// schema identifier 不满足 adapter 的有界安全合同。
    InvalidSchema,
    /// 受管 schema 在目标 database 中不存在。
    SchemaNotFound,
    /// 专用 migration endpoint 与业务 datasource 身份不一致。
    TargetMismatch,
    /// 非事务 migration 缺少可验证的完成证据。
    NonTransactionalEvidenceRequired(i64),
    /// 非事务 SQL 返回成功后，声明的完成证据仍不成立。
    CompletionEvidenceMissing(i64),
    /// 非事务 migration 超出自身独立执行预算。
    NonTransactionalTimeout(i64, u64),
    /// 底层连接、catalog 或 migration 执行失败的脱敏分类。
    Backend(String),
}

impl std::fmt::Display for MigrationError {
    /// 业务作用: 输出不含 SQL 正文、schema 名称或数据库凭据的稳定错误信息。
    ///
    /// # 参数
    /// - `formatter`: 标准格式化输出目标。
    ///
    /// 返回: 将当前失败分类写入输出目标的结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending(versions) => {
                write!(formatter, "migrations not applied: versions {versions:?}")
            }
            Self::Extra(versions) => {
                write!(
                    formatter,
                    "database has migrations missing from source: versions {versions:?}"
                )
            }
            Self::ChecksumMismatch(version) => {
                write!(
                    formatter,
                    "migration checksum mismatch at version {version}"
                )
            }
            Self::Dirty(version) => write!(
                formatter,
                "migration {version} is partially applied; inspect it before startup"
            ),
            Self::LockTimeout(timeout_ms) => write!(
                formatter,
                "migration lock was not acquired within {timeout_ms}ms"
            ),
            Self::DirtyOverrideUnsupported => write!(
                formatter,
                "allow_dirty=true is unsafe and unsupported; inspect the dirty migration explicitly"
            ),
            Self::InvalidLockTimeout(timeout_ms) => write!(
                formatter,
                "migration lock timeout {timeout_ms}ms exceeds the framework hard limit"
            ),
            Self::InvalidSchema => write!(formatter, "migration schema identifier is invalid"),
            Self::SchemaNotFound => write!(formatter, "migration schema does not exist"),
            Self::TargetMismatch => write!(
                formatter,
                "migration endpoint does not match the business database and schema"
            ),
            Self::NonTransactionalEvidenceRequired(version) => write!(
                formatter,
                "non-transactional migration {version} requires explicit completion evidence"
            ),
            Self::CompletionEvidenceMissing(version) => write!(
                formatter,
                "non-transactional migration {version} completed without verifiable evidence"
            ),
            Self::NonTransactionalTimeout(version, timeout_ms) => write!(
                formatter,
                "non-transactional migration {version} exceeded {timeout_ms}ms"
            ),
            Self::Backend(reason) => write!(formatter, "migration backend error: {reason}"),
        }
    }
}

impl std::error::Error for MigrationError {}

/// 业务作用: 比较嵌入式声明和数据库 catalog，生成不含 SQL 正文的确定性差异。
///
/// # 参数
/// - `embedded`: 当前二进制携带的 up migration 描述。
/// - `applied`: 数据库 migration 表中的执行记录。
///
/// 返回: 按版本升序排列的 pending/extra，并给出首个 checksum drift 与 dirty 版本。
pub fn compare_migrations(
    embedded: &[EmbeddedMigration],
    applied: &[AppliedMigration],
) -> MigrationComparison {
    let embedded_by_version: HashMap<_, _> = embedded
        .iter()
        .map(|migration| (migration.version, migration))
        .collect();
    let embedded_versions: HashSet<_> = embedded_by_version.keys().copied().collect();

    let mut applied_success = HashMap::new();
    let mut dirty = None;
    for migration in applied {
        if migration.success {
            applied_success.insert(migration.version, migration);
        } else if dirty.is_none_or(|current| migration.version < current) {
            dirty = Some(migration.version);
        }
    }

    let mut pending = embedded_versions
        .iter()
        .filter(|version| !applied_success.contains_key(version))
        .copied()
        .collect::<Vec<_>>();
    let mut extra = applied_success
        .keys()
        .filter(|version| !embedded_versions.contains(version))
        .copied()
        .collect::<Vec<_>>();
    pending.sort_unstable();
    extra.sort_unstable();

    let checksum_drift = embedded_by_version
        .iter()
        .filter_map(|(version, expected)| {
            applied_success
                .get(version)
                .filter(|actual| actual.checksum != expected.checksum)
                .map(|_| *version)
        })
        .min();

    MigrationComparison {
        pending,
        extra,
        checksum_drift,
        dirty,
    }
}
