use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use super::{ConfigError, ConfigPath, ErrorKind, Result};

/// 解析、来源和展开共享的有限预算。
#[derive(Clone, Debug)]
pub struct LoadLimits {
    /// 单份来源正文的最大字节数。
    pub source_bytes: usize,
    /// 一轮装配全部来源正文的累计字节上限。
    pub total_bytes: usize,
    /// 导入声明的最大数量。
    pub declarations: usize,
    /// 文件通配模式的最大数量。
    pub patterns: usize,
    /// 主文件、profile 和导入共同占用的文档数量上限。
    pub sources: usize,
    /// 单个文件模式允许匹配的最大文件数。
    pub matches_per_pattern: usize,
    /// 单次目录枚举允许检查的最大条目数。
    pub directory_entries: usize,
    /// 文档层级、表达式嵌套及求值依赖深度上限。
    pub depth: usize,
    /// 文档解析、候选树及观察目标规模的节点上限。
    pub nodes: usize,
    /// 单个文本值允许占用的最大字节数。
    pub string_bytes: usize,
    /// 表达式求值累计展开文本的字节上限。
    pub expanded_bytes: usize,
    /// 文本累计规模及最终候选序列化的字节上限。
    pub output_bytes: usize,
    /// 一轮求值允许访问依赖节点的次数上限。
    pub dependency_visits: usize,
    /// 同时保留的非递归目录观察数量上限。
    pub watch_directories: usize,
}

impl Default for LoadLimits {
    /// 业务作用：为独立配置装配提供有限的默认资源范围。
    /// 参数说明：无。
    /// 返回：主文件、profile 和导入共享的预算。
    fn default() -> Self {
        Self {
            source_bytes: 4 * 1024 * 1024,
            total_bytes: 32 * 1024 * 1024,
            declarations: 128,
            patterns: 64,
            sources: 128,
            matches_per_pattern: 64,
            directory_entries: 4096,
            depth: 64,
            nodes: 100_000,
            string_bytes: 1024 * 1024,
            expanded_bytes: 16 * 1024 * 1024,
            output_bytes: 32 * 1024 * 1024,
            dependency_visits: 200_000,
            watch_directories: 256,
        }
    }
}

/// 显式关闭与未提供选择相区分，避免多个 loader 相互依赖进程环境变动。
#[derive(Clone, Debug, Default)]
pub enum ProfileSelection {
    /// 从固定环境快照读取 profile 名称。
    #[default]
    Environment,
    /// 关闭 profile 文件选择，不读取环境中的 profile 设置。
    Disabled,
    /// 使用调用方指定的 profile 名称。
    Named(String),
}

/// 调用方提供的受信路径类型与表达式策略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueHint {
    /// 要求求值结果保持字符串，拒绝其它类型。
    String,
    /// 将可解析的布尔或数字文本转为标量，其余文本保持字符串。
    Scalar,
    /// 将文本按严格 JSON 值解析，非法 JSON 直接拒绝。
    Json,
    /// 保留原始文本，不解释该子树中的占位符。
    Literal,
    /// 显式恢复表达式求值，可覆盖父路径的字面量提示；仍受全局开关约束。
    Resolve,
}

/// 严格加载策略由调用方固定，不从导入业务文档取值。
#[derive(Clone, Debug)]
pub struct LoadPolicy {
    /// 读取、解析、展开和观察共用的有限预算。
    pub limits: LoadLimits,
    /// 允许按环境映射规则覆盖配置树，默认开启。
    pub environment_overlay: bool,
    /// 配置引用未命中时允许查询环境原样键及规范化别名，默认开启。
    pub environment_fallback: bool,
    /// 允许环境覆盖以严格 JSON 表示数组或对象，默认关闭。
    pub structured_environment: bool,
    /// 启用占位符求值，默认开启。
    pub placeholders: bool,
    /// 无法求值的引用是否保留原文；默认关闭并返回错误。
    pub preserve_unresolved: bool,
    /// 已选择的 profile 不存在时拒绝装配，默认开启。
    pub strict_profile: bool,
    /// 多种 profile 扩展名同时存在时拒绝装配，默认开启。
    pub reject_ambiguous_profile: bool,
    /// 文件读取和观察允许的根目录；空集合表示不附加目录限制。
    pub allowed_roots: Vec<PathBuf>,
    /// 按结构化字段路径设置的类型和表达式提示，最长父路径优先。
    pub hints: BTreeMap<ConfigPath, ValueHint>,
    /// 宿主设置后在阶段边界停止装配的共享标记，不中断已经进行的同步读取。
    pub cancelled: Arc<AtomicBool>,
}

impl Default for LoadPolicy {
    /// 业务作用：默认启用明确环境来源与严格表达式，拒绝含糊 profile。
    /// 参数说明：无。
    /// 返回：不读取外部策略的初始加载约束。
    fn default() -> Self {
        Self {
            limits: LoadLimits::default(),
            environment_overlay: true,
            environment_fallback: true,
            structured_environment: false,
            placeholders: true,
            preserve_unresolved: false,
            strict_profile: true,
            reject_ambiguous_profile: true,
            allowed_roots: Vec::new(),
            hints: BTreeMap::new(),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl LoadPolicy {
    /// 业务作用：在有界工作阶段之间响应宿主撤销权威。
    /// 参数说明：无。
    /// 返回：未取消时成功；取消后拒绝产生新候选。
    pub fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(ConfigError::new(ErrorKind::Cancelled))
        } else {
            Ok(())
        }
    }

    /// 业务作用：选择最具体的字段策略，父级声明可保护整棵子树。
    /// 参数说明：`path` 是当前字段位置。
    /// 返回：最长匹配路径上的类型或求值策略。
    pub(crate) fn hint(&self, path: &ConfigPath) -> Option<ValueHint> {
        self.hints
            .iter()
            .filter(|(parent, _)| path.starts_with(parent))
            .max_by_key(|(parent, _)| parent.0.len())
            .map(|(_, hint)| *hint)
    }
}
