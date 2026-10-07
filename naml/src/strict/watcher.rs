use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use super::{ConfigError, ErrorKind, FilePattern, LoadPolicy, Result, WatchPlan};
use crate::watch::WatchPathIdentity;

/// 观察信号不携带原路径，丢事件时不要求虚构某个命中文件。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigWatchEvent {
    /// 收到与当前来源或模式相关的文件变化。
    Changed,
    /// 后端提示事件可能不完整，需要重新扫描完整来源计划。
    Rescan,
    /// 文件观察后端报告错误，需要检查健康状态并补读来源。
    BackendError,
}

/// 可查询的观察状态与资源占用，不等同于配置已经应用。
#[derive(Clone, Debug, Default)]
pub struct WatchHealth {
    /// 后端是否保持正常；不能据此判断业务配置已应用。
    pub backend_ok: bool,
    /// 事件处理回调是否曾发生 panic。
    pub handler_failed: bool,
    /// 是否仍需通过完整装配补读来源。
    pub needs_rescan: bool,
    /// 当前来源计划要求观察的目录数量。
    pub required_directories: usize,
    /// 当前观察器实际保留的目录数量。
    pub watched_directories: usize,
    /// 已不属于当前计划但尚未成功解除观察的目录数量。
    pub retained_directories: usize,
}

struct Targets {
    exact: HashSet<PathBuf>,
    patterns: Vec<(FilePattern, HashSet<PathBuf>)>,
    recovery: Vec<PathBuf>,
    directories: HashSet<PathBuf>,
}

/// 严格来源观察器只发有界信号，不读取或发布业务配置。
pub struct ConfigWatcher {
    watcher: RecommendedWatcher,
    targets: Arc<RwLock<Targets>>,
    directories: HashSet<PathBuf>,
    health: Arc<Mutex<WatchHealth>>,
    policy: LoadPolicy,
}

impl Targets {
    /// 业务作用：在回调外固定所有路径别名，回调只做内存匹配。
    /// 参数说明：`plan` 是真实加载及缺失候选；`policy` 约束目录规模。
    /// 返回：含模式目录与恢复祖先的完整目标。
    fn prepare(plan: &WatchPlan, policy: &LoadPolicy) -> Result<Self> {
        policy.check_cancelled()?;
        let mut targets = Self {
            exact: HashSet::new(),
            patterns: Vec::new(),
            recovery: Vec::new(),
            directories: HashSet::new(),
        };
        for path in plan
            .files
            .iter()
            .map(|file| file.path())
            .chain(plan.missing_files.iter().map(PathBuf::as_path))
            .chain(plan.profile_candidates.iter().map(PathBuf::as_path))
            .chain(plan.dependencies.iter().map(PathBuf::as_path))
        {
            let identity = WatchPathIdentity::new(path);
            targets.exact.extend(identity.aliases);
            for directory in identity.directories {
                targets.exact.insert(directory.clone());
                targets.observe_directory(directory.clone(), policy)?;
                if let Some(parent) = directory.parent() {
                    if policy.allowed_roots.is_empty()
                        || policy
                            .allowed_roots
                            .iter()
                            .any(|root| parent.starts_with(root))
                    {
                        targets.observe_directory(parent.into(), policy)?;
                    }
                }
            }
        }
        for (pattern, _, _) in &plan.patterns {
            let identity = WatchPathIdentity::new(pattern.directory());
            for directory in identity.directories {
                targets.observe_directory(directory, policy)?;
            }
            let aliases = identity.aliases;
            for directory in &aliases {
                targets.observe_directory(directory.clone(), policy)?;
            }
            targets.exact.extend(aliases.iter().cloned());
            targets.patterns.push((pattern.clone(), aliases));
        }
        Ok(targets)
    }

    /// 业务作用：缺失目录使用最近可观察祖先，避免递归观察无关文件系统。
    /// 参数说明：`directory` 为所需目录；`policy` 包含允许根及限额。
    /// 返回：有限的非递归目录观察计划。
    fn observe_directory(&mut self, directory: PathBuf, policy: &LoadPolicy) -> Result<()> {
        let mut ancestor = directory.as_path();
        loop {
            match std::fs::metadata(ancestor) {
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => return Err(ConfigError::new(ErrorKind::Observation)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    ancestor = ancestor
                        .parent()
                        .ok_or_else(|| ConfigError::new(ErrorKind::Observation))?;
                }
                Err(_) => return Err(ConfigError::new(ErrorKind::Observation)),
            }
        }
        if !policy.allowed_roots.is_empty() {
            let real = std::fs::canonicalize(ancestor)
                .map_err(|_| ConfigError::new(ErrorKind::Observation))?;
            let inside = policy
                .allowed_roots
                .iter()
                .any(|root| std::fs::canonicalize(root).is_ok_and(|root| real.starts_with(root)));
            if !inside {
                return Err(ConfigError::new(ErrorKind::PolicyChanged));
            }
        }
        self.directories.insert(ancestor.into());
        if ancestor != directory {
            self.recovery.push(directory);
        }
        if self.directories.len() > policy.limits.watch_directories
            || self.exact.len() > policy.limits.nodes
        {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        Ok(())
    }

    /// 业务作用：仅通过既定身份与共同模式判断事件相关性。
    /// 参数说明：`path` 为后端提供的路径。
    /// 返回：命中已读文件、模式或缺失目录恢复边界时为真。
    fn matches(&self, path: &Path) -> bool {
        if self.exact.contains(path)
            || self
                .recovery
                .iter()
                .any(|directory| directory.starts_with(path))
        {
            return true;
        }
        let Some(parent) = path.parent() else {
            return false;
        };
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        self.patterns
            .iter()
            .any(|(pattern, directories)| directories.contains(parent) && pattern.matches(name))
    }
}

impl ConfigWatcher {
    /// 业务作用：为严格来源建立精确观察和无路径重扫通知。
    /// 参数说明：`plan` 是加载来源；`policy` 为观察预算；`handler` 应只发送合并唤醒信号。
    /// 返回：全部必要目录可观察才返回实例；失败不留下半可用实例。
    pub fn new(
        plan: &WatchPlan,
        policy: LoadPolicy,
        handler: impl Fn(ConfigWatchEvent) + Send + Sync + 'static,
    ) -> Result<Self> {
        let targets = Arc::new(RwLock::new(Targets::prepare(plan, &policy)?));
        let health = Arc::new(Mutex::new(WatchHealth {
            backend_ok: true,
            ..Default::default()
        }));
        let callback_targets = targets.clone();
        let callback_health = health.clone();
        let watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| {
                let signal = match result {
                    Err(_) => {
                        let mut state = callback_health
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.backend_ok = false;
                        state.needs_rescan = true;
                        Some(ConfigWatchEvent::BackendError)
                    }
                    Ok(event)
                        if event.need_rescan()
                            || event.paths.is_empty()
                            || matches!(event.kind, EventKind::Any | EventKind::Other) =>
                    {
                        callback_health
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .needs_rescan = true;
                        Some(ConfigWatchEvent::Rescan)
                    }
                    Ok(event)
                        if matches!(
                            event.kind,
                            EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                        ) =>
                    {
                        let targets = callback_targets
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        event
                            .paths
                            .iter()
                            .any(|path| targets.matches(path))
                            .then_some(ConfigWatchEvent::Changed)
                    }
                    _ => None,
                };
                if let Some(signal) = signal {
                    // 回调前已经释放观察锁；应用 handler 异常只影响健康状态，不使后端线程失去观察权威。
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(signal)))
                        .is_err()
                    {
                        let mut state = callback_health
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.handler_failed = true;
                        state.needs_rescan = true;
                    }
                }
            },
            notify::Config::default(),
        )
        .map_err(|_| ConfigError::new(ErrorKind::Observation))?;
        let mut instance = Self {
            watcher,
            targets,
            directories: HashSet::new(),
            health,
            policy,
        };
        instance.reconcile(plan)?;
        Ok(instance)
    }

    /// 业务作用：即使候选值未变化也按新的来源计划对账观察。
    /// 参数说明：`plan` 是新的来源事实。
    /// 返回：新增观察全部就绪才切换目标；撤销失败保留资源并记录状态。
    pub fn reconcile(&mut self, plan: &WatchPlan) -> Result<()> {
        let next = Targets::prepare(plan, &self.policy)?;
        if self.directories.union(&next.directories).count() > self.policy.limits.watch_directories
        {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        let mut mounted: Vec<PathBuf> = Vec::new();
        // 新旧观察同时存在的峰值已计费，先建立新增目录才能切换来源过滤。
        let additions = next
            .directories
            .difference(&self.directories)
            .cloned()
            .collect::<Vec<_>>();
        for directory in &additions {
            if self
                .watcher
                .watch(directory, RecursiveMode::NonRecursive)
                .is_err()
            {
                for prior in mounted {
                    if self.watcher.unwatch(&prior).is_err() {
                        self.directories.insert(prior);
                    }
                }
                self.update_health(false);
                return Err(ConfigError::new(ErrorKind::Observation));
            }
            mounted.push(directory.clone());
        }
        self.directories.extend(mounted);
        let old_directories = self
            .directories
            .difference(&next.directories)
            .cloned()
            .collect::<Vec<_>>();
        *self
            .targets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        // 目标已经完整切换，撤旧失败仅保留被过滤的额外观察，下一次对账再次清退。
        for directory in old_directories {
            if self.watcher.unwatch(&directory).is_ok() {
                self.directories.remove(&directory);
            }
        }
        self.update_health(true);
        Ok(())
    }

    /// 业务作用：查询观察健康与新旧集合切换的资源占用。
    /// 参数说明：无。
    /// 返回：与业务配置版本无关的观察状态。
    pub fn health(&self) -> WatchHealth {
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：调用方完成全量补读后确认重扫信号已经处理。
    /// 参数说明：无。
    /// 返回：清除重扫标记，后端健康仍由成功重建或后续状态决定。
    pub fn acknowledge_rescan(&self) {
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .needs_rescan = false;
    }

    /// 业务作用：对账结束后发布完整观察规模与退化状态。
    /// 参数说明：`success` 表示本次新增观察全部成功。
    /// 返回：更新内部健康快照。
    fn update_health(&self, success: bool) {
        let targets = self
            .targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let required = targets.directories.len();
        let mut state = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.required_directories = required;
        state.watched_directories = self.directories.len();
        state.retained_directories = self.directories.difference(&targets.directories).count();
        if !success {
            state.backend_ok = false;
            state.needs_rescan = true;
        }
    }
}
