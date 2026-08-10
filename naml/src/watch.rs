//! 精确文件目标的通用配置 watcher。

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::YmlLocalSources;

/// 配置文件事件所属来源。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum YmlWatchKind {
    /// 主配置文件发生变化。
    Base,
    /// 当前 profile 的任一候选文件发生变化。
    Profile,
    /// 应用附加的声明式依赖发生变化。
    Dependency,
}

/// 已经过精确路径过滤的配置变化事件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YmlWatchEvent {
    /// 变化来源类型。
    pub kind: YmlWatchKind,
    /// 与目标集合匹配的事件路径。
    pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
struct WatchPathIdentity {
    logical: PathBuf,
    aliases: HashSet<PathBuf>,
    directories: HashSet<PathBuf>,
}

impl WatchPathIdentity {
    /// 业务作用：同时保留声明路径、当前真实路径与符号链接节点，使真实文件修改和链接换代都可匹配。
    ///
    /// 参数说明：`path` 是 loader 来源、应用依赖或 notify 事件路径。
    ///
    /// 返回：稳定逻辑路径、全部等价别名，以及必须观察的逻辑和真实父目录。
    fn new(path: &Path) -> Self {
        let logical = absolute_watch_path(path);
        let mut aliases = HashSet::new();
        let mut visited = HashSet::new();
        collect_watch_aliases(&logical, &mut aliases, &mut visited, 0);
        if let Ok(canonical) = std::fs::canonicalize(&logical) {
            aliases.insert(canonical);
        }
        let directories = aliases
            .iter()
            .filter_map(|alias| alias.parent())
            .map(|directory| {
                std::fs::canonicalize(directory).unwrap_or_else(|_| directory.to_path_buf())
            })
            .collect();
        Self {
            logical,
            aliases,
            directories,
        }
    }

    /// 业务作用：判断一个操作系统事件路径是否与配置目标的任一逻辑或真实身份相交。
    ///
    /// 参数说明：`targets` 是某一来源类别的路径身份集合。
    ///
    /// 返回：任一身份相同时返回 `true`，否则返回 `false`。
    fn matches(&self, targets: &HashSet<PathBuf>) -> bool {
        !self.aliases.is_disjoint(targets)
    }
}

#[derive(Clone, Debug)]
struct WatchTargets {
    base: HashSet<PathBuf>,
    profiles: HashSet<PathBuf>,
    dependencies: HashSet<PathBuf>,
    directories: HashSet<PathBuf>,
}

impl WatchTargets {
    /// 业务作用：把 loader 来源与应用依赖归一成操作系统事件可比较的绝对路径集合。
    ///
    /// 参数说明：`sources` 是 naml 本地来源；`dependencies` 是应用声明的附加文件。
    ///
    /// 返回：按主文件、profile、附加依赖分组的精确目标集合。
    fn new(sources: &YmlLocalSources, dependencies: &[PathBuf]) -> Self {
        let base = WatchPathIdentity::new(sources.base_file());
        let mut directories = base.directories;
        let base = base.aliases;
        let mut profiles = HashSet::new();
        for path in sources.profile_watch_files() {
            let identity = WatchPathIdentity::new(path);
            profiles.extend(identity.aliases);
            directories.extend(identity.directories);
        }
        let mut dependency_targets = HashSet::new();
        for path in dependencies {
            let identity = WatchPathIdentity::new(path);
            dependency_targets.extend(identity.aliases);
            directories.extend(identity.directories);
        }
        Self {
            base,
            profiles,
            dependencies: dependency_targets,
            directories,
        }
    }

    /// 业务作用：将 notify 的目录级事件收窄为真实配置目标，防止日志和临时文件触发换版。
    ///
    /// 参数说明：`paths` 是单个 notify 事件携带的路径集合。
    ///
    /// 返回：按来源类型分组的匹配事件；无目标命中时返回空集合。
    fn classify(&self, paths: &[PathBuf]) -> Vec<YmlWatchEvent> {
        let observed = paths
            .iter()
            .map(|path| WatchPathIdentity::new(path))
            .collect::<Vec<_>>();
        [
            self.classified_event(YmlWatchKind::Base, &observed, &self.base),
            self.classified_event(YmlWatchKind::Profile, &observed, &self.profiles),
            self.classified_event(YmlWatchKind::Dependency, &observed, &self.dependencies),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// 业务作用：为单一来源类别汇总命中的逻辑事件路径，避免一个底层事件重复返回同一路径。
    ///
    /// 参数说明：`kind` 是来源类别；`observed` 是展开后的事件身份；`targets` 是目标身份集合。
    ///
    /// 返回：存在命中时返回分类事件，否则返回 `None`。
    fn classified_event(
        &self,
        kind: YmlWatchKind,
        observed: &[WatchPathIdentity],
        targets: &HashSet<PathBuf>,
    ) -> Option<YmlWatchEvent> {
        let mut seen = HashSet::new();
        let paths = observed
            .iter()
            .filter(|path| path.matches(targets))
            .filter(|path| seen.insert(path.logical.clone()))
            .map(|path| path.logical.clone())
            .collect::<Vec<_>>();
        (!paths.is_empty()).then_some(YmlWatchEvent { kind, paths })
    }

    /// 业务作用：计算必须监听的父目录，既捕获原子 rename，又避免递归观察无关目录树。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有精确目标的去重父目录集合。
    fn directories(&self) -> HashSet<PathBuf> {
        self.directories.clone()
    }
}

/// 可动态对账来源与附加依赖的目录级 watcher。
pub struct YmlWatcher {
    watcher: RecommendedWatcher,
    targets: Arc<RwLock<WatchTargets>>,
    watched_dirs: HashSet<PathBuf>,
}

impl YmlWatcher {
    /// 业务作用：建立精确配置 watcher，并把同步 notify 回调转换为中性 `YmlWatchEvent`。
    ///
    /// 参数说明：`sources` 是 naml 来源；`dependencies` 是应用附加依赖；`handler` 接收过滤后的事件。
    ///
    /// 返回：全部父目录成功挂载后返回 watcher；初始化或任一目录监听失败时返回错误。
    pub fn new(
        sources: &YmlLocalSources,
        dependencies: &[PathBuf],
        handler: impl Fn(YmlWatchEvent) + Send + Sync + 'static,
    ) -> Result<Self> {
        let targets = Arc::new(RwLock::new(WatchTargets::new(sources, dependencies)));
        let callback_targets = targets.clone();
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| {
                let event = match result {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::warn!(error = %error, "yml: 文件 watcher 后端报告观察异常");
                        return;
                    }
                };
                if !matches!(
                    event.kind,
                    EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                ) {
                    return;
                }
                // 在调用应用 handler 前释放内部读锁，避免慢处理阻塞来源对账。
                let classified = callback_targets
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .classify(&event.paths);
                for event in classified {
                    handler(event);
                }
            },
            notify::Config::default(),
        )
        .context("yml: 初始化文件 watcher 失败")?;
        let watched_dirs = targets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .directories();
        let mut mounted: Vec<PathBuf> = Vec::new();
        for directory in &watched_dirs {
            if let Err(error) = watcher.watch(directory, RecursiveMode::NonRecursive) {
                // 初始化不返回半可用实例；尽力撤销已挂载目录后让 watcher 整体析构。
                for prior in mounted {
                    let _ = watcher.unwatch(&prior);
                }
                return Err(error)
                    .with_context(|| format!("yml: 监听配置目录 {} 失败", directory.display()));
            }
            mounted.push(directory.clone());
        }
        Ok(Self {
            watcher,
            targets,
            watched_dirs,
        })
    }

    /// 业务作用：在成功解析新配置后原子更新目标集合，并增量挂载新增目录、撤销失效目录。
    ///
    /// 参数说明：`sources` 是新一轮 naml 来源；`dependencies` 是应用新解析出的附加依赖。
    ///
    /// 返回：新增目录全部监听成功时更新目标并返回成功；失败时保留旧目标和旧监听集合。
    pub fn reconcile(&mut self, sources: &YmlLocalSources, dependencies: &[PathBuf]) -> Result<()> {
        let next_targets = WatchTargets::new(sources, dependencies);
        let next_dirs = next_targets.directories();
        let additions = next_dirs
            .difference(&self.watched_dirs)
            .cloned()
            .collect::<Vec<_>>();
        let mut mounted: Vec<PathBuf> = Vec::new();
        // 先建立全部新增目录观察，避免发布新目标后存在尚未可见的事件窗口。
        for directory in &additions {
            if let Err(error) = self.watcher.watch(directory, RecursiveMode::NonRecursive) {
                let mut retained = Vec::new();
                for prior in mounted {
                    if let Err(rollback_error) = self.watcher.unwatch(&prior) {
                        tracing::warn!(dir = %prior.display(), error = %rollback_error, "yml: 回退新增配置目录监听失败，保留额外观察并等待下次对账");
                        retained.push(prior);
                    }
                }
                // 回退失败的目录仍被底层观察，记入集合可避免后续重复挂载；旧目标过滤
                // 会阻止这些额外事件穿过，不会在新增目录失败时提前切换配置来源。
                self.watched_dirs.extend(retained);
                return Err(error).with_context(|| {
                    format!("yml: 监听新增配置目录 {} 失败", directory.display())
                });
            }
            mounted.push(directory.clone());
        }

        // 只有新增目录全部可观察后才一次发布目标集合，回调不会读到半更新分类。
        *self
            .targets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next_targets;
        self.watched_dirs.extend(additions);

        // 新目标已经生效，撤销旧目录即使失败也只会形成被精确过滤的额外观察。
        for directory in self
            .watched_dirs
            .difference(&next_dirs)
            .cloned()
            .collect::<Vec<_>>()
        {
            match self.watcher.unwatch(&directory) {
                Ok(()) => {
                    self.watched_dirs.remove(&directory);
                }
                Err(error) => {
                    tracing::warn!(dir = %directory.display(), error = %error, "yml: 撤销旧配置目录监听失败，保留额外观察并等待下次对账");
                }
            }
        }
        Ok(())
    }
}

/// 业务作用：递归展开路径中的符号链接，收集链接节点、重定向后的完整路径和最终真实路径。
///
/// 参数说明：`path` 是本轮待展开路径；`aliases` 收集身份；`visited` 阻断链接环；`depth` 限制异常链。
///
/// 返回：无返回值；无法读取或超过链接深度时保留已经收集的安全身份。
fn collect_watch_aliases(
    path: &Path,
    aliases: &mut HashSet<PathBuf>,
    visited: &mut HashSet<PathBuf>,
    depth: usize,
) {
    const MAX_SYMLINK_DEPTH: usize = 40;
    let path = absolute_watch_path(path);
    aliases.insert(path.clone());
    if depth >= MAX_SYMLINK_DEPTH || !visited.insert(path.clone()) {
        return;
    }

    let components = path
        .components()
        .map(|component| component.as_os_str().to_os_string())
        .collect::<Vec<_>>();
    let mut prefix = PathBuf::new();
    for (index, component) in components.iter().enumerate() {
        prefix.push(component);
        let Ok(metadata) = std::fs::symlink_metadata(&prefix) else {
            continue;
        };
        if !metadata.file_type().is_symlink() {
            continue;
        }
        aliases.insert(prefix.clone());
        // 当前链接节点之前没有尚未展开的符号链接，因此此处折叠 `..` 不会改变内核路径语义；
        // 同时保留归一身份可匹配 notify 返回的不含父级分量的链接换代路径。
        aliases.insert(lexically_normalize_watch_path(&prefix));
        let Ok(target) = std::fs::read_link(&prefix) else {
            return;
        };
        let mut redirected = if target.is_absolute() {
            target
        } else {
            prefix
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(target)
        };
        for suffix in components.iter().skip(index + 1) {
            redirected.push(suffix);
        }
        collect_watch_aliases(&redirected, aliases, visited, depth + 1);
        return;
    }
    // 扫描至末尾仍未遇到符号链接后，`..` 才能安全按词法折叠；提前折叠会让
    // `link/../application.yml` 偏离 loader 交由内核解析的真实来源。
    aliases.insert(lexically_normalize_watch_path(&path));
    if let Ok(canonical) = std::fs::canonicalize(&path) {
        aliases.insert(canonical);
    }
}

/// 业务作用：把声明路径转换成绝对路径，同时保留影响符号链接父级语义的 `..` 分量。
///
/// 参数说明：`path` 是绝对或相对配置路径。
///
/// 返回：绝对路径；相对路径以当前工作目录为基准，原有路径分量保持不变。
fn absolute_watch_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

/// 业务作用：在路径已确认不含未展开符号链接后消除词法冗余，匹配操作系统事件的标准路径形态。
///
/// 参数说明：`path` 是已经完成符号链接感知扫描的绝对路径或链接节点前缀。
///
/// 返回：移除 `.` 并折叠可消解 `..` 后的路径，不访问文件系统。
fn lexically_normalize_watch_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}
