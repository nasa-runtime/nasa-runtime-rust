use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::Write,
    path::{Path, PathBuf},
};

use super::{
    bind, expand_pattern, read_source, ConfigError, ConfigPath, EnvironmentSnapshot, ErrorKind,
    FileEvidence, FilePattern, LoadPolicy, ParsedDocument, PathSegment, ProfileSelection, Result,
    SourceDocument,
};
use crate::YmlImport;

/// 已完成读取与解析的来源记录，显式访问名称与摘要需要调用方承担保密责任。
#[derive(Clone)]
pub struct SourceRecord {
    /// 本轮来源按装配顺序分配的编号。
    pub id: usize,
    /// 来源原始正文的字节数。
    pub bytes: usize,
    /// 来源在装配计划中的职责。
    pub kind: SourceKind,
    /// 导入来源所属的声明下标；主文档和 profile 不设置。
    pub import_index: Option<usize>,
    /// 文件导入在自然排序后的匹配下标；显式单文件为零，非文件来源不设置。
    pub match_index: Option<usize>,
    name: String,
    digest: [u8; 32],
}

/// 本轮来源的职责，名称和正文另由调用方受控访问。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceKind {
    /// 显式加载的主配置文档。
    Main,
    /// 按固定 profile 选择的覆盖文档。
    Profile,
    /// 文件导入声明取得的文档。
    File,
    /// 由远端提供器取得并交给本地装配的文档。
    Remote,
    /// 调用方直接提供的内存文档。
    Memory,
}

/// 本轮来源与下一轮新增/删除观察所需的完整事实。
#[derive(Clone, Default)]
pub struct WatchPlan {
    /// 已读取文件的身份与内容证据。
    pub files: Vec<FileEvidence>,
    /// 宿主补充的外部文件依赖，仅用于观察变化。
    pub dependencies: Vec<PathBuf>,
    /// 本轮明确缺失的可选文件，后续出现时需要重建候选。
    pub missing_files: Vec<PathBuf>,
    /// profile 格式选择涉及的候选路径，用于发现新增或删除。
    pub profile_candidates: Vec<PathBuf>,
    /// 模式、可选标记及本轮有序匹配集合，用于重新枚举和观察。
    pub patterns: Vec<(FilePattern, bool, Vec<PathBuf>)>,
}

/// 完整候选独立于运行态，绑定成功也不会自动发布业务配置。
pub struct LoadedConfig {
    tree: Value,
    /// 按装配顺序记录的完整来源清单。
    pub sources: Vec<SourceRecord>,
    /// 候选复验和后续观察所需的来源事实。
    pub watch: WatchPlan,
    /// 字段在文档合并过程中对应的有序来源编号。
    pub origins: BTreeMap<ConfigPath, Vec<usize>>,
    /// 表达式实际访问的配置字段依赖。
    pub dependencies: BTreeMap<ConfigPath, BTreeSet<ConfigPath>>,
    /// 由环境结构覆盖写入的字段路径。
    pub environment_paths: BTreeSet<ConfigPath>,
    /// 环境结构覆盖的字段与原始环境键名，不包含环境原值。
    pub environment_origins: BTreeMap<ConfigPath, String>,
    /// 表达式求值时实际访问的环境键名。
    pub environment_dependencies: BTreeMap<ConfigPath, BTreeSet<String>>,
    fingerprint: [u8; 32],
}

/// 显式严格加载入口，环境、工作目录和策略在创建后固定。
#[derive(Clone)]
pub struct ConfigLoader {
    environment: EnvironmentSnapshot,
    policy: LoadPolicy,
    base: Option<PathBuf>,
    profile_pattern: Option<String>,
    profile_env: String,
    profile: ProfileSelection,
    env_prefix: String,
    env_separator: String,
    local_imports: bool,
}

/// 引导阶段已经读取的文档及来源计划，后续装配不重新打开主文件/profile。
pub struct PreparedLoad {
    loader: ConfigLoader,
    merged: Merged,
    bootstrap: Value,
    bootstrap_environment: BTreeSet<ConfigPath>,
    imports: Vec<YmlImport>,
    watch: WatchPlan,
}

struct Merged {
    tree: Value,
    records: Vec<SourceRecord>,
    origins: BTreeMap<ConfigPath, Vec<usize>>,
    bytes: usize,
    parsed_nodes: usize,
    parsed_text: usize,
}

impl ConfigLoader {
    /// 业务作用：创建完全不读取文件和真实环境的内存加载器。
    /// 参数说明：`environment` 是调用方固定的快照。
    /// 返回：尚未指定本地文件的严格入口。
    pub fn memory(environment: EnvironmentSnapshot) -> Self {
        Self {
            environment,
            policy: LoadPolicy::default(),
            base: None,
            profile_pattern: None,
            profile_env: "APP_PROFILE".into(),
            profile: ProfileSelection::Disabled,
            env_prefix: "APP".into(),
            env_separator: "__".into(),
            local_imports: true,
        }
    }

    /// 业务作用：固定标准主路径、profile 模板、环境及工作目录。
    /// 参数说明：无。
    /// 返回：标准严格加载器；无法取得工作目录或环境时失败。
    pub fn standard() -> Result<Self> {
        let directory =
            std::env::current_dir().map_err(|_| ConfigError::new(ErrorKind::Unreadable))?;
        let mut loader = Self::memory(EnvironmentSnapshot::capture()?);
        loader.base = Some(directory.join("zcf/application.yml"));
        loader.profile_pattern = Some(
            directory
                .join("zcf/application-{profile}")
                .to_str()
                .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))?
                .into(),
        );
        loader.profile = ProfileSelection::Environment;
        Ok(loader)
    }

    /// 业务作用：在创建阶段固定文件的绝对位置。
    /// 参数说明：`path` 为主文档路径。
    /// 返回：相对路径已绑定当前目录的加载器。
    pub fn base_file(mut self, path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        self.base = Some(absolute(&path)?);
        Ok(self)
    }

    /// 业务作用：明确 profile 的目录模板，避免跟随进程工作目录变动。
    /// 参数说明：`pattern` 可包含一个 profile 名占位符。
    /// 返回：模板固定的加载器。
    pub fn profile_pattern(mut self, pattern: impl Into<PathBuf>) -> Result<Self> {
        self.profile_pattern = Some(
            absolute(&pattern.into())?
                .to_str()
                .ok_or_else(|| ConfigError::new(ErrorKind::Encoding))?
                .into(),
        );
        Ok(self)
    }

    /// 业务作用：显式选择或关闭 profile，优先于环境选择。
    /// 参数说明：`profile` 是加载器级选择策略。
    /// 返回：选择已固定的新加载器。
    pub fn profile(mut self, profile: ProfileSelection) -> Self {
        self.profile = profile;
        self
    }

    /// 业务作用：设置 profile 的原样环境名称。
    /// 参数说明：`name` 是快照中的确切键。
    /// 返回：使用指定名称的加载器。
    pub fn profile_env(mut self, name: impl Into<String>) -> Self {
        self.profile_env = name.into();
        self
    }

    /// 业务作用：明确环境结构覆盖的命名规则。
    /// 参数说明：`prefix` 是前缀；`separator` 是层级分隔符，空串表示单下划线前缀与平面键。
    /// 返回：命名规则固定的加载器。
    pub fn environment_mapping(
        mut self,
        prefix: impl Into<String>,
        separator: impl Into<String>,
    ) -> Self {
        self.env_prefix = prefix.into();
        self.env_separator = separator.into();
        self
    }

    /// 业务作用：应用受信的预算、字段和来源策略。
    /// 参数说明：`policy` 由调用方构造，不接受导入文档修改。
    /// 返回：策略固定的新加载器。
    pub fn policy(mut self, policy: LoadPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// 业务作用：控制文件入口是否展开本地 import 声明。
    /// 参数说明：`enabled` 为显式启用状态。
    /// 返回：改变装配范围的加载器；内存入口始终不自行读文件。
    pub fn local_imports(mut self, enabled: bool) -> Self {
        self.local_imports = enabled;
        self
    }

    /// 业务作用：仅装配调用方已经取得的文档，环境最后覆盖。
    /// 参数说明：`documents` 的顺序就是来源优先级。
    /// 返回：完整可绑定的候选，不会因文档名称打开文件。
    pub fn load_documents(&self, documents: &[SourceDocument]) -> Result<LoadedConfig> {
        let mut merged = Merged::new();
        if documents.is_empty() {
            return Err(ConfigError::new(ErrorKind::Missing));
        }
        for document in documents {
            merged.add(document, &self.policy)?;
        }
        self.finish(merged, WatchPlan::default())
    }

    /// 业务作用：完成本地来源装配和解析，不启动业务资源。
    /// 参数说明：无。
    /// 返回：完整配置；远端声明存在但未提供文档时按必需性拒绝。
    pub fn load(&self) -> Result<LoadedConfig> {
        self.prepare()?.finish(&BTreeMap::new())
    }

    /// 业务作用：读取主文件/profile 并只求值来源计划的依赖闭包。
    /// 参数说明：无。
    /// 返回：可以交给配置中心适配器补充远端文档的固定计划。
    pub fn prepare(&self) -> Result<PreparedLoad> {
        let base = self
            .base
            .as_ref()
            .ok_or_else(|| ConfigError::new(ErrorKind::Declaration))?;
        let mut merged = Merged::new();
        let mut watch = WatchPlan::default();
        let source = read_source(base, false, &self.policy)?
            .ok_or_else(|| ConfigError::new(ErrorKind::Missing))?;
        merged.add(&source.document, &self.policy)?;
        merged
            .records
            .last_mut()
            .expect("main source recorded")
            .kind = SourceKind::Main;
        watch.files.push(source.evidence);
        if let Some(profile) = self.selected_profile()? {
            let template = self
                .profile_pattern
                .as_ref()
                .ok_or_else(|| ConfigError::new(ErrorKind::Declaration))?;
            let path = PathBuf::from(template.replace("{profile}", &profile));
            let mut candidates = vec![path.clone()];
            if path.extension().is_none() {
                for extension in ["toml", "json", "yaml", "yml"] {
                    candidates.push(path.with_extension(extension));
                }
            }
            watch.profile_candidates = candidates.clone();
            let mut available = Vec::new();
            for candidate in candidates {
                match std::fs::symlink_metadata(&candidate) {
                    Ok(_) => {
                        let source = read_source(&candidate, false, &self.policy)?
                            .ok_or_else(|| ConfigError::new(ErrorKind::Missing))?;
                        available.push(source);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        watch.missing_files.push(candidate);
                    }
                    Err(_) => return Err(ConfigError::new(ErrorKind::Unreadable)),
                }
            }
            if available.is_empty() && self.policy.strict_profile {
                return Err(ConfigError::new(ErrorKind::Missing));
            }
            if available.len() > 1 && self.policy.reject_ambiguous_profile {
                return Err(ConfigError::new(ErrorKind::Duplicate));
            }
            if let Some(source) = available.into_iter().next() {
                reject_duplicate(&watch.files, &source.evidence)?;
                merged.add(&source.document, &self.policy)?;
                merged
                    .records
                    .last_mut()
                    .expect("profile source recorded")
                    .kind = SourceKind::Profile;
                watch.files.push(source.evidence);
            }
        }
        let mut bootstrap = merged.tree.clone();
        let bootstrap_environment = apply_environment(&mut bootstrap, self)?
            .into_keys()
            .collect();
        let imports = if self.local_imports && bootstrap.get("yml").is_some() {
            if bootstrap
                .pointer("/yml/imports")
                .and_then(Value::as_array)
                .is_some_and(|list| list.len() > self.policy.limits.declarations)
            {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
            let path = ConfigPath::default().key("yml");
            let selected = super::expression::resolve_selected_overlaid(
                &bootstrap,
                std::slice::from_ref(&path),
                &self.environment,
                &self.policy,
                &bootstrap_environment,
            )?;
            let tree = serde_json::json!({"yml": selected[&path]});
            super::imports::parse_imports_limited(
                &tree,
                base.parent().unwrap_or(Path::new(".")),
                self.policy.limits.declarations,
            )?
        } else {
            Vec::new()
        };
        validate_import_limits(&imports, &self.policy)?;
        Ok(PreparedLoad {
            loader: self.clone(),
            merged,
            bootstrap,
            bootstrap_environment,
            imports,
            watch,
        })
    }

    /// 业务作用：供受限读取适配器复用创建时固定的主文件身份。
    /// 参数说明：无。
    /// 返回：绝对主路径，纯内存入口没有路径。
    pub fn base_path(&self) -> Option<&Path> {
        self.base.as_deref()
    }

    /// 业务作用：提供固定快照供宿主分阶段配置与业务隔离读取共享。
    /// 参数说明：无。
    /// 返回：只读环境快照，不重新读取进程环境。
    pub fn environment(&self) -> &EnvironmentSnapshot {
        &self.environment
    }

    /// 业务作用：让宿主为实际来源建立同策略的观察器。
    /// 参数说明：无。
    /// 返回：不可由配置文件改变的加载约束。
    pub fn load_policy(&self) -> &LoadPolicy {
        &self.policy
    }

    /// 业务作用：在解析完成前保持环境优先级，并计算有界的有效值身份。
    /// 参数说明：`merged` 为来源合并结果；`watch` 为读取事实。
    /// 返回：解析和结果预算均通过的候选。
    fn finish(&self, mut merged: Merged, watch: WatchPlan) -> Result<LoadedConfig> {
        let environment_origins = apply_environment(&mut merged.tree, self)?;
        let environment_paths = environment_origins.keys().cloned().collect();
        let resolution = super::expression::resolve_overlaid(
            &merged.tree,
            &self.environment,
            &self.policy,
            &environment_paths,
        )
        .map_err(|mut error| {
            if let Some(path) = &error.path {
                if !environment_paths.iter().any(|root| path.starts_with(root)) {
                    error.source_id = merged
                        .origins
                        .get(path)
                        .and_then(|origins| origins.last())
                        .copied();
                }
            }
            error
        })?;
        let fingerprint = tree_fingerprint(&resolution.tree, self.policy.limits.output_bytes)?;
        Ok(LoadedConfig {
            tree: resolution.tree,
            sources: merged.records,
            watch,
            origins: merged.origins,
            dependencies: resolution.dependencies,
            environment_dependencies: resolution.environment,
            environment_paths,
            environment_origins,
            fingerprint,
        })
    }

    /// 业务作用：从固定快照或显式设置选择单个安全 profile 名。
    /// 参数说明：无。
    /// 返回：名称或关闭状态，路径型名称明确拒绝。
    pub fn selected_profile(&self) -> Result<Option<String>> {
        let profile = match &self.profile {
            ProfileSelection::Disabled => None,
            ProfileSelection::Named(name) => Some(name.as_str()),
            ProfileSelection::Environment => self.environment.get(&self.profile_env)?,
        }
        .map(str::trim)
        .filter(|value| !value.is_empty());
        if let Some(name) = profile {
            if name.len() > 128
                || name.contains(['/', '\\'])
                || name.contains("..")
                || name.chars().any(char::is_control)
            {
                return Err(ConfigError::new(ErrorKind::Declaration));
            }
        }
        Ok(profile.map(str::to_string))
    }
}

impl PreparedLoad {
    /// 业务作用：给上层提供已校验的有序来源声明。
    /// 参数说明：无。
    /// 返回：不包含实际远端连接动作的描述列表。
    pub fn imports(&self) -> &[YmlImport] {
        &self.imports
    }

    /// 业务作用：让配置中心门面保留 nacos.imports 在前的既有合同。
    /// 参数说明：`imports` 为调用方已校验的前置声明。
    /// 返回：总声明预算内的固定计划。
    pub fn prepend_imports(mut self, mut imports: Vec<YmlImport>) -> Result<Self> {
        imports.append(&mut self.imports);
        validate_import_limits(&imports, &self.loader.policy)?;
        self.imports = imports;
        Ok(self)
    }

    /// 业务作用：查询引导字段是否声明，避免把缺省段误判为求值失败。
    /// 参数说明：`path` 是结构化字段位置。
    /// 返回：仅表示存在性，不输出尚未解析的业务值。
    pub fn contains(&self, path: &ConfigPath) -> bool {
        path.get(&self.bootstrap).is_some()
    }

    /// 业务作用：让宿主枚举已声明能力名称而不提前求值其连接材料。
    /// 参数说明：`path` 是引导映射的位置。
    /// 返回：现有子字段路径；缺失映射为空，非映射声明拒绝。
    pub fn children(&self, path: &ConfigPath) -> Result<Vec<ConfigPath>> {
        match path.get(&self.bootstrap) {
            None => Ok(Vec::new()),
            Some(Value::Object(map)) => Ok(map.keys().map(|key| path.key(key)).collect()),
            Some(_) => Err(ConfigError::new(ErrorKind::Declaration).at(path)),
        }
    }

    /// 业务作用：让宿主为已确认的引导字段设置精确类型或求值规则。
    /// 参数说明：`path` 是字段路径；`hint` 是受信宿主选择的规则。
    /// 返回：采用新规则的计划，来源声明和环境快照保持冻结。
    pub fn with_hint(mut self, path: ConfigPath, hint: super::ValueHint) -> Self {
        self.loader.policy.hints.insert(path, hint);
        self
    }

    /// 业务作用：为明确禁用的可选能力保留原始文本，不访问其未启用的连接材料。
    /// 参数说明：`path` 是宿主已确认不参与本轮初始化的子树。
    /// 返回：该路径使用字面量策略的固定计划；不会改变来源读取权限。
    pub fn protect(mut self, path: ConfigPath) -> Self {
        self.loader
            .policy
            .hints
            .insert(path, super::ValueHint::Literal);
        self
    }

    /// 业务作用：只向启动方提供已经求值的必要引导字段。
    /// 参数说明：`paths` 是启动身份、连接和信任设置的字段路径。
    /// 返回：选定字段的结果，不提前求值其它业务表达式。
    pub fn bootstrap(&self, paths: &[ConfigPath]) -> Result<BTreeMap<ConfigPath, Value>> {
        super::expression::resolve_selected_overlaid(
            &self.bootstrap,
            paths,
            &self.loader.environment,
            &self.loader.policy,
            &self.bootstrap_environment,
        )
    }

    /// 业务作用：按固定声明位置装配本地和调用方取得的远端文档。
    /// 参数说明：`remote` 以 import 条目索引标识文档，未声明项不能注入。
    /// 返回：所有必需来源齐全且身份复验通过的候选。
    pub fn finish(mut self, remote: &BTreeMap<usize, SourceDocument>) -> Result<LoadedConfig> {
        for index in remote.keys() {
            if !matches!(self.imports.get(*index), Some(YmlImport::Nacos(_))) {
                return Err(ConfigError::new(ErrorKind::Declaration));
            }
        }
        let mut remote_identities = BTreeSet::new();
        for (index, import) in self.imports.iter().enumerate() {
            match import {
                YmlImport::File { path, optional } => {
                    let pattern = FilePattern::new(path)?;
                    let files = expand_pattern(&pattern, *optional, &self.loader.policy)?;
                    if pattern.is_glob() {
                        self.watch
                            .patterns
                            .push((pattern.clone(), *optional, files.clone()));
                    }
                    for (match_index, file) in files.into_iter().enumerate() {
                        let source = read_source(
                            &file,
                            *optional && !pattern.is_glob(),
                            &self.loader.policy,
                        )?;
                        if let Some(source) = source {
                            reject_duplicate(&self.watch.files, &source.evidence)?;
                            self.merged
                                .add_import(&source.document, &self.loader.policy)?;
                            let record = self
                                .merged
                                .records
                                .last_mut()
                                .expect("file source recorded");
                            record.kind = SourceKind::File;
                            record.import_index = Some(index);
                            record.match_index = Some(match_index);
                            self.watch.files.push(source.evidence);
                        } else {
                            self.watch.missing_files.push(file);
                        }
                    }
                }
                YmlImport::Nacos(import) => {
                    let identity = (import.group.clone(), import.data_id.clone());
                    if !remote_identities.insert(identity) {
                        return Err(ConfigError::new(ErrorKind::Duplicate));
                    }
                    match remote.get(&index) {
                        Some(document) => {
                            if let Some(format) = &import.file_extension {
                                if crate::ConfigFormat::from_extension(format)
                                    .map_err(|_| ConfigError::new(ErrorKind::Unsupported))?
                                    != document.format
                                {
                                    return Err(ConfigError::new(ErrorKind::Declaration));
                                }
                            }
                            self.merged.add_import(document, &self.loader.policy)?;
                            let record = self
                                .merged
                                .records
                                .last_mut()
                                .expect("remote source recorded");
                            record.kind = SourceKind::Remote;
                            record.import_index = Some(index);
                        }
                        None if import.optional => {}
                        None => return Err(ConfigError::new(ErrorKind::Missing)),
                    }
                }
            }
        }
        let loaded = self.loader.finish(self.merged, self.watch)?;
        loaded.watch.revalidate(&self.loader.policy)?;
        Ok(loaded)
    }
}

impl WatchPlan {
    /// 业务作用：为有序文件身份、缺失候选及模式生成独立观察身份。
    /// 参数说明：无。
    /// 返回：内部对账摘要；即使值相同，来源替换也会改变此身份。
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for file in &self.files {
            hash.update([0]);
            hash.update(file.observation_fingerprint());
        }
        for (kind, paths) in [
            (1u8, &self.missing_files),
            (2, &self.profile_candidates),
            (3, &self.dependencies),
        ] {
            for path in paths {
                hash.update([kind]);
                fingerprint_path(&mut hash, path);
            }
        }
        for (pattern, optional, files) in &self.patterns {
            hash.update([4, u8::from(*optional)]);
            fingerprint_path(&mut hash, &pattern.path());
            hash.update((files.len() as u64).to_be_bytes());
            for path in files {
                fingerprint_path(&mut hash, path);
            }
        }
        hash.finalize().into()
    }

    /// 业务作用：已知来源或模式集合变化时拒绝旧候选。
    /// 参数说明：`policy` 是原读取策略。
    /// 返回：读取证据与重新枚举的集合均一致时成功。
    pub fn revalidate(&self, policy: &LoadPolicy) -> Result<()> {
        for source in &self.files {
            source.revalidate(policy)?;
        }
        for path in &self.missing_files {
            if read_source(path, true, policy)?.is_some() {
                return Err(ConfigError::new(ErrorKind::SourceChanged));
            }
        }
        for (pattern, optional, previous) in &self.patterns {
            if &expand_pattern(pattern, *optional, policy)? != previous {
                return Err(ConfigError::new(ErrorKind::SourceChanged));
            }
        }
        Ok(())
    }
}

/// 只读装配报告只给出规模，不打印路径、值、环境名或敏感摘要。
#[derive(Clone, Debug)]
pub struct CheckReport {
    /// 实际装配的文档数量。
    pub sources: usize,
    /// 来源正文的累计字节数。
    pub input_bytes: usize,
    /// 保留文档来源记录的字段路径数量。
    pub fields: usize,
    /// 环境结构覆盖写入的字段数量。
    pub environment_overrides: usize,
    /// 表达式字段依赖集合的累计边数。
    pub dependency_edges: usize,
    /// 观察计划中保留读取证据的文件数量，包含主文件和 profile。
    pub matched_files: usize,
    /// 观察计划中的文件模式数量。
    pub patterns: usize,
}

impl LoadedConfig {
    /// 业务作用：返回不触发业务发布或外部副作用的装配诊断。
    /// 参数说明：无。
    /// 返回：完整候选的安全规模报告，不能替代业务校验和组件应用状态。
    pub fn report(&self) -> CheckReport {
        CheckReport {
            sources: self.sources.len(),
            input_bytes: self.sources.iter().map(|source| source.bytes).sum(),
            fields: self.origins.len(),
            environment_overrides: self.environment_paths.len(),
            dependency_edges: self.dependencies.values().map(BTreeSet::len).sum(),
            matched_files: self.watch.files.len(),
            patterns: self.watch.patterns.len(),
        }
    }

    /// 业务作用：显式借用含原值的候选，调用方负责材料边界。
    /// 参数说明：无。
    /// 返回：已求值树，不表示业务组件已经应用。
    pub fn tree(&self) -> &Value {
        &self.tree
    }
    /// 业务作用：将完整树所有权交给宿主的候选发布流程。
    /// 参数说明：无。
    /// 返回：消费来源结果后的配置树。
    pub fn into_tree(self) -> Value {
        self.tree
    }
    /// 业务作用：按业务结构绑定整个候选。
    /// 参数说明：无。
    /// 返回：通过类型与自定义 Serde 校验的配置。
    pub fn bind<T: DeserializeOwned>(&self) -> Result<T> {
        bind(self.tree.clone())
    }
    /// 业务作用：让分段初始化的组件复用相同文本绑定规则。
    /// 参数说明：`path` 是组件拥有的子树。
    /// 返回：子树目标配置；缺失或类型错误被拒绝。
    pub fn bind_at<T: DeserializeOwned>(&self, path: &ConfigPath) -> Result<T> {
        let value = path
            .get(&self.tree)
            .ok_or_else(|| ConfigError::new(ErrorKind::Missing).at(path))?;
        bind(value.clone()).map_err(|mut error| {
            let mut full = path.clone();
            if let Some(child) = error.path {
                full.0.extend(child.0);
            }
            error.path = Some(full);
            error
        })
    }
    /// 业务作用：取得值身份供宿主去重，来源观察仍须独立对账。
    /// 参数说明：无。
    /// 返回：候选摘要材料，不能作为公开秘密指纹输出。
    pub fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
    /// 业务作用：把有序来源事实与观察目标合成独立身份，不混用有效值身份。
    /// 参数说明：无。
    /// 返回：包含内存及远端文档身份的来源摘要，仅供受控比较。
    pub fn source_fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(self.watch.fingerprint());
        for source in &self.sources {
            hash.update((source.name.len() as u64).to_le_bytes());
            hash.update(source.name.as_bytes());
            hash.update(source.digest);
            hash.update([source.kind as u8]);
            for index in [source.import_index, source.match_index] {
                hash.update(index.map_or(u64::MAX, |index| index as u64).to_le_bytes());
            }
        }
        hash.finalize().into()
    }
    /// 业务作用：执行只读业务校验，不安装资源。
    /// 参数说明：`validate` 为纯校验函数。
    /// 返回：全部业务约束成立时成功，错误由调用方使用安全字段位置表达。
    pub fn validate(&self, validate: impl FnOnce(&Value) -> Result<()>) -> Result<()> {
        validate(&self.tree)
    }
}

impl SourceRecord {
    /// 业务作用：显式获取来源名称以供受控管理界面使用。
    /// 参数说明：无。
    /// 返回：原始身份名称，不适合默认日志输出。
    pub fn name(&self) -> &str {
        &self.name
    }
    /// 业务作用：为调用方保留同一文档字节的内容身份。
    /// 参数说明：无。
    /// 返回：源摘要，仅供内部比较。
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

impl Merged {
    /// 业务作用：为一轮装配创建空基线，不继承上一轮已删除来源的值。
    /// 参数说明：无。
    /// 返回：没有任何来源贡献的树。
    fn new() -> Self {
        Self {
            tree: Value::Object(Map::new()),
            records: Vec::new(),
            origins: BTreeMap::new(),
            bytes: 0,
            parsed_nodes: 0,
            parsed_text: 0,
        }
    }
    /// 业务作用：检查本轮共享预算后解析和合并文档。
    /// 参数说明：`document` 是取得的正文；`policy` 是本轮约束。
    /// 返回：完整合并成功，不重复解析同一文档。
    fn add(&mut self, document: &SourceDocument, policy: &LoadPolicy) -> Result<()> {
        let parsed = self.parse(document, policy)?;
        self.add_parsed(document, parsed);
        Ok(())
    }
    /// 业务作用：导入文档不能重新声明控制自身加载的权限。
    /// 参数说明：`document` 是外部导入；`policy` 是固定策略。
    /// 返回：仅业务字段进入来源合并。
    fn add_import(&mut self, document: &SourceDocument, policy: &LoadPolicy) -> Result<()> {
        let parsed = self.parse(document, policy)?;
        if parsed.value.get("yml").is_some()
            || parsed.value.get("nacos").is_some()
            || parsed.value.get("secret_providers").is_some()
        {
            // 来源声明的权限检查先于合并，不能被后面的覆盖层掩盖。
            return Err(ConfigError::new(ErrorKind::PolicyChanged).in_source(parsed.source_id));
        }
        self.add_parsed(document, parsed);
        Ok(())
    }
    /// 业务作用：让所有文件和内存来源共同消耗数量及字节预算。
    /// 参数说明：`document` 为下一来源；`policy` 是共享限制。
    /// 返回：通过预算和严格解析的文档。
    fn parse(&mut self, document: &SourceDocument, policy: &LoadPolicy) -> Result<ParsedDocument> {
        self.bytes = self
            .bytes
            .checked_add(document.bytes.len())
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        if self.bytes > policy.limits.total_bytes || self.records.len() >= policy.limits.sources {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        let parsed = document.parse(self.records.len(), policy)?;
        let mut nodes = vec![parsed.tree()];
        while let Some(value) = nodes.pop() {
            self.parsed_nodes = self.parsed_nodes.saturating_add(1);
            match value {
                Value::Object(map) => {
                    self.parsed_text = self
                        .parsed_text
                        .saturating_add(map.keys().map(String::len).sum::<usize>());
                    nodes.extend(map.values());
                }
                Value::Array(list) => nodes.extend(list),
                Value::String(text) => {
                    self.parsed_text = self.parsed_text.saturating_add(text.len())
                }
                _ => {}
            }
            if self.parsed_nodes > policy.limits.nodes
                || self.parsed_text > policy.limits.output_bytes
            {
                return Err(ConfigError::new(ErrorKind::Limit).in_source(parsed.source_id));
            }
        }
        Ok(parsed)
    }
    /// 业务作用：合并已验证文档并保留字段覆盖来源。
    /// 参数说明：`document` 提供名称；`parsed` 提供唯一解析结果。
    /// 返回：原地更新私有候选，不影响运行态。
    fn add_parsed(&mut self, document: &SourceDocument, parsed: ParsedDocument) {
        merge(
            &mut self.tree,
            parsed.value,
            &ConfigPath::default(),
            parsed.source_id,
            &mut self.origins,
        );
        self.records.push(SourceRecord {
            id: parsed.source_id,
            kind: SourceKind::Memory,
            import_index: None,
            match_index: None,
            name: document.name.clone(),
            digest: parsed.digest,
            bytes: parsed.bytes,
        });
    }
}

/// 业务作用：保持映射深合并、数组整体替换和显式 null 的语义。
/// 参数说明：`target/value` 是两层节点；`path/source/origins` 记录覆盖关系。
/// 返回：更新尚未发布的候选树。
fn merge(
    target: &mut Value,
    value: Value,
    path: &ConfigPath,
    source: usize,
    origins: &mut BTreeMap<ConfigPath, Vec<usize>>,
) {
    origins.entry(path.clone()).or_default().push(source);
    match (target, value) {
        (Value::Object(target), Value::Object(values)) => {
            for (key, value) in values {
                merge(
                    target.entry(key.clone()).or_insert(Value::Null),
                    value,
                    &path.key(&key),
                    source,
                    origins,
                );
            }
        }
        (target, value) => {
            origins.retain(|child, _| child == path || !child.starts_with(path));
            record_children(&value, path, source, origins);
            *target = value;
        }
    }
}

/// 业务作用：节点整体替换后为所有新子节点建立来源归属。
/// 参数说明：`value/path/source/origins` 指定新子树与来源记录。
/// 返回：补充数组或映射内部的字段来源。
fn record_children(
    value: &Value,
    path: &ConfigPath,
    source: usize,
    origins: &mut BTreeMap<ConfigPath, Vec<usize>>,
) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let child = path.key(key);
                origins.insert(child.clone(), vec![source]);
                record_children(value, &child, source, origins);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                let child = path.index(index);
                origins.insert(child.clone(), vec![source]);
                record_children(value, &child, source, origins);
            }
        }
        _ => {}
    }
}

/// 业务作用：固定相对路径的工作目录，不在热加载时重新解释。
/// 参数说明：`path` 是调用方提供路径。
/// 返回：绝对路径或当前目录错误。
fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.into())
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| ConfigError::new(ErrorKind::Unreadable))?
            .join(path))
    }
}

/// 业务作用：零匹配条目也计入来源规划上限。
/// 参数说明：`imports` 是完整声明；`policy` 为声明预算。
/// 返回：模式和声明数量有限时成功。
fn validate_import_limits(imports: &[YmlImport], policy: &LoadPolicy) -> Result<()> {
    let patterns = imports.iter().filter(|entry| matches!(entry, YmlImport::File {path, ..} if path.to_string_lossy().contains(['*', '?']))).count();
    if imports.len() > policy.limits.declarations || patterns > policy.limits.patterns {
        Err(ConfigError::new(ErrorKind::Limit))
    } else {
        Ok(())
    }
}

/// 业务作用：不同声明不能重复读取同一真实配置文件。
/// 参数说明：`prior` 是已有证据；`next` 是新来源。
/// 返回：身份唯一时成功。
fn reject_duplicate(prior: &[FileEvidence], next: &FileEvidence) -> Result<()> {
    if prior.iter().any(|old| old.same_file(next)) {
        Err(ConfigError::new(ErrorKind::Duplicate))
    } else {
        Ok(())
    }
}

/// 业务作用：统一环境层优先级并拒绝同层路径碰撞。
/// 参数说明：`tree` 为环境覆盖前树；`loader` 固定快照和命名规则。
/// 返回：本轮被环境赋值的路径与确切变量名，名称只供受控解释。
fn apply_environment(
    tree: &mut Value,
    loader: &ConfigLoader,
) -> Result<BTreeMap<ConfigPath, String>> {
    if !loader.policy.environment_overlay {
        return Ok(BTreeMap::new());
    }
    let separator = if loader.env_separator.is_empty() {
        "_"
    } else {
        &loader.env_separator
    };
    let prefix = format!("{}{separator}", loader.env_prefix);
    let mut entries = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for (name, text) in loader.environment.overlay(&prefix)? {
        let original_name = name;
        let name = name.to_ascii_lowercase();
        let suffix = &name[prefix.len()..];
        if suffix.is_empty() {
            return Err(ConfigError::new(ErrorKind::Declaration));
        }
        let segments: Vec<&str> = if loader.env_separator.is_empty() {
            vec![suffix]
        } else {
            suffix.split(&loader.env_separator).collect()
        };
        let mut path = ConfigPath::default();
        for segment in segments {
            if segment.is_empty() {
                return Err(ConfigError::new(ErrorKind::PathConflict));
            }
            if loader.policy.structured_environment
                && segment.bytes().all(|byte| byte.is_ascii_digit())
                && path.get(tree).is_some_and(Value::is_array)
            {
                path.0.push(PathSegment::Index(
                    segment
                        .parse()
                        .map_err(|_| ConfigError::new(ErrorKind::PathConflict))?,
                ));
            } else {
                path.0.push(PathSegment::Key(segment.into()));
            }
            if path.0.len() > loader.policy.limits.depth || suffix.len() > 4096 {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
        }
        if entries
            .keys()
            .any(|prior: &ConfigPath| prior.starts_with(&path) || path.starts_with(prior))
        {
            return Err(ConfigError::new(ErrorKind::PathConflict).at(&path));
        }
        let value =
            if loader.policy.structured_environment && text.trim_start().starts_with(['[', '{']) {
                let document = SourceDocument::new(
                    "environment",
                    crate::ConfigFormat::Json,
                    format!("{{\"value\":{text}}}"),
                );
                document.parse(0, &loader.policy)?.value["value"].clone()
            } else {
                Value::String(text.into())
            };
        origins.insert(path.clone(), original_name.to_string());
        entries.insert(path, value);
    }
    for (path, value) in entries {
        set_value(tree, &path.0, value).map_err(|error| error.at(&path))?;
    }
    Ok(origins)
}

/// 业务作用：结构覆盖不创建稀疏数组，也不以父子冲突猜测容器类型。
/// 参数说明：`tree` 是当前节点；`path` 是剩余路径；`value` 为原值或显式结构化值。
/// 返回：合法路径完成覆盖，容器类型冲突失败。
fn set_value(tree: &mut Value, path: &[PathSegment], value: Value) -> Result<()> {
    let Some((segment, rest)) = path.split_first() else {
        *tree = value;
        return Ok(());
    };
    match segment {
        PathSegment::Key(key) => {
            if tree.is_null() {
                *tree = Value::Object(Map::new());
            }
            let map = tree
                .as_object_mut()
                .ok_or_else(|| ConfigError::new(ErrorKind::PathConflict))?;
            set_value(map.entry(key.clone()).or_insert(Value::Null), rest, value)
        }
        PathSegment::Index(index) => {
            let item = tree
                .as_array_mut()
                .and_then(|array| array.get_mut(*index))
                .ok_or_else(|| ConfigError::new(ErrorKind::PathConflict))?;
            set_value(item, rest, value)
        }
    }
}

struct DigestWriter {
    digest: Sha256,
    size: usize,
    limit: usize,
}
impl Write for DigestWriter {
    /// 业务作用：结果摘要的序列化过程中限制大小，不先创建无界 Vec。
    /// 参数说明：`bytes` 为下一个序列化片段。
    /// 返回：预算内写入长度，超限后拒绝继续序列化。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.size = self
            .size
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("limit"))?;
        if self.size > self.limit {
            return Err(std::io::Error::other("limit"));
        }
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    /// 业务作用：摘要写入不持有外部缓冲区。
    /// 参数说明：无。
    /// 返回：立即成功。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 业务作用：计算仅供内部值去重的有界序列化身份。
/// 参数说明：`tree` 为候选；`limit` 为最终序列化字节上限。
/// 返回：摘要或预算错误。
fn tree_fingerprint(tree: &Value, limit: usize) -> Result<[u8; 32]> {
    let mut writer = DigestWriter {
        digest: Sha256::new(),
        size: 0,
        limit,
    };
    serde_json::to_writer(&mut writer, tree).map_err(|_| ConfigError::new(ErrorKind::Limit))?;
    Ok(writer.digest.finalize().into())
}

impl fmt::Debug for SourceRecord {
    /// 业务作用：来源默认诊断隐藏名称和摘要。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：数值编号和输入字节数。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceRecord")
            .field("id", &self.id)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for WatchPlan {
    /// 业务作用：观察默认诊断只显示规模，不显示路径。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：来源数量摘要。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchPlan")
            .field("files", &self.files.len())
            .field("patterns", &self.patterns.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for LoadedConfig {
    /// 业务作用：完整候选的默认诊断隐藏业务值和环境材料。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：来源和字段数量摘要。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedConfig")
            .field("sources", &self.sources.len())
            .field("fields", &self.origins.len())
            .finish_non_exhaustive()
    }
}

/// 业务作用：以带长度的编码保留路径边界，避免串联摘要歧义。
/// 参数说明：`hash` 是候选摘要；`path` 是逻辑来源身份。
/// 返回：仅更新内部摘要。
fn fingerprint_path(hash: &mut Sha256, path: &Path) {
    let bytes = path.as_os_str().as_encoded_bytes();
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}
