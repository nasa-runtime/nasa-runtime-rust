//! [`YmlLoader`] —— 分层加载入口:本地主配置 + profile + 内存 overlay + 环境变量 →(占位符)→ `T`。

use std::path::PathBuf;

use anyhow::Context;
use serde::de::DeserializeOwned;

use crate::placeholder::{resolve_placeholders, resolve_placeholders_preserving_unresolved};
use crate::source::{read_local_source, resolve_local_sources};
use crate::YmlLocalSources;

/// 配置内容格式。
///
/// 关键契约:**远端 Nacos 内容的格式不能按 dataId 后缀猜**,必须由本地引导配置显式声明;只有
/// `config` crate 已启用的格式受支持,其它在 [`from_extension`](Self::from_extension) 里 fail-fast,不默认 YAML。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    /// YAML 配置文本。
    Yaml,
    /// JSON 配置文本。
    Json,
    /// TOML 配置文本。
    Toml,
}

impl ConfigFormat {
    /// 业务作用：由 `file_extension` 字符串映射:`yaml`/`yml`→Yaml、`json`→Json、`toml`→Toml;其它 **fail-fast**。
    ///
    /// # 参数
    /// - `ext`: 配置文件扩展名或远端配置显式声明的格式名。
    pub fn from_extension(ext: &str) -> anyhow::Result<Self> {
        match ext.trim().to_ascii_lowercase().as_str() {
            "yaml" | "yml" => Ok(ConfigFormat::Yaml),
            "json" => Ok(ConfigFormat::Json),
            "toml" => Ok(ConfigFormat::Toml),
            other => anyhow::bail!(
                "yml: 不支持的配置格式 file_extension={other:?}(仅 yaml/yml/json/toml)"
            ),
        }
    }

    /// 业务作用：转换为 file format 表示；用于对接下游接口。
    fn to_file_format(self) -> config::FileFormat {
        match self {
            ConfigFormat::Yaml => config::FileFormat::Yaml,
            ConfigFormat::Json => config::FileFormat::Json,
            ConfigFormat::Toml => config::FileFormat::Toml,
        }
    }
}

/// 内存 overlay:一段【已在内存里】的配置文本 + 它的 [`ConfigFormat`],按加入顺序叠加(后加覆盖先加)。
///
/// 典型来源:Nacos 拉回的配置文档(格式由本地 `file_extension` 决定)、内嵌配置或特殊启动参数。
///
/// `required`:该 overlay 内容是否必须有效。
///   - `true` → 内容为空或解析失败 = `Err`(fail-fast)。
///   - `false` → 内容为空则跳过;解析失败则 warn+skip。
///
/// ⚠️【optional ≠ 可以吞格式错】:`required`/`optional` 只表达「源是否可缺」。
///   一份【已经拉取/读取成功、内容在手】的配置若格式错,应 `required=true` 让它 fail-fast——门面层
///   (config-boot)对「已 present 的文档」正是设 `required=true`,不因源 optional 就静默吞掉格式错。
#[derive(Debug, Clone)]
pub struct YmlOverlay {
    /// 便于日志/排查的名字(如 dataId `tidb.yml`),不参与合并。
    pub name: String,
    /// 配置原文。
    pub content: String,
    /// 是否必需(见结构体说明)。
    pub required: bool,
    /// 内容格式(YAML/JSON/TOML)——**不猜**,由调用方按 `file_extension` 决定。
    pub format: ConfigFormat,
}

/// 带本地来源事实的加载结果，供热更新、指纹和审计复用与本轮解析完全一致的文件集合。
#[derive(Debug, Clone)]
pub struct LoadedYml<T> {
    /// 完成文件、overlay、环境变量与占位符合并后的强类型配置。
    pub value: T,
    /// 本轮实际加载文件与精确监听候选。
    pub sources: YmlLocalSources,
}

impl<T> LoadedYml<T> {
    /// 业务作用：消费带来源结果并取出强类型配置，适合不再需要 watcher 元数据的调用方。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本轮加载得到的强类型值。
    pub fn into_value(self) -> T {
        self.value
    }
}

impl YmlOverlay {
    /// 业务作用：必需 overlay(内容空/解析失败即 `Err`)。
    ///
    /// # 参数
    /// - `name`: overlay 名称,用于日志和错误定位。
    /// - `content`: overlay 配置原文。
    /// - `format`: overlay 内容格式,由调用方显式指定。
    pub fn required(
        name: impl Into<String>,
        content: impl Into<String>,
        format: ConfigFormat,
    ) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
            required: true,
            format,
        }
    }

    /// 业务作用：可选 overlay(内容空/解析失败则 warn+skip)。
    ///
    /// # 参数
    /// - `name`: overlay 名称,用于日志和错误定位。
    /// - `content`: overlay 配置原文。
    /// - `format`: overlay 内容格式,由调用方显式指定。
    pub fn optional(
        name: impl Into<String>,
        content: impl Into<String>,
        format: ConfigFormat,
    ) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
            required: false,
            format,
        }
    }
}

/// 分层配置加载器。字段全私有,只经 [`standard`](Self::standard) + 链式 `with_*` 定制;
/// 可复用(`&self` 加载,不消费),热刷新时重新 `load_with_overlays` 即可。
#[derive(Debug, Clone)]
pub struct YmlLoader {
    base_file: PathBuf,
    profile_env: String,
    profile_pattern: String,
    env_prefix: String,
    env_separator: String,
    resolve_placeholders: bool,
    preserve_unresolved_placeholders: bool,
    max_file_bytes: Option<usize>,
}

impl YmlLoader {
    /// 业务作用：标准约定加载器。
    ///
    /// 默认本地路径为 `zcf/application.yml`。
    ///
    /// 默认值:
    ///   - 主配置    `zcf/application.yml`(格式按扩展名推断,这里是 YAML)
    ///   - profile   `zcf/application-{APP_PROFILE}`(**仅当 `APP_PROFILE` 显式非空时才加载**;文件缺失不报错)
    ///   - 环境变量  前缀 `APP`、分隔 `__`(`APP__DATABASE__URL` → `database.url`),最后叠加、最高优先级
    ///
    /// ⚠️【profile 无默认】:**不指定 `APP_PROFILE` 就是普通启动,不是 dev 启动**;
    ///   未设置时【不加载】任何 `application-{profile}` 文件,即便磁盘上存在 `application-dev.yml`。
    ///
    /// 路径相对【进程工作目录】。
    pub fn standard() -> Self {
        Self {
            base_file: PathBuf::from("zcf/application.yml"),
            profile_env: "APP_PROFILE".to_string(),
            profile_pattern: "zcf/application-{profile}".to_string(),
            env_prefix: "APP".to_string(),
            env_separator: "__".to_string(),
            resolve_placeholders: true,
            preserve_unresolved_placeholders: false,
            max_file_bytes: None,
        }
    }

    /// 业务作用：覆盖主配置文件路径(默认 `zcf/application.yml`;格式按扩展名推断)。
    ///
    /// # 参数
    /// - `path`: 主配置文件路径,相对进程工作目录或绝对路径均可。
    pub fn base_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.base_file = path.into();
        self
    }

    /// 业务作用：覆盖决定 profile 的环境变量名(默认 `APP_PROFILE`)。
    ///
    /// # 参数
    /// - `name`: 用于读取 profile 名称的环境变量名。
    pub fn profile_env(mut self, name: impl Into<String>) -> Self {
        self.profile_env = name.into();
        self
    }

    /// 业务作用：覆盖 profile 文件名模式(默认 `zcf/application-{profile}`,`{profile}` 会被替换;
    /// 不写扩展名,`with_name` 自动探测 `.yml`/`.yaml`/`.json`/`.toml`)。
    ///
    /// # 参数
    /// - `pattern`: profile 文件名模式,其中 `{profile}` 会替换为环境变量读取到的 profile。
    pub fn profile_file_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.profile_pattern = pattern.into();
        self
    }

    /// 业务作用：覆盖环境变量前缀(默认 `APP`)。
    ///
    /// # 参数
    /// - `prefix`: 环境变量前缀,用于筛选参与配置覆盖的变量。
    pub fn env_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.env_prefix = prefix.into();
        self
    }

    /// 业务作用：覆盖环境变量层级分隔符(默认 `__`)。
    ///
    /// # 参数
    /// - `sep`: 环境变量键映射为配置层级时使用的分隔符。
    pub fn env_separator(mut self, sep: impl Into<String>) -> Self {
        self.env_separator = sep.into();
        self
    }

    /// 业务作用：是否在合并配置源后解析 `${...}` 占位符(默认 true)。
    ///
    /// # 参数
    /// - `enabled`: `true` 表示保持默认配置占位符解析;`false` 表示保留 `${...}` 字面量,
    ///   交给调用方自己的 DSL / 运行期变量系统解释。
    pub fn resolve_placeholders(mut self, enabled: bool) -> Self {
        self.resolve_placeholders = enabled;
        self
    }

    /// 业务作用：是否在解析时保留未命中的 `${...}` 字面量(默认 false,即 fail-fast)。
    ///
    /// # 参数
    /// - `enabled`: `true` 表示已能解析的配置/env/default 占位符照常解析,未命中的
    ///   占位符保留给调用方自己的 DSL / 运行期变量系统解释。
    pub fn preserve_unresolved_placeholders(mut self, enabled: bool) -> Self {
        self.preserve_unresolved_placeholders = enabled;
        self
    }

    /// 业务作用：限制主配置和 profile 的单文件读取量，防止配置源异常增长造成无界内存占用。
    ///
    /// 参数说明：`max_bytes` 是每个本地配置文件允许读取的最大字节数；overlay 仍由调用方控制。
    ///
    /// 返回：带读取上限的新 loader；文件超过上限时所有加载入口均失败闭合。
    pub fn max_file_bytes(mut self, max_bytes: usize) -> Self {
        self.max_file_bytes = Some(max_bytes);
        self
    }

    /// 业务作用：加载并反序列化成 `T`(等价于无 overlay 的 [`load_with_overlays`](Self::load_with_overlays))。
    pub fn load<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        self.load_with_overlays(&[])
    }

    /// 业务作用：加载强类型配置并返回与本轮解析一致的本地来源事实，供应用建立热更新监听。
    ///
    /// 参数说明：无。
    ///
    /// 返回：配置与来源解析均成功时返回 `LoadedYml<T>`；任一来源或反序列化失败时返回错误。
    pub fn load_tracked<T: DeserializeOwned>(&self) -> anyhow::Result<LoadedYml<T>> {
        self.load_tracked_with_overlays(&[])
    }

    /// 业务作用：加载 + 按顺序叠加内存 overlay,再反序列化成 `T`。
    ///
    /// **overlay 顺序就是覆盖顺序**:`overlays[i+1]` 覆盖 `overlays[i]` 的同名 key。
    /// Nacos 多配置就走这里 —— 门面层把 import 列表按声明顺序拉成 overlay 传进来。
    ///
    /// # 参数
    /// - `overlays`: 按优先级叠加的配置 overlay 列表。
    ///
    /// 返回：全部来源合并并成功反序列化时返回强类型配置；任一必需来源失败时返回错误。
    pub fn load_with_overlays<T: DeserializeOwned>(
        &self,
        overlays: &[YmlOverlay],
    ) -> anyhow::Result<T> {
        self.load_tracked_with_overlays(overlays)
            .map(LoadedYml::into_value)
    }

    /// 业务作用：叠加内存 overlay 后加载强类型配置，并保留本轮实际本地来源供热更新对账。
    ///
    /// 参数说明：`overlays` 按数组顺序覆盖本地来源，环境变量仍具有最高优先级。
    ///
    /// 返回：完整合并和反序列化成功时返回配置及来源；失败时不产生部分结果。
    pub fn load_tracked_with_overlays<T: DeserializeOwned>(
        &self,
        overlays: &[YmlOverlay],
    ) -> anyhow::Result<LoadedYml<T>> {
        let (tree, sources) = self.build_tree(overlays, None)?;
        let value = serde_json::from_value(tree).context("yml: 反序列化目标类型失败")?;
        Ok(LoadedYml { value, sources })
    }

    /// 业务作用：加载并返回【合并 + 占位符已解析】的 Value 树(不做强类型反序列化)。
    ///
    /// 用途:引导阶段需在【拉取远端配置之前】先从本地树解析出 import 列表(见门面 `nasa::yml::nacos`);
    /// 此时占位符已解析,`nacos:${server.name}.yml` 里的 `${}` 已解开、能算出真正的 dataId。
    pub fn load_tree(&self) -> anyhow::Result<serde_json::Value> {
        self.load_tree_with_overlays(&[])
    }

    /// 业务作用：同 [`load_tree`](Self::load_tree),但先按顺序叠加内存 overlay。
    ///
    /// # 参数
    /// - `overlays`: 叠加到本地主配置/profile 之后的内存配置列表,后者覆盖前者。
    ///
    /// 返回：成功时返回占位符已解析的通用配置树；来源读取或合并失败时返回错误。
    pub fn load_tree_with_overlays(
        &self,
        overlays: &[YmlOverlay],
    ) -> anyhow::Result<serde_json::Value> {
        // 生产路径:env_override=None → 读【真实进程环境】。
        self.build_tree(overlays, None).map(|(tree, _)| tree)
    }

    /// 业务作用：解析当前主文件与 profile 环境得到本地来源计划，不读取或反序列化文件内容。
    ///
    /// 参数说明：无。
    ///
    /// 返回：实际加载文件与精确监听候选；已存在但格式不受支持的 profile 返回错误。
    pub fn local_sources(&self) -> anyhow::Result<YmlLocalSources> {
        resolve_local_sources(
            &self.base_file,
            &self.profile_env,
            &self.profile_pattern,
            None,
        )
    }

    /// 业务作用：主配置文件所在目录(本地 `file:` import 的相对基准;`standard()` 下为 `zcf/`)。
    pub fn base_file_dir(&self) -> &std::path::Path {
        self.base_file
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
    }

    /// 业务作用：加载核心(显式接收 env 快照以隔离进程级环境变量读取):合并各 source → 转 Value 树 → 解析占位符。
    ///
    /// 【source 叠加顺序 = 优先级低→高】:
    ///
    /// 1. 主配置文件(格式按扩展名推断)
    /// 2. profile 文件 —— **仅当 `APP_PROFILE` 显式非空时**加载(无默认);缺失不报错
    /// 3. 内存 overlay(含 Nacos 多配置,各自带 `format`)—— 位于【本地文件之后、环境变量之前】:
    ///    远端配置能覆盖本地默认,但运维仍可用 `APP__...` 环境变量【最终】压过一切(应急覆盖不被远端夺走)。
    /// 4. 环境变量 `APP__...` —— 最后叠加,最高优先级。
    ///
    /// config crate 对各 source 做【叶子级深合并】(Nacos 的 `log:` 只补/改单键,不换整段)。
    ///
    /// # 参数
    /// - `overlays`: 按优先级叠加的配置 overlay 列表。
    /// - `env_override`: 环境变量 overlay 映射,优先级高于文件和内存 overlay。
    ///
    /// 返回：成功时返回最终配置树及本轮本地来源；失败时不发布部分树或来源状态。
    fn build_tree(
        &self,
        overlays: &[YmlOverlay],
        env_override: Option<&config::Map<String, String>>,
    ) -> anyhow::Result<(serde_json::Value, YmlLocalSources)> {
        let sources = resolve_local_sources(
            &self.base_file,
            &self.profile_env,
            &self.profile_pattern,
            env_override,
        )?;
        let base_format = self.base_format()?;

        // 在单一文件句柄内完成有界读取，再将内存文本交给 config 合并器；避免应用层
        // metadata 预检与解析器二次打开之间的替换窗口。
        let base_content = read_local_source(sources.base_file(), self.max_file_bytes)?;
        let mut builder = config::Config::builder().add_source(config::File::from_str(
            &base_content,
            base_format.to_file_format(),
        ));

        // profile 是否存在及其扩展名由同一来源计划决定，watcher 与加载器不再各自猜测。
        if let Some(profile) = sources.active_profile_file() {
            let extension = profile
                .extension()
                .context("yml: 活动 profile 文件缺少格式扩展名")?
                .to_str()
                .context("yml: 活动 profile 文件扩展名不是 UTF-8")?;
            let format = ConfigFormat::from_extension(extension)?;
            let content = read_local_source(profile, self.max_file_bytes)?;
            builder = builder.add_source(config::File::from_str(&content, format.to_file_format()));
        }

        // 3) 内存 overlay:按各自 format 叠加(后加覆盖先加)。
        //    per-overlay 预校验:optional 的坏 overlay 只 warn+skip,不连累整体;required 的坏 overlay fail-fast。
        for ov in overlays {
            if ov.content.trim().is_empty() {
                if ov.required {
                    anyhow::bail!("yml: 必需 overlay `{}` 内容为空", ov.name);
                }
                tracing::warn!(overlay = %ov.name, "yml: overlay 内容为空,跳过");
                continue;
            }
            if let Err(e) = validate_overlay(&ov.content, ov.format) {
                if ov.required {
                    return Err(e)
                        .with_context(|| format!("yml: 必需 overlay `{}` 解析失败", ov.name));
                }
                tracing::warn!(overlay = %ov.name, error = %e, "yml: 可选 overlay 解析失败,跳过");
                continue;
            }
            builder = builder.add_source(config::File::from_str(
                &ov.content,
                ov.format.to_file_format(),
            ));
        }

        // 先固化环境层之前的树，后面用它识别已经声明为字符串的字段。仅靠文本猜测类型会把
        // 全数字密码一类值误判成整数，而已有配置值正是最可靠、且不引入业务 schema 的类型依据。
        let base_cfg = builder.build().context("yml: 合并文件与内存配置源失败")?;
        let base_tree: serde_json::Value = base_cfg
            .clone()
            .try_deserialize()
            .context("yml: 环境覆盖前配置转 Value 树失败")?;

        // 4) 环境变量 APP__ 最后叠加。source 注入:调用方可传 map(隔离进程 env),生产 None → 读真实 env。
        let mut env = config::Environment::with_prefix(&self.env_prefix)
            .separator(&self.env_separator)
            .try_parsing(true);
        if let Some(map) = env_override {
            env = env.source(Some(map.clone()));
        }
        let cfg = config::Config::builder()
            .add_source(base_cfg)
            .add_source(env)
            .build()
            .context("yml: 合并环境配置源失败")?;
        // 转通用 Value 树(占位符此时仍是字面量)。
        let mut tree: serde_json::Value =
            cfg.try_deserialize().context("yml: 配置转 Value 树失败")?;
        // 环境源仍负责布尔值和数值解析；只有基线明确声明为字符串的叶子恢复为原始文本，
        // 既保留类型化覆盖，又保证数字形态的口令、编号和文本标识不会改变类型。
        self.restore_declared_string_overrides(&mut tree, &base_tree, env_override);
        // 默认解析 ${...};需要保留运行期 DSL 变量的调用方可显式关闭。
        // 解析失败(未命中且无默认值 / 循环引用)即 fail-fast:所有 load 入口经此传播 Err,
        // 应用启动入口 `?` 后进程启动失败退出。
        if self.resolve_placeholders && self.preserve_unresolved_placeholders {
            resolve_placeholders_preserving_unresolved(&mut tree)?;
        } else if self.resolve_placeholders {
            resolve_placeholders(&mut tree)?;
        }
        Ok((tree, sources))
    }

    /// 业务作用：把环境层中覆盖既有字符串字段的值恢复为原始文本。
    ///
    /// 通用环境源会主动推断布尔值与数值。该行为适合端口、开关等字段，却会让全数字
    /// 口令从字符串变成整数。本方法只依据环境层之前已经存在的叶子类型做定向恢复，
    /// 不改变数值、布尔值、新增字段或复合结构的原有解析规则。
    ///
    /// # 参数
    ///
    /// - `tree`：已经叠加环境层、即将解析占位符的最终配置树。
    /// - `base_tree`：叠加环境层之前的配置树，用于判断字段声明类型。
    /// - `env_override`：调用方可注入的环境映射；为空时读取当前进程环境。
    fn restore_declared_string_overrides(
        &self,
        tree: &mut serde_json::Value,
        base_tree: &serde_json::Value,
        env_override: Option<&config::Map<String, String>>,
    ) {
        let entries: Vec<(String, String)> = match env_override {
            Some(values) => values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            None => std::env::vars().collect(),
        };
        let prefix_separator = if self.env_separator.is_empty() {
            "_"
        } else {
            self.env_separator.as_str()
        };
        let prefix = format!("{}{prefix_separator}", self.env_prefix).to_ascii_lowercase();

        for (raw_key, raw_value) in entries {
            let normalized_key = raw_key.to_ascii_lowercase();
            let Some(without_prefix) = normalized_key.strip_prefix(&prefix) else {
                continue;
            };
            let path = if self.env_separator.is_empty() {
                without_prefix.to_string()
            } else {
                without_prefix.replace(&self.env_separator, ".")
            };
            if Self::tree_value(base_tree, &path).is_some_and(serde_json::Value::is_string) {
                if let Some(target) = Self::tree_value_mut(tree, &path) {
                    *target = serde_json::Value::String(raw_value);
                }
            }
        }
    }

    /// 业务作用：按点号分隔路径借用配置树中的叶子值。
    ///
    /// # 参数
    ///
    /// - `tree`：需要查询的配置树根节点。
    /// - `path`：由环境变量层级分隔符归一得到的点号路径。
    fn tree_value<'a>(tree: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
        path.split('.')
            .try_fold(tree, |current, segment| current.get(segment))
    }

    /// 业务作用：按点号分隔路径可变借用配置树中的叶子值。
    ///
    /// # 参数
    ///
    /// - `tree`：需要修改的配置树根节点。
    /// - `path`：由环境变量层级分隔符归一得到的点号路径。
    fn tree_value_mut<'a>(
        tree: &'a mut serde_json::Value,
        path: &str,
    ) -> Option<&'a mut serde_json::Value> {
        let mut current = tree;
        for segment in path.split('.') {
            current = current.get_mut(segment)?;
        }
        Some(current)
    }

    /// 业务作用：base_file 的格式(按扩展名推断;无扩展名默认 YAML —— base 通常是 `application.yml`)。
    fn base_format(&self) -> anyhow::Result<ConfigFormat> {
        match self.base_file.extension().and_then(|e| e.to_str()) {
            Some(ext) => ConfigFormat::from_extension(ext),
            None => Ok(ConfigFormat::Yaml),
        }
    }
}

/// 业务作用：单个 overlay 的独立语法校验(按其 format;供 optional overlay 的 warn+skip 判定)。
///
/// # 参数
/// - `content`: 待处理的文本内容。
/// - `format`: 配置文本格式,决定使用 YAML、JSON 或 TOML parser。
fn validate_overlay(content: &str, format: ConfigFormat) -> anyhow::Result<()> {
    config::Config::builder()
        .add_source(config::File::from_str(content, format.to_file_format()))
        .build()
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{e}"))
}
