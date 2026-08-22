//! 国际化翻译工具，提供稳定缓存键、全局开关和可替换翻译引擎。
//!
//! 该模块只提供机制:全局 enable/disable、语言别名、可替换 cache、可替换
//! translate engine、失败回原文。具体词库、缓存和远端服务均由业务 adapter 注入，
//! 避免 `base` 反向依赖业务数据库或网络客户端。

use crate::strings;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// 默认中文源语言。
pub const ZH: &str = "zh-CN";
/// 未知或空白语言使用的默认回退语言。
pub const DEFAULT_FALLBACK_LANG: &str = "en";
/// 稳定 cache key 中的 source/target 分隔符。
pub const CACHE_ARROW: &str = "->";

/// 翻译缓存接口,只暴露翻译器需要的最小 key-value 能力。
pub trait TranslateCache: Send + Sync + 'static {
    /// 业务作用：读取缓存;未命中返回 `None`。
    ///
    /// 参数说明:
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键，包含源语言、目标语言和原文。
    ///
    /// 返回: 命中时返回译文副本，未命中或底层读取不可用时返回 `None`。
    fn get(&self, key: &str) -> Option<String>;

    /// 业务作用：写入缓存。实现内部错误应自行吞掉/记录,不得影响主流程。
    ///
    /// 参数说明:
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    /// - `value`: 翻译成功后的目标语言文本。
    ///
    /// 返回: 无返回值；实现不得把缓存写入失败传播到翻译主流程。
    fn put(&self, key: &str, value: &str);

    /// 业务作用：重载/清空缓存。默认 no-op。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 无返回值；默认实现不改变缓存状态。
    fn reload(&self) {}
}

/// 默认本地内存缓存,用于未注入外部缓存时的轻量兜底。
#[derive(Default)]
pub struct MemoryTranslateCache {
    inner: RwLock<HashMap<String, String>>,
}

impl MemoryTranslateCache {
    /// 业务作用：新建空缓存。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 不包含任何翻译条目的进程内缓存。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：当前缓存条数。主要给诊断用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前缓存中的翻译条目数量；锁失效时终止当前调用。
    pub fn len(&self) -> usize {
        self.inner.read().expect("translator cache poisoned").len()
    }

    /// 业务作用：是否为空。主要给诊断用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前没有翻译条目时返回 `true`，否则返回 `false`。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TranslateCache for MemoryTranslateCache {
    /// 业务作用：从进程内翻译缓存读取译文，供翻译主流程在调用 engine 前复用。
    ///
    /// 参数说明:
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    ///
    /// 返回: 命中时返回译文副本，未命中时返回 `None`；锁失效时终止当前调用。
    fn get(&self, key: &str) -> Option<String> {
        self.inner
            .read()
            .expect("translator cache poisoned")
            .get(key)
            .cloned()
    }

    /// 业务作用：将已翻译文本写入进程内缓存，供后续同键请求复用。
    ///
    /// 参数说明:
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    /// - `value`: 翻译成功后的目标语言文本。
    ///
    /// 返回: 无返回值；同名键会被新译文覆盖，锁失效时终止当前调用。
    fn put(&self, key: &str, value: &str) {
        self.inner
            .write()
            .expect("translator cache poisoned")
            .insert(key.to_string(), value.to_string());
    }

    /// 业务作用：重新加载翻译表；用于让后续翻译读取最新的内存快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 无返回值；清空当前全部内存条目，锁失效时终止当前调用。
    fn reload(&self) {
        self.inner
            .write()
            .expect("translator cache poisoned")
            .clear();
    }
}

/// 远程/外部翻译失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslateError {
    message: String,
}

impl TranslateError {
    /// 业务作用：保存底层翻译失败原因，供 engine 向组合层返回稳定错误。
    ///
    /// 参数说明:
    ///
    /// - `message`: 描述底层翻译失败原因的文本，供日志和调用方诊断使用。
    ///
    /// 返回: 保存指定稳定原因文本的翻译错误。
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// 业务作用：向调用方暴露当前翻译错误保存的原因文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前错误保存的原因文本切片。
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TranslateError {
    /// 业务作用：实现可读格式化输出，供错误链和日志展示。
    ///
    /// 参数说明:
    ///
    /// - `f`: 标准格式化器，用于写入错误消息。
    ///
    /// 返回: 错误文本成功写入时返回成功，否则透传格式化错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for TranslateError {}

/// 已归一化后的翻译请求。
///
/// `source_lang` / `target_lang` 已经过 [`normalize_lang`] 处理，业务侧 engine
/// 可以直接把它们映射到自己的 provider 协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranslateRequest<'a> {
    source_lang: &'a str,
    target_lang: &'a str,
    text: &'a str,
}

impl<'a> TranslateRequest<'a> {
    /// 业务作用：把归一化语言与原文组合为借用式翻译请求，供 engine 统一消费。
    ///
    /// 参数说明:
    ///
    /// - `source_lang`: 已归一化或待归一化的源语言码。
    /// - `target_lang`: 已归一化或待归一化的目标语言码。
    /// - `text`: 待翻译的原文。
    ///
    /// 返回: 借用三个输入字段的翻译请求，不复制文本内容。
    pub fn new(source_lang: &'a str, target_lang: &'a str, text: &'a str) -> Self {
        Self {
            source_lang,
            target_lang,
            text,
        }
    }

    /// 业务作用：向翻译 engine 提供请求的源语言标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前请求借用的源语言标签。
    pub fn source_lang(&self) -> &'a str {
        self.source_lang
    }

    /// 业务作用：向翻译 engine 提供请求的目标语言标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前请求借用的目标语言标签。
    pub fn target_lang(&self) -> &'a str {
        self.target_lang
    }

    /// 业务作用：向翻译 engine 提供请求中不可变的原文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前请求借用的原文。
    pub fn text(&self) -> &'a str {
        self.text
    }
}

/// 底层翻译器抽象。
///
/// 框架只负责调用这个 trait，不内建任何具体翻译服务；业务侧负责 provider、词库和组合策略。
pub trait TranslateEngine: Send + Sync + 'static {
    /// 业务作用：返回 `Ok(Some(text))` 表示翻译成功;`Ok(None)` 或 `Err` 均由工具层回退原文。
    ///
    /// 参数说明:
    ///
    /// - `request`: 已包含源语言、目标语言和原文的翻译请求。
    ///
    /// 返回: 命中译文时返回 `Ok(Some(text))`，无结果返回 `Ok(None)`，provider 失败返回
    /// [`TranslateError`]；上层会对后两种结果回退原文。
    fn translate(&self, request: TranslateRequest<'_>) -> Result<Option<String>, TranslateError>;
}

/// [`TranslateEngine`] 的 provider 命名别名。
pub use TranslateEngine as TranslateProvider;

/// 函数/闭包形式的翻译器,方便业务启动时直接注入轻量 adapter。
pub struct FnTranslateEngine<F> {
    inner: F,
}

impl<F> FnTranslateEngine<F> {
    /// 业务作用：包装一个 `(from, to, text) -> Result<Option<String>, TranslateError>` 函数。
    ///
    /// 参数说明:
    ///
    /// - `inner`: 业务提供的翻译闭包；依次接收源语言、目标语言和原文。
    ///
    /// 返回: 持有所给闭包的翻译引擎。
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> TranslateEngine for FnTranslateEngine<F>
where
    F: Fn(&str, &str, &str) -> Result<Option<String>, TranslateError> + Send + Sync + 'static,
{
    /// 业务作用：按请求语言和文本键查找译文；用于向调用方返回命中的本地化文本。
    ///
    /// 参数说明:
    ///
    /// - `request`: 当前翻译调用的源语言、目标语言和原文。
    ///
    /// 返回: 原样返回业务闭包产生的译文、空结果或翻译错误。
    fn translate(&self, request: TranslateRequest<'_>) -> Result<Option<String>, TranslateError> {
        (self.inner)(request.source_lang(), request.target_lang(), request.text())
    }
}

/// 业务作用：构建函数/闭包翻译器。
///
/// 参数说明:
///
/// - `inner`: 业务提供的翻译闭包；依次接收源语言、目标语言和原文。
///
/// 返回: 持有所给闭包的 [`FnTranslateEngine`]。
pub fn engine_from_fn<F>(inner: F) -> FnTranslateEngine<F>
where
    F: Fn(&str, &str, &str) -> Result<Option<String>, TranslateError> + Send + Sync + 'static,
{
    FnTranslateEngine::new(inner)
}

/// 多翻译器 fallback 组合。前一个翻译器返回空、`None` 或 `Err` 时尝试下一个。
#[derive(Default)]
pub struct FallbackTranslateEngine {
    engines: Vec<Arc<dyn TranslateEngine>>,
}

impl FallbackTranslateEngine {
    /// 业务作用：新建 fallback 组合。
    ///
    /// 参数说明:
    ///
    /// - `engines`: 按优先级排列的底层翻译器；前一个无有效译文时会继续尝试下一个。
    ///
    /// 返回: 按迭代顺序保存全部翻译器的 fallback 组合。
    pub fn new<I>(engines: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn TranslateEngine>>,
    {
        Self {
            engines: engines.into_iter().collect(),
        }
    }

    /// 业务作用：添加一个底层翻译器。
    ///
    /// 参数说明:
    ///
    /// - `engine`: 新增的底层翻译器，会追加到 fallback 顺序的末尾。
    ///
    /// 返回: 无返回值；后续请求会在既有翻译器均未命中后调用该实例。
    pub fn push(&mut self, engine: Arc<dyn TranslateEngine>) {
        self.engines.push(engine);
    }

    /// 业务作用：底层翻译器数量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前 fallback 组合保存的翻译器数量。
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// 业务作用：是否没有底层翻译器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 未登记翻译器时返回 `true`，否则返回 `false`。
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }
}

impl TranslateEngine for FallbackTranslateEngine {
    /// 业务作用：按请求语言和文本键查找译文；用于向调用方返回命中的本地化文本。
    ///
    /// 参数说明:
    ///
    /// - `request`: 当前翻译调用的源语言、目标语言和原文。
    ///
    /// 返回: 首个非空白译文；全部翻译器无结果或失败时返回 `Ok(None)`，不会向上传播单个
    /// provider 错误。
    fn translate(&self, request: TranslateRequest<'_>) -> Result<Option<String>, TranslateError> {
        for engine in &self.engines {
            if let Some(value) = strings::option_non_blank(engine.translate(request).ok().flatten())
            {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

/// 单飞条带锁数量。定长,与 key 数无关 → 常数内存。
const LOCK_STRIPES: usize = 256;

/// 翻译模块的全局共享状态。
///
/// 保存开关、缓存、底层翻译器、语言别名和单飞条带锁。它只承载机制状态，
/// 不保存业务请求上下文，也不绑定任何具体翻译服务。
struct TranslatorState {
    /// 全局翻译开关；关闭时所有翻译入口直接返回原文。
    enabled: AtomicBool,
    /// 翻译缓存实现，默认是内存缓存，业务可替换为 Redis/DB 等实现。
    cache: RwLock<Arc<dyn TranslateCache>>,
    /// 当前底层翻译器；未设置时 cache miss 会回退原文。
    engine: RwLock<Option<Arc<dyn TranslateEngine>>>,
    /// 语言别名到标准语言码的映射表。
    lang_mapper: RwLock<HashMap<String, String>>,
    /// 定长条带锁池保证锁内存不随用户文本基数增长。`hash(key) % LOCK_STRIPES` 选择 stripe，
    /// 同 key 恒落同一 stripe 完成单飞去重，不同 key 可以共享 stripe 以换取常数内存。
    locks: Vec<Mutex<()>>,
}

/// 业务作用：key → 条带索引(单飞:同 key 恒落同一 stripe)。
///
/// 参数说明:
/// - `key`: 待翻译文本或翻译缓存 key。
///
/// 返回: 同一 key 稳定映射到的条带锁索引。
fn lock_stripe(key: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % LOCK_STRIPES
}

impl TranslatorState {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 翻译关闭、使用空内存缓存、未设置 engine 且带内置语言映射的共享状态。
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            cache: RwLock::new(Arc::new(MemoryTranslateCache::new())),
            engine: RwLock::new(None),
            lang_mapper: RwLock::new(default_alias_map()),
            locks: (0..LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }
}

static STATE: OnceLock<TranslatorState> = OnceLock::new();

/// 业务作用：返回全局翻译状态；用于集中管理翻译器、别名和缓存。
///
/// 参数说明: 无。
///
/// 返回: 进程内唯一的翻译共享状态，首次调用时完成初始化。
fn state() -> &'static TranslatorState {
    STATE.get_or_init(TranslatorState::new)
}

/// 业务作用：开启翻译。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；后续翻译入口开始执行归一、缓存和 engine 流程。
pub fn enable() {
    state().enabled.store(true, Ordering::SeqCst);
}

/// 业务作用：关闭翻译。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；后续翻译入口直接返回原文，已缓存内容和 engine 保持不变。
pub fn disable() {
    state().enabled.store(false, Ordering::SeqCst);
}

/// 业务作用：是否已启用翻译。
///
/// 参数说明: 无。
///
/// 返回: 全局翻译开关开启时返回 `true`，否则返回 `false`。
pub fn is_enabled() -> bool {
    state().enabled.load(Ordering::SeqCst)
}

/// 业务作用：设置缓存策略。
///
/// 参数说明:
///
/// - `cache`: 全局翻译缓存实现；后续读取和写入都会通过该实例完成。
///
/// 返回: 无返回值；原缓存被替换但不会由本函数清空，状态锁失效时终止当前调用。
pub fn set_cache(cache: Arc<dyn TranslateCache>) {
    *state()
        .cache
        .write()
        .expect("translator cache lock poisoned") = cache;
}

/// 业务作用：获取当前缓存策略。
///
/// 参数说明: 无。
///
/// 返回: 当前全局缓存实现的共享句柄；状态锁失效时终止当前调用。
pub fn get_cache() -> Arc<dyn TranslateCache> {
    state()
        .cache
        .read()
        .expect("translator cache lock poisoned")
        .clone()
}

/// 业务作用：重载缓存。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；行为由当前 [`TranslateCache::reload`] 实现决定。
pub fn reload_cache() {
    get_cache().reload();
}

/// 业务作用：设置底层翻译器。
///
/// 参数说明:
///
/// - `engine`: 全局翻译器实现；缓存未命中时会调用它获取译文。
///
/// 返回: 无返回值；替换后仅影响后续缓存未命中的请求，状态锁失效时终止当前调用。
pub fn set_engine(engine: Arc<dyn TranslateEngine>) {
    *state()
        .engine
        .write()
        .expect("translator engine lock poisoned") = Some(engine);
}

/// 业务作用：获取当前底层翻译器。
///
/// 参数说明: 无。
///
/// 返回: 已设置时返回当前 engine 的共享句柄，未设置时返回 `None`；状态锁失效时终止当前调用。
pub fn get_engine() -> Option<Arc<dyn TranslateEngine>> {
    state()
        .engine
        .read()
        .expect("translator engine lock poisoned")
        .clone()
}

/// 业务作用：清除底层翻译器。未设置 engine 时,cache 未命中会直接回原文。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；后续缓存未命中的请求回退原文，既有缓存保持不变。
pub fn clear_engine() {
    *state()
        .engine
        .write()
        .expect("translator engine lock poisoned") = None;
}

/// 业务作用：业务侧注入底层翻译器。等价于 [`set_engine`]。
///
/// 参数说明:
///
/// - `translator`: 业务注入的翻译器实现；后续缓存未命中时使用该实现。
///
/// 返回: 无返回值；语义与 [`set_engine`] 相同。
pub fn set_translator(translator: Arc<dyn TranslateEngine>) {
    set_engine(translator);
}

/// 业务作用：获取当前业务注入的底层翻译器。等价于 [`get_engine`]。
///
/// 参数说明: 无。
///
/// 返回: 已设置时返回当前 engine 的共享句柄，否则返回 `None`。
pub fn get_translator() -> Option<Arc<dyn TranslateEngine>> {
    get_engine()
}

/// 业务作用：清除业务注入的底层翻译器。等价于 [`clear_engine`]。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；后续缓存未命中的请求回退原文。
pub fn clear_translator() {
    clear_engine();
}

/// 业务作用：以 provider 命名设置翻译引擎，语义等同于 [`set_engine`]。
///
/// 参数说明:
/// - `provider`: 业务注入的翻译器实现；语义等同于 [`set_engine`] 的 `engine`。
///
/// 返回: 无返回值；后续缓存未命中的翻译请求会使用该实例。
pub fn set_provider(provider: Arc<dyn TranslateProvider>) {
    set_engine(provider);
}

/// 业务作用：清除以 provider 命名登记的翻译引擎，语义等同于 [`clear_engine`]。
///
/// 参数说明: 无。
///
/// 返回: 无返回值；清除后缓存未命中的请求会回退原文。
pub fn clear_provider() {
    clear_engine();
}

/// 业务作用：添加单个语言映射。
///
/// 参数说明:
///
/// - `raw`: 外部请求或配置中可能出现的语言别名。
/// - `normalized`: 框架内部统一使用的语言码。
///
/// 返回: 无返回值；归一化后的别名键会覆盖同名映射，状态锁失效时终止当前调用。
pub fn put_mapper(raw: impl Into<String>, normalized: impl Into<String>) {
    let raw = raw.into();
    let normalized = normalized.into();
    state()
        .lang_mapper
        .write()
        .expect("translator mapper lock poisoned")
        .insert(lang_key(&raw), normalized);
}

/// 业务作用：批量添加语言映射。
///
/// 参数说明:
///
/// - `mapper`: 多个语言别名到标准语言码的映射项。
///
/// 返回: 无返回值；按迭代顺序写入映射，同名键保留最后一个值，状态锁失效时终止当前调用。
pub fn put_all_mapper<I, K, V>(mapper: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let mut guard = state()
        .lang_mapper
        .write()
        .expect("translator mapper lock poisoned");
    for (k, v) in mapper {
        let k = k.into();
        guard.insert(lang_key(&k), v.into());
    }
}

/// 业务作用：当前语言映射表快照。
///
/// 参数说明: 无。
///
/// 返回: 当前语言别名表的独立副本；后续全局修改不会改变该快照。
pub fn alias_map() -> HashMap<String, String> {
    state()
        .lang_mapper
        .read()
        .expect("translator mapper lock poisoned")
        .clone()
}

/// 业务作用：构造内置语言别名表，为常见语言标签提供稳定归一结果。
///
/// 参数说明: 无。
///
/// 返回: 语言别名到标准语言标签的独立映射表。
pub fn default_alias_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    add_alias(
        &mut map,
        "zh-CN",
        &["zh", "zh-cn", "zh-hans", "zh-hans-cn", "zh-sg", "chs", "cn"],
    );
    add_alias(
        &mut map,
        "zh-TW",
        &[
            "zh-tw",
            "zh-hant",
            "zh-hant-tw",
            "zh-hant-hk",
            "zh-hk",
            "zh-mo",
            "cht",
            "tw",
            "hk",
        ],
    );
    add_alias(
        &mut map,
        "en",
        &["en", "en-us", "en-gb", "en-au", "en-ca", "en-nz"],
    );
    add_alias(&mut map, "ja", &["ja", "ja-jp", "jp"]);
    add_alias(&mut map, "ko", &["ko", "ko-kr", "kr"]);
    add_alias(&mut map, "vi", &["vi", "vi-vn", "vn"]);
    add_alias(&mut map, "th", &["th", "th-th"]);
    add_alias(&mut map, "id", &["id", "id-id", "in", "in-id"]);
    add_alias(&mut map, "ms", &["ms", "ms-my"]);
    add_alias(&mut map, "tl", &["tl", "fil", "tl-ph"]);
    add_alias(&mut map, "ru", &["ru", "ru-ru"]);
    add_alias(&mut map, "es", &["es", "es-es", "es-mx", "es-ar", "es-419"]);
    add_alias(&mut map, "pt", &["pt", "pt-br", "pt-pt"]);
    add_alias(&mut map, "fr", &["fr", "fr-fr", "fr-ca"]);
    add_alias(&mut map, "de", &["de", "de-de", "de-at"]);
    add_alias(&mut map, "it", &["it", "it-it"]);
    add_alias(&mut map, "nl", &["nl", "nl-nl"]);
    add_alias(&mut map, "pl", &["pl", "pl-pl"]);
    add_alias(&mut map, "tr", &["tr", "tr-tr"]);
    add_alias(&mut map, "ar", &["ar", "ar-sa", "ar-eg", "ar-ae"]);
    add_alias(&mut map, "he", &["he", "iw", "he-il"]);
    add_alias(&mut map, "fa", &["fa", "fa-ir"]);
    add_alias(&mut map, "hi", &["hi", "hi-in"]);
    add_alias(&mut map, "bn", &["bn", "bn-bd", "bn-in"]);
    map
}

/// 业务作用：把语言别名写入映射表；用于将多种输入归一到同一语言键。
///
/// 参数说明:
///
/// - `map`: 待写入的别名表。
/// - `normalized`: 多个别名最终归一到的标准语言码。
/// - `raw`: 外部可能传入的一组语言别名。
///
/// 返回: 无返回值；所有别名按统一键格式写入给定映射表。
fn add_alias(map: &mut HashMap<String, String>, normalized: &str, raw: &[&str]) {
    for item in raw {
        map.insert(lang_key(item), normalized.to_string());
    }
}

/// 业务作用：将语言码或别名归一到内置映射表的标准语言标签。
///
/// 参数说明:
/// - `raw`: 外部传入的语言码或语言别名；空白和未知值会回退到 [`DEFAULT_FALLBACK_LANG`]。
///
/// 返回: 命中别名时返回标准语言标签，空白或未知值返回默认回退语言。
pub fn normalize_lang(raw: &str) -> String {
    let key = lang_key(raw);
    if key.is_empty() {
        return DEFAULT_FALLBACK_LANG.to_string();
    }
    state()
        .lang_mapper
        .read()
        .expect("translator mapper lock poisoned")
        .get(&key)
        .cloned()
        .unwrap_or_else(|| DEFAULT_FALLBACK_LANG.to_string())
}

/// 业务作用：规范化语言键；用于消除大小写和分隔符差异。
///
/// 参数说明:
///
/// - `raw`: 外部语言码原文；会裁剪空白、统一大小写和分隔符。
///
/// 返回: 移除列表参数并统一为小写连字符格式的语言键。
fn lang_key(raw: &str) -> String {
    let mut key = raw.trim().to_ascii_lowercase().replace('_', "-");
    if let Some(idx) = key.find(',') {
        key.truncate(idx);
    }
    if let Some(idx) = key.find(';') {
        key.truncate(idx);
    }
    key.trim().to_string()
}

/// 业务作用：构建稳定 cache key:`{from}->{to}:{word}`。
///
/// 参数说明:
///
/// - `from`: 已归一化的源语言码。
/// - `to`: 已归一化的目标语言码。
/// - `word`: 原始待翻译文本。
///
/// 返回: 由源语言、目标语言和原文确定的缓存键。
pub fn cache_key(from: &str, to: &str, word: &str) -> String {
    format!("{from}{CACHE_ARROW}{to}:{word}")
}

/// 业务作用：用默认目标语言翻译。当前没有请求上下文语言持有器,默认等价 `translate_to(ZH, word)`。
///
/// 参数说明:
///
/// - `word`: 待翻译的原文；翻译关闭、空白或未命中时原样返回。
///
/// 返回: 从默认源语言到默认目标语言的译文；流程未启用或无有效结果时返回原文。
pub fn translate(word: &str) -> String {
    translate_to(ZH, word)
}

/// 业务作用：从中文翻译到指定语言。
///
/// 参数说明:
///
/// - `lang_to`: 目标语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `word`: 待翻译的原文；翻译关闭、空白或未命中时原样返回。
///
/// 返回: 从默认中文源语言到目标语言的译文；流程未启用或无有效结果时返回原文。
pub fn translate_to(lang_to: &str, word: &str) -> String {
    translate_from_to(ZH, lang_to, word)
}

/// 业务作用：从指定源语言翻译到指定目标语言。
///
/// 参数说明:
///
/// - `lang_from`: 源语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `lang_to`: 目标语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `word`: 待翻译的原文；翻译关闭、空白、同语种或未命中时原样返回。
///
/// 返回: 缓存或 engine 命中的非空白译文；流程关闭、同语种、无 engine、无结果或 provider
/// 失败时返回原文。成功译文会写入当前缓存。
pub fn translate_from_to(lang_from: &str, lang_to: &str, word: &str) -> String {
    if !is_enabled() || word.trim().is_empty() {
        return word.to_string();
    }

    let from = normalize_lang(lang_from);
    let to = normalize_lang(lang_to);
    if from == to {
        return word.to_string();
    }

    let key = cache_key(&from, &to, word);
    let cache = get_cache();
    if let Some(value) = strings::option_non_blank(cache.get(&key)) {
        return value;
    }

    // 条带锁单飞:同 key 恒落同一 stripe 串行。poison 时 `into_inner` 恢复——调用方注入的引擎若
    // panic 时仍恢复锁内值，避免一次引擎故障永久毒化整条 stripe。
    let stripe = &state().locks[lock_stripe(&key)];
    let _guard = stripe
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(value) = strings::option_non_blank(cache.get(&key)) {
        return value;
    }

    let engine = state()
        .engine
        .read()
        .expect("translator engine lock poisoned")
        .clone();
    let Some(engine) = engine else {
        return word.to_string();
    };

    let request = TranslateRequest::new(&from, &to, word);
    match engine.translate(request) {
        Ok(Some(value)) if !value.trim().is_empty() => {
            cache.put(&key, &value);
            value
        }
        _ => word.to_string(),
    }
}
