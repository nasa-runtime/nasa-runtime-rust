//! 国际化翻译工具,保留旧业务翻译器的缓存 key 与开关语义。
//!
//! 该模块只提供机制:全局 enable/disable、语言别名、可替换 cache、可替换
//! translate engine、失败回原文。DB/Redis/Google/DeepL/AI/内部词库等实现
//! 均由业务 adapter 注入,避免 `base` 反向依赖业务数据库或重型网络客户端。

use crate::strings;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// 默认中文源语言。
pub const ZH: &str = "zh-CN";
/// unknown / blank language fallback(对照 active-exch `LangNormalizer.DEFAULT_FALLBACK`)。
pub const DEFAULT_FALLBACK_LANG: &str = "en";
/// 兼容 cache key 中的 source/target 分隔符。
pub const CACHE_ARROW: &str = "->";

/// 翻译缓存接口,只暴露翻译器需要的最小 key-value 能力。
pub trait TranslateCache: Send + Sync + 'static {
    /// 读取缓存;未命中返回 `None`。
    ///
    /// # 参数
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键，包含源语言、目标语言和原文。
    fn get(&self, key: &str) -> Option<String>;

    /// 写入缓存。实现内部错误应自行吞掉/记录,不得影响主流程。
    ///
    /// # 参数
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    /// - `value`: 翻译成功后的目标语言文本。
    fn put(&self, key: &str, value: &str);

    /// 重载/清空缓存。默认 no-op。
    fn reload(&self) {}
}

/// 默认本地内存缓存,用于未注入外部缓存时的轻量兜底。
#[derive(Default)]
pub struct MemoryTranslateCache {
    inner: RwLock<HashMap<String, String>>,
}

impl MemoryTranslateCache {
    /// 新建空缓存。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前缓存条数。主要给诊断用。
    pub fn len(&self) -> usize {
        self.inner.read().expect("translator cache poisoned").len()
    }

    /// 是否为空。主要给诊断用。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TranslateCache for MemoryTranslateCache {
    /// 读取 get 数据；用于查询当前缓存、配置或远端状态。
    ///
    /// # 参数
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    fn get(&self, key: &str) -> Option<String> {
        self.inner
            .read()
            .expect("translator cache poisoned")
            .get(key)
            .cloned()
    }

    /// 写入 put 数据；用于更新缓存、配置或远端状态。
    ///
    /// # 参数
    ///
    /// - `key`: 由 [`cache_key`] 生成的翻译缓存键。
    /// - `value`: 翻译成功后的目标语言文本。
    fn put(&self, key: &str, value: &str) {
        self.inner
            .write()
            .expect("translator cache poisoned")
            .insert(key.to_string(), value.to_string());
    }

    /// 重新加载翻译表；用于让后续翻译读取最新的内存快照。
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
    /// 新建错误。
    ///
    /// # 参数
    ///
    /// - `message`: 描述底层翻译失败原因的文本，供日志和调用方诊断使用。
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// 错误消息。
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TranslateError {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    ///
    /// - `f`: 标准格式化器，用于写入错误消息。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for TranslateError {}

/// 已归一化后的翻译请求。
///
/// `source_lang` / `target_lang` 已经过 [`normalize_lang`] 处理,业务侧 engine
/// 可以直接把它们映射到 Google、DeepL、内部词库、LLM 等自己的协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranslateRequest<'a> {
    source_lang: &'a str,
    target_lang: &'a str,
    text: &'a str,
}

impl<'a> TranslateRequest<'a> {
    /// 新建翻译请求。
    ///
    /// # 参数
    ///
    /// - `source_lang`: 已归一化或待归一化的源语言码。
    /// - `target_lang`: 已归一化或待归一化的目标语言码。
    /// - `text`: 待翻译的原文。
    pub fn new(source_lang: &'a str, target_lang: &'a str, text: &'a str) -> Self {
        Self {
            source_lang,
            target_lang,
            text,
        }
    }

    /// 源语言。
    pub fn source_lang(&self) -> &'a str {
        self.source_lang
    }

    /// 目标语言。
    pub fn target_lang(&self) -> &'a str {
        self.target_lang
    }

    /// 待翻译文本。
    pub fn text(&self) -> &'a str {
        self.text
    }
}

/// 底层翻译器抽象。
///
/// 框架只负责调用这个 trait,不内建、不写死任何具体翻译服务。业务侧可以实现
/// Google、DeepL、OpenAI、公司内部词库、DB 字典或任意组合策略。
pub trait TranslateEngine: Send + Sync + 'static {
    /// 返回 `Ok(Some(text))` 表示翻译成功;`Ok(None)` 或 `Err` 均由工具层回退原文。
    ///
    /// # 参数
    ///
    /// - `request`: 已包含源语言、目标语言和原文的翻译请求。
    fn translate(&self, request: TranslateRequest<'_>) -> Result<Option<String>, TranslateError>;
}

/// 兼容旧命名:新代码优先使用 [`TranslateEngine`]。
pub use TranslateEngine as TranslateProvider;

/// 函数/闭包形式的翻译器,方便业务启动时直接注入轻量 adapter。
pub struct FnTranslateEngine<F> {
    inner: F,
}

impl<F> FnTranslateEngine<F> {
    /// 包装一个 `(from, to, text) -> Result<Option<String>, TranslateError>` 函数。
    ///
    /// # 参数
    ///
    /// - `inner`: 业务提供的翻译闭包；依次接收源语言、目标语言和原文。
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> TranslateEngine for FnTranslateEngine<F>
where
    F: Fn(&str, &str, &str) -> Result<Option<String>, TranslateError> + Send + Sync + 'static,
{
    /// 按请求语言和文本键查找译文；用于向调用方返回命中的本地化文本。
    ///
    /// # 参数
    ///
    /// - `request`: 当前翻译调用的源语言、目标语言和原文。
    fn translate(&self, request: TranslateRequest<'_>) -> Result<Option<String>, TranslateError> {
        (self.inner)(request.source_lang(), request.target_lang(), request.text())
    }
}

/// 构建函数/闭包翻译器。
///
/// # 参数
///
/// - `inner`: 业务提供的翻译闭包；依次接收源语言、目标语言和原文。
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
    /// 新建 fallback 组合。
    ///
    /// # 参数
    ///
    /// - `engines`: 按优先级排列的底层翻译器；前一个无有效译文时会继续尝试下一个。
    pub fn new<I>(engines: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn TranslateEngine>>,
    {
        Self {
            engines: engines.into_iter().collect(),
        }
    }

    /// 添加一个底层翻译器。
    ///
    /// # 参数
    ///
    /// - `engine`: 新增的底层翻译器，会追加到 fallback 顺序的末尾。
    pub fn push(&mut self, engine: Arc<dyn TranslateEngine>) {
        self.engines.push(engine);
    }

    /// 底层翻译器数量。
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// 是否没有底层翻译器。
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }
}

impl TranslateEngine for FallbackTranslateEngine {
    /// 按请求语言和文本键查找译文；用于向调用方返回命中的本地化文本。
    ///
    /// # 参数
    ///
    /// - `request`: 当前翻译调用的源语言、目标语言和原文。
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
    /// 定长条带锁池:替代旧的无界 `HashMap<String, Arc<Mutex<()>>>`(每个不同 word 一条、永不移除 →
    /// 翻译任意用户文本内存单调增长的 DoS)。`hash(key) % LOCK_STRIPES` 选 stripe:同 key 恒落同一 stripe
    /// 从而串行(单飞去重),不同 key 偶发共享 stripe(可接受的假争用),换来常数内存。
    locks: Vec<Mutex<()>>,
}

/// key → 条带索引(单飞:同 key 恒落同一 stripe)。
///
/// # 参数
/// - `key`: 待翻译文本或翻译缓存 key。
fn lock_stripe(key: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % LOCK_STRIPES
}

impl TranslatorState {
    /// 构造新实例；用于集中初始化内部字段和默认状态。
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

/// 返回全局翻译状态；用于集中管理翻译器、别名和缓存。
fn state() -> &'static TranslatorState {
    STATE.get_or_init(TranslatorState::new)
}

/// 开启翻译。
pub fn enable() {
    state().enabled.store(true, Ordering::SeqCst);
}

/// 关闭翻译。
pub fn disable() {
    state().enabled.store(false, Ordering::SeqCst);
}

/// 是否已启用翻译。
pub fn is_enabled() -> bool {
    state().enabled.load(Ordering::SeqCst)
}

/// 设置缓存策略。
///
/// # 参数
///
/// - `cache`: 全局翻译缓存实现；后续读取和写入都会通过该实例完成。
pub fn set_cache(cache: Arc<dyn TranslateCache>) {
    *state()
        .cache
        .write()
        .expect("translator cache lock poisoned") = cache;
}

/// 获取当前缓存策略。
pub fn get_cache() -> Arc<dyn TranslateCache> {
    state()
        .cache
        .read()
        .expect("translator cache lock poisoned")
        .clone()
}

/// 重载缓存。
pub fn reload_cache() {
    get_cache().reload();
}

/// 设置底层翻译器。
///
/// # 参数
///
/// - `engine`: 全局翻译器实现；缓存未命中时会调用它获取译文。
pub fn set_engine(engine: Arc<dyn TranslateEngine>) {
    *state()
        .engine
        .write()
        .expect("translator engine lock poisoned") = Some(engine);
}

/// 获取当前底层翻译器。
pub fn get_engine() -> Option<Arc<dyn TranslateEngine>> {
    state()
        .engine
        .read()
        .expect("translator engine lock poisoned")
        .clone()
}

/// 清除底层翻译器。未设置 engine 时,cache 未命中会直接回原文。
pub fn clear_engine() {
    *state()
        .engine
        .write()
        .expect("translator engine lock poisoned") = None;
}

/// 业务侧注入底层翻译器。等价于 [`set_engine`]。
///
/// # 参数
///
/// - `translator`: 业务注入的翻译器实现；后续缓存未命中时使用该实现。
pub fn set_translator(translator: Arc<dyn TranslateEngine>) {
    set_engine(translator);
}

/// 获取当前业务注入的底层翻译器。等价于 [`get_engine`]。
pub fn get_translator() -> Option<Arc<dyn TranslateEngine>> {
    get_engine()
}

/// 清除业务注入的底层翻译器。等价于 [`clear_engine`]。
pub fn clear_translator() {
    clear_engine();
}

/// 兼容旧命名:新代码优先使用 [`set_translator`] 或 [`set_engine`]。
///
/// # 参数
///
/// - `provider`: 业务注入的翻译器实现；语义等同于 [`set_engine`] 的 `engine`。
pub fn set_provider(provider: Arc<dyn TranslateProvider>) {
    set_engine(provider);
}

/// 兼容旧命名:新代码优先使用 [`clear_translator`] 或 [`clear_engine`]。
pub fn clear_provider() {
    clear_engine();
}

/// 添加单个语言映射。
///
/// # 参数
///
/// - `raw`: 外部请求或配置中可能出现的语言别名。
/// - `normalized`: 框架内部统一使用的语言码。
pub fn put_mapper(raw: impl Into<String>, normalized: impl Into<String>) {
    let raw = raw.into();
    let normalized = normalized.into();
    state()
        .lang_mapper
        .write()
        .expect("translator mapper lock poisoned")
        .insert(lang_key(&raw), normalized);
}

/// 批量添加语言映射。
///
/// # 参数
///
/// - `mapper`: 多个语言别名到标准语言码的映射项。
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

/// 当前语言映射表快照。
pub fn alias_map() -> HashMap<String, String> {
    state()
        .lang_mapper
        .read()
        .expect("translator mapper lock poisoned")
        .clone()
}

/// active-exch `LangNormalizer` 同款默认 alias 表。
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

/// 把语言别名写入映射表；用于将多种输入归一到同一语言键。
///
/// # 参数
///
/// - `map`: 待写入的别名表。
/// - `normalized`: 多个别名最终归一到的标准语言码。
/// - `raw`: 外部可能传入的一组语言别名。
fn add_alias(map: &mut HashMap<String, String>, normalized: &str, raw: &[&str]) {
    for item in raw {
        map.insert(lang_key(item), normalized.to_string());
    }
}

/// 归一化语言码(对照 active-exch `LangNormalizer.normalize`)。
///
/// # 参数
///
/// - `raw`: 外部传入的语言码或语言别名；空白和未知值会回退到 [`DEFAULT_FALLBACK_LANG`]。
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

/// 规范化语言键；用于消除大小写和分隔符差异。
///
/// # 参数
///
/// - `raw`: 外部语言码原文；会裁剪空白、统一大小写和分隔符。
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

/// 构建兼容 cache key:`{from}->{to}:{word}`。
///
/// # 参数
///
/// - `from`: 已归一化的源语言码。
/// - `to`: 已归一化的目标语言码。
/// - `word`: 原始待翻译文本。
pub fn cache_key(from: &str, to: &str, word: &str) -> String {
    format!("{from}{CACHE_ARROW}{to}:{word}")
}

/// 用默认目标语言翻译。当前没有请求上下文语言持有器,默认等价 `translate_to(ZH, word)`。
///
/// # 参数
///
/// - `word`: 待翻译的原文；翻译关闭、空白或未命中时原样返回。
pub fn translate(word: &str) -> String {
    translate_to(ZH, word)
}

/// 从中文翻译到指定语言。
///
/// # 参数
///
/// - `lang_to`: 目标语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `word`: 待翻译的原文；翻译关闭、空白或未命中时原样返回。
pub fn translate_to(lang_to: &str, word: &str) -> String {
    translate_from_to(ZH, lang_to, word)
}

/// 从指定源语言翻译到指定目标语言。
///
/// # 参数
///
/// - `lang_from`: 源语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `lang_to`: 目标语言码或别名，会先经过 [`normalize_lang`] 归一化。
/// - `word`: 待翻译的原文；翻译关闭、空白、同语种或未命中时原样返回。
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
    // panic,不永久毒化整条 stripe(修 P3:panic 引擎污染 per-key 锁)。
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
