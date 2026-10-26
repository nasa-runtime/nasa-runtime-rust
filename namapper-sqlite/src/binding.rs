use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use namapper_core::MapperL2Cache;
use natx_sqlite::{SqliteConn, SqliteDataSource};

/// Client 逐名称固定首次取得的 owner；停机后的旧 client 不会静默指向下一实例。
#[derive(Default)]
pub struct SourceBinding(Mutex<Bindings>);

#[derive(Default)]
struct Bindings {
    explicit: bool,
    sources: BTreeMap<String, SqliteDataSource>,
}

impl SourceBinding {
    /// 业务作用：把显式来源绑定到 client 的同名槽。
    /// 参数说明：
    /// - `source`：宿主授予的来源使用句柄。
    /// 返回：首次或同身份重复绑定成功，不允许替换已有 owner。
    pub fn bind(&self, source: SqliteDataSource) -> anyhow::Result<()> {
        let mut binding = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = binding.sources.get(source.name()) {
            anyhow::ensure!(
                existing.same_source(&source),
                "SQLite client datasource owner is already fixed"
            );
        } else {
            binding.sources.insert(source.name().to_owned(), source);
        }
        binding.explicit = true;
        Ok(())
    }

    /// 业务作用：取得方法指定来源并在首次调用时固定身份。
    /// 参数说明：
    /// - `name`：宏生成的静态 datasource 名称。
    /// 返回：已有绑定或全局目录中的当前来源，不自动重新绑定旧 client。
    pub fn resolve(&self, name: &str) -> anyhow::Result<SqliteDataSource> {
        let mut binding = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(source) = binding.sources.get(name) {
            return Ok(source.clone());
        }
        // 显式 owned client 只能使用已授予的来源，不回落到碰巧同名的全局资源。
        anyhow::ensure!(
            !binding.explicit,
            "SQLite datasource is not explicitly bound to this client"
        );
        let source = natx_sqlite::source_for_call(name)?;
        binding.sources.insert(name.to_owned(), source.clone());
        Ok(source)
    }
}

/// 业务作用：在缓存和连接之前执行 Mapper 声明的事务传播门禁。
/// 参数说明：
/// - `source`：client 固定的来源。
/// - `mode`：auto、mandatory 或 never。
/// 返回：来源和事务上下文均允许调用时成功。
pub fn check_mapper_source(source: &SqliteDataSource, mode: &str) -> anyhow::Result<()> {
    natx_sqlite::check_source(source)?;
    match mode {
        "mandatory" => anyhow::ensure!(
            natx_sqlite::in_transaction(),
            "SQLite Mapper requires a transaction"
        ),
        "never" => anyhow::ensure!(
            natx_core_driver().is_none(),
            "SQLite Mapper forbids ambient transactions"
        ),
        "auto" => {}
        _ => anyhow::bail!("unknown SQLite Mapper transaction mode"),
    }
    Ok(())
}

/// 业务作用：检查 SQLite runtime 共享的跨 driver 事务身份。
/// 参数说明：无。
/// 返回：当前环境事务 driver 或空值。
fn natx_core_driver() -> Option<natx_sqlite::DatabaseDriver> {
    natx_sqlite::current_driver()
}

/// 业务作用：按固定来源取得受控 SQL 执行连接，避免查找与执行之间更换 owner。
/// 参数说明：
/// - `source`：本次调用已固定的来源。
/// - `mode`：方法传播声明。
/// 返回：符合传播门禁的连接或明确拒绝。
pub async fn mapper_conn_from(source: &SqliteDataSource, mode: &str) -> anyhow::Result<SqliteConn> {
    check_mapper_source(source, mode)?;
    natx_sqlite::conn_from(source).await
}

/// 业务作用：缓存读取之前让原生解析器确认最终 SQL 没有写入或多语句副作用。
/// 参数说明：
/// - `source`：固定来源。
/// - `sql`：动态 SQL 完整展开后的文本。
/// 返回：只读验证成功，不执行 SQL step。
pub async fn validate_cached_read(source: &SqliteDataSource, sql: &str) -> anyhow::Result<()> {
    let mut connection = natx_sqlite::conn_from(source).await?;
    connection.validate_read(sql).await
}

/// 业务作用：保持已由宏解析过的 SQLite 引号字节，避免以其它方言的反斜线规则改写文本。
/// 参数说明：
/// - `sql`：受控节点拼接后的 SQL。
/// 返回：仅移除整个语句首尾空白，保留字面量内部内容。
pub fn normalize_sql_whitespace(sql: &str) -> String {
    sql.trim().to_owned()
}

/// 业务作用：把共享缓存隔离到文件 owner 和已提交状态，事务内只允许显式读取已有缓存。
/// 参数说明：
/// - `cache`：宿主提供的可选缓存实现。
/// - `source`：本次方法固定的数据源。
/// 返回：状态可信时返回作用域缓存；宏决定事务内是否启用，事务查询结果不写入共享缓存。
pub fn scoped_cache(
    cache: Option<Arc<dyn MapperL2Cache>>,
    source: &SqliteDataSource,
) -> Option<Arc<dyn MapperL2Cache>> {
    if !source.cache_safe() {
        return None;
    }
    cache.map(|cache| {
        Arc::new(ScopedCache {
            cache,
            source: source.clone(),
            epoch: source.cache_epoch(),
            in_transaction: natx_sqlite::in_transaction(),
        }) as Arc<dyn MapperL2Cache>
    })
}

struct ScopedCache {
    cache: Arc<dyn MapperL2Cache>,
    source: SqliteDataSource,
    epoch: u64,
    in_transaction: bool,
}

impl ScopedCache {
    /// 业务作用：为静态 Mapper namespace 补齐运行期文件 owner，避免每次提交产生新 namespace。
    /// 参数说明：
    /// - `key`：宏生成的 driver、datasource 与 Mapper 身份。
    /// 返回：与其它文件来源隔离、在 owner 内保持稳定的缓存键。
    fn key(&self, key: &str) -> String {
        format!("{}:{key}", self.source.cache_namespace())
    }
}

#[async_trait::async_trait]
impl MapperL2Cache for ScopedCache {
    /// 业务作用：保留底层 single-flight，同时拒绝并发提交前留下的缓存代际。
    /// 参数说明：
    /// - `key`：当前 Mapper namespace。
    /// - `hash_key`：只读 SQL 与绑定签名。
    /// - `ttl_ms`：缓存保留时间。
    /// - `loader`：只允许消费一次的数据库读取回调。
    /// 返回：可信命中或实际读取结果；状态变化时旁路，绝不重放已消费的 loader。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: namapper_core::MapperCacheLoader<'_>,
    ) -> anyhow::Result<namapper_core::MapperCacheLoad> {
        use namapper_core::{MapperCacheLoad, MapperCacheLoadState};
        if let Some(bytes) = self.get(key, hash_key).await? {
            return Ok(MapperCacheLoad {
                bytes,
                state: MapperCacheLoadState::Hit,
            });
        }
        // opt-in 只接受共享缓存的弱一致读取；未提交快照不得借 single-flight 或回填写入全局缓存。
        if self.in_transaction
            || !self.source.cache_safe()
            || self.source.cache_epoch() != self.epoch
        {
            return Ok(MapperCacheLoad {
                bytes: loader().await?,
                state: MapperCacheLoadState::Loaded,
            });
        }
        let scoped_key = self.key(key);
        self.cache.evict(&scoped_key, hash_key).await?;
        let pending = std::sync::Mutex::new(Some(loader));
        let pending_ref = &pending;
        let epoch = self.epoch;
        let encoded_loader: namapper_core::MapperCacheLoader<'_> = Box::new(move || {
            Box::pin(async move {
                let loader = pending_ref
                    .lock()
                    .map_err(|_| anyhow::anyhow!("SQLite cache loader is unavailable"))?
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("SQLite cache loader was already consumed"))?;
                let bytes = loader().await?;
                let mut encoded = epoch.to_le_bytes().to_vec();
                encoded.extend(bytes);
                Ok(encoded)
            })
        });
        let result = self
            .cache
            .get_or_load(&scoped_key, hash_key, ttl_ms, encoded_loader)
            .await?;
        natx_sqlite::check_source(&self.source)?;
        let matching = result
            .bytes
            .get(..8)
            .is_some_and(|bytes| bytes == epoch.to_le_bytes());
        if matching && self.source.cache_safe() && self.source.cache_epoch() == epoch {
            return Ok(MapperCacheLoad {
                bytes: result.bytes[8..].to_vec(),
                state: result.state,
            });
        }
        let remaining = pending
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite cache loader is unavailable"))?
            .take();
        if let Some(loader) = remaining {
            return Ok(MapperCacheLoad {
                bytes: loader().await?,
                state: MapperCacheLoadState::Loaded,
            });
        }
        anyhow::ensure!(matching, "SQLite cache returned an invalid generation");
        Ok(MapperCacheLoad {
            bytes: result.bytes[8..].to_vec(),
            state: MapperCacheLoadState::Loaded,
        })
    }

    /// 业务作用：只读取仍对应当前已提交状态的缓存。
    /// 参数说明：
    /// - `key`：Mapper namespace。
    /// - `hash_key`：SQL 与参数签名。
    /// 返回：状态变化则未命中，缓存服务失败原样传播。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        natx_sqlite::check_source(&self.source)?;
        if !self.source.cache_safe() || self.source.cache_epoch() != self.epoch {
            return Ok(None);
        }
        let result = self.cache.get(&self.key(key), hash_key).await?;
        natx_sqlite::check_source(&self.source)?;
        if self.source.cache_safe() && self.source.cache_epoch() == self.epoch {
            Ok(result.and_then(|bytes| {
                let epoch = bytes.get(..8)?.try_into().ok().map(u64::from_le_bytes)?;
                (epoch == self.epoch).then(|| bytes[8..].to_vec())
            }))
        } else {
            Ok(None)
        }
    }

    /// 业务作用：避免并发写入完成之后把较早查询结果写入当前缓存状态。
    /// 参数说明：
    /// - `key`：Mapper namespace。
    /// - `hash_key`：查询签名。
    /// - `value`：序列化结果。
    /// - `ttl_ms`：缓存保留时间。
    /// 返回：状态仍可信才写入，状态变化直接旁路。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        if self.in_transaction
            || !self.source.cache_safe()
            || self.source.cache_epoch() != self.epoch
        {
            return Ok(());
        }
        let mut encoded = self.epoch.to_le_bytes().to_vec();
        encoded.extend_from_slice(value);
        self.cache
            .put(&self.key(key), hash_key, &encoded, ttl_ms)
            .await
    }

    /// 业务作用：删除当前查询使用的作用域缓存字段。
    /// 参数说明：
    /// - `key`：Mapper namespace。
    /// - `hash_key`：查询签名。
    /// 返回：底层缓存确认删除或返回错误。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.cache.evict(&self.key(key), hash_key).await
    }

    /// 业务作用：清理本次方法使用的缓存状态，清理失败时隔离来源缓存。
    /// 参数说明：
    /// - `key`：需要失效的 Mapper namespace。
    /// 返回：底层清理结果，失败后后续调用直接读取数据库。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        let result = self.cache.clear_key(&self.key(key)).await;
        if result.is_err() {
            self.source.quarantine_cache();
        }
        result
    }
}

/// 业务作用：为 SQLite IN 列表生成匿名参数，拒绝空列表和超过引擎参数容量的展开。
/// 参数说明：
/// - `len`：列表元素数量。
/// 返回：数量一致的占位符或明确边界错误。
pub fn sql_in_placeholders(len: usize) -> anyhow::Result<String> {
    anyhow::ensure!(
        (1..=32766).contains(&len),
        "SQLite IN list must contain 1..=32766 values"
    );
    Ok(std::iter::repeat_n("?", len).collect::<Vec<_>>().join(","))
}

/// 业务作用: 应用动态 SQL `trim/where/set` 的前后缀与 token 剥离规则。
///
/// 参数说明：
/// - `body`: 已展开动态节点的 SQL 片段。
/// - `prefix`: 非空 body 前缀。
/// - `suffix`: 非空 body 后缀。
/// - `prefix_overrides`: 可从片段头部剥离的 token。
/// - `suffix_overrides`: 可从片段尾部剥离的 token。
///
/// 返回: 空片段返回 `None`；其它情况返回规范化后的完整片段。
pub fn apply_sql_trim(
    body: &str,
    prefix: &str,
    suffix: &str,
    prefix_overrides: &[&str],
    suffix_overrides: &[&str],
) -> Option<String> {
    let normalized = normalize_sql_whitespace(body);
    let mut trimmed = normalized.trim().to_owned();
    for token in prefix_overrides.iter().map(|token| token.trim()) {
        if !token.is_empty() && starts_with_sql_token(&trimmed, token) {
            trimmed = trimmed[token.len()..].trim_start().to_owned();
            break;
        }
    }
    for token in suffix_overrides.iter().map(|token| token.trim()) {
        if !token.is_empty() && ends_with_sql_token(&trimmed, token) {
            trimmed.truncate(trimmed.len() - token.len());
            trimmed = trimmed.trim_end().to_owned();
            break;
        }
    }
    if trimmed.is_empty() {
        return None;
    }
    let mut out = String::new();
    if !prefix.trim().is_empty() {
        out.push_str(prefix.trim());
        out.push(' ');
    }
    out.push_str(&trimmed);
    if !suffix.trim().is_empty() {
        out.push(' ');
        out.push_str(suffix.trim());
    }
    Some(out)
}

/// 业务作用: 按 SQL token 边界判断片段是否以指定 override 开头。
///
/// 参数说明：
/// - `value`: 已规范化 SQL 片段。
/// - `token`: trim 配置提供的头部 token。
///
/// 返回: 大小写无关匹配且后侧是 token 边界时为 `true`。
fn starts_with_sql_token(value: &str, token: &str) -> bool {
    value
        .get(..token.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(token))
        && value[token.len()..]
            .chars()
            .next()
            .is_none_or(is_sql_token_boundary)
}

/// 业务作用: 按 SQL token 边界判断片段是否以指定 override 结尾。
///
/// 参数说明：
/// - `value`: 已规范化 SQL 片段。
/// - `token`: trim 配置提供的尾部 token。
///
/// 返回: 大小写无关匹配且前侧是 token 边界时为 `true`。
fn ends_with_sql_token(value: &str, token: &str) -> bool {
    if value.len() < token.len() {
        return false;
    }
    let start = value.len() - token.len();
    value
        .get(start..)
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(token))
        && value[..start]
            .chars()
            .next_back()
            .is_none_or(is_sql_token_boundary)
}

/// 业务作用: 判断字符是否能分隔两个 SQL token。
///
/// 参数说明：
/// - `ch`: token 相邻字符。
///
/// 返回: 非 identifier 字符视为安全边界。
fn is_sql_token_boundary(ch: char) -> bool {
    !(ch == '_' || ch.is_ascii_alphanumeric())
}
