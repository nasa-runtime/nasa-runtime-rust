//! 配置中心客户端 [`NacosConfigClient`]:`fetch`(拉裸文本)+ `watch`(推送回调拿裸文本)+ channel 变体。
//! **只吐原文,不解析、不认识 AppConfig**;只建 ConfigService(不碰命名服务)。

use crate::client::{self, NacosProps};

/// 配置中心客户端:内含 ConfigService(由 SDK 维护其连接)。必须持有到不再需要配置为止(drop 即断连)。
/// feature 关时为空壳(永不被构造,因为 [`connect`](Self::connect) 已先 bail)。
pub struct NacosConfigClient {
    #[cfg(feature = "nacos")]
    config: nacos_sdk::api::config::ConfigService,
    /// 默认分组(`*_default_group` 便利方法用)。
    #[cfg(feature = "nacos")]
    group: String,
}

impl std::fmt::Debug for NacosConfigClient {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NacosConfigClient");
        #[cfg(feature = "nacos")]
        s.field("group", &self.group);
        s.finish_non_exhaustive()
    }
}

/// 业务作用：配置侧参数校验(data_id/group 非空)。
///
/// # 参数
/// - `data_id`: 业务标识,用于定位具体对象或记录。
/// - `group`: 消费组、服务分组或任务分组名称。
#[cfg(feature = "nacos")]
fn validate_cfg(data_id: &str, group: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!data_id.trim().is_empty(), "nacos: data_id 不能为空");
    anyhow::ensure!(!group.trim().is_empty(), "nacos: group 不能为空");
    client::ensure_no_outer_ws("data_id", data_id)?;
    client::ensure_no_outer_ws("group", group)?;
    Ok(())
}

impl NacosConfigClient {
    /// 业务作用：连接配置中心(只建 ConfigService,含 HTTP 鉴权)。feature 关 → `Err`;开 → 校验参数后连接。
    ///
    /// # 参数
    /// - `props`: Nacos 连接参数,提供服务端地址、命名空间、默认分组和鉴权信息。
    pub async fn connect(props: &NacosProps) -> anyhow::Result<NacosConfigClient> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = props;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            use nacos_sdk::api::config::ConfigServiceBuilder;
            client::validate_props(props)?;
            tracing::info!(server_addr = %props.server_addr, namespace = %props.namespace, "connecting nacos config service");
            let config = ConfigServiceBuilder::new(client::sdk_client_props(props))
                .enable_auth_plugin_http()
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("nacos: build ConfigService failed: {e}"))?;
            Ok(NacosConfigClient {
                config,
                group: props.group.clone(),
            })
        }
    }

    /// 业务作用：拉一次配置原文(yaml/properties/json 由内容本身决定,本 crate 不解析)。app 拿到后自己 parse+merge+apply。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    /// - `group`: Nacos 配置 group,不能为空。
    pub async fn fetch(&self, data_id: &str, group: &str) -> anyhow::Result<String> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            // 单点拉取逻辑抽到 get_one(供 fetch / fetch_many 复用,避免两处 get_config 逻辑分叉)。
            get_one(&self.config, data_id, group).await
        }
    }

    /// 业务作用：同 [`fetch`](Self::fetch),但用 client 默认 group(连接时 `NacosProps::group`),省得每次传。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    pub async fn fetch_default_group(&self, data_id: &str) -> anyhow::Result<String> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = data_id;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let group = self.group.clone();
            self.fetch(data_id, &group).await
        }
    }

    /// 业务作用：拉一次配置,同时返回【原文 + 服务端 md5】。
    ///
    /// 相比 [`fetch`](Self::fetch) 多返回 Nacos 服务端计算的 md5:CAS 写入([`publish_cas`](Self::publish_cas))
    /// 需要拿【当前远端内容的 md5】作为 compare-and-set 基线,直接复用服务端 md5 可避免调用方本地重算 md5 与
    /// 服务端算法不一致导致 CAS 永远失配。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    /// - `group`: Nacos 配置 group,不能为空。
    ///
    /// # 返回
    /// `(内容原文, 服务端 md5)`。
    pub async fn fetch_with_md5(
        &self,
        data_id: &str,
        group: &str,
    ) -> anyhow::Result<(String, String)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            let resp = self
                .config
                .get_config(data_id.to_string(), group.to_string())
                .await
                .map_err(|e| anyhow::anyhow!("nacos: get_config({data_id},{group}) failed: {e}"))?;
            Ok((resp.content().to_string(), resp.md5().to_string()))
        }
    }

    /// 业务作用：CAS(compare-and-set)发布配置:仅当 Nacos 端【当前内容 md5】等于 `cas_md5` 时才写入。
    ///
    /// 用于「并发编辑安全」场景:调用方先 [`fetch_with_md5`](Self::fetch_with_md5) 拿到基线 md5,写入时带上它;
    /// 若在读-改-写窗口内有人抢先改了该 dataId,服务端 md5 已变,本次写被服务端拒绝(返回 `Ok(false)`),
    /// 调用方据此让前端 rebase 后重试,避免相互覆盖。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    /// - `group`: Nacos 配置 group,不能为空。
    /// - `content`: 期望写入的新配置原文(不能为空)。
    /// - `cas_md5`: compare-and-set 基线,应为【写入前远端当前内容】的 md5。
    ///
    /// # 返回
    /// - `Ok(true)`:CAS 命中,已发布。
    /// - `Ok(false)`:CAS 失配(远端已被他人改动),未写入——调用方应让前端 rebase。
    /// - `Err(_)`:网络或服务端错误。
    pub async fn publish_cas(
        &self,
        data_id: &str,
        group: &str,
        content: &str,
        cas_md5: &str,
    ) -> anyhow::Result<bool> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group, content, cas_md5);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            anyhow::ensure!(!content.trim().is_empty(), "nacos: content 不能为空");
            anyhow::ensure!(!cas_md5.trim().is_empty(), "nacos: cas_md5 不能为空");
            // WHY:content_type 固定 yaml,与 watch 侧 `with_file_extension("yaml")` 一致,避免 Nacos 端配置类型漂移。
            self.config
                .publish_config_cas(
                    data_id.to_string(),
                    group.to_string(),
                    content.to_string(),
                    Some("yaml".to_string()),
                    cas_md5.to_string(),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!("nacos: publish_config_cas({data_id},{group}) failed: {e}")
                })
        }
    }

    /// 业务作用：注册配置变更监听:每次推送把【新原文】交给 `on_change`(回调内 parse+merge+apply 由 app 决定)。
    /// 返回 [`WatchGuard`];drop 只做 best-effort 注销监听,也可显式 `guard.close().await`。回调是 trait 要求的【同步】方法,
    /// 内部别做阻塞 IO(纯 CPU 的 parse/merge 直接做;要异步就 `tokio::spawn` 或改用 [`watch_channel`](Self::watch_channel))。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `data_id`: 要监听的 Nacos 配置 dataId。
    /// - `group`: 要监听的 Nacos 配置 group。
    /// - `on_change`: 配置变更回调,收到的是新配置原文。
    pub async fn watch<F>(
        &self,
        data_id: &str,
        group: &str,
        on_change: F,
    ) -> anyhow::Result<WatchGuard>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group, on_change);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            use std::sync::Arc;
            validate_cfg(data_id, group)?;
            let listener: Arc<dyn nacos_sdk::api::config::ConfigChangeListener> =
                Arc::new(CbListener(on_change));
            self.config
                .add_listener(
                    data_id.to_string(),
                    group.to_string(),
                    Arc::clone(&listener),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!("nacos: add_listener({data_id},{group}) failed: {e}")
                })?;
            tracing::info!(data_id, group, "nacos config watch registered");
            Ok(WatchGuard {
                config: self.config.clone(),
                data_id: data_id.to_string(),
                group: group.to_string(),
                listener,
                armed: true,
                handle: tokio::runtime::Handle::current(),
            })
        }
    }

    /// 业务作用：同 [`watch`](Self::watch),但用 client 默认 group。
    ///
    /// # 参数
    /// - `data_id`: 要监听的 Nacos 配置 dataId。
    /// - `on_change`: 配置变更回调,收到的是新配置原文。
    pub async fn watch_default_group<F>(
        &self,
        data_id: &str,
        on_change: F,
    ) -> anyhow::Result<WatchGuard>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, on_change);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let group = self.group.clone();
            self.watch(data_id, &group, on_change).await
        }
    }

    /// 业务作用：channel 变体:返回 [`WatchGuard`] + `watch::Receiver<String>`,便于 app 用 `select!` 统一处理。
    /// **顺序:先注册 listener,再 fetch 播种初值**——避免"fetch 后、注册前"窗口里的变更丢失。
    /// 用 `pushed` 原子标记防竞态:listener 注册后、fetch 返回前若已推过更新,则不再用(可能更旧的)fetch 初值覆盖回去。
    /// fetch 失败时显式 `close` 监听,不留残余。guard 与 rx 都要持有。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `data_id`: 要监听的 Nacos 配置 dataId。
    /// - `group`: 要监听的 Nacos 配置 group。
    pub async fn watch_channel(
        &self,
        data_id: &str,
        group: &str,
    ) -> anyhow::Result<(WatchGuard, tokio::sync::watch::Receiver<String>)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            use std::sync::atomic::{AtomicBool, Ordering};
            use std::sync::Arc;

            let (tx, rx) = tokio::sync::watch::channel(String::new());
            let pushed = Arc::new(AtomicBool::new(false));
            let tx2 = tx.clone();
            let pushed2 = Arc::clone(&pushed);
            let guard = self
                .watch(data_id, group, move |s| {
                    pushed2.store(true, Ordering::SeqCst);
                    let _ = tx2.send(s);
                })
                .await?;

            // fetch 失败:显式 close 监听(不只靠 Drop best-effort),再返回原始错误。
            let initial = match self.fetch(data_id, group).await {
                Ok(v) => v,
                Err(e) => {
                    if let Err(ce) = guard.close().await {
                        tracing::warn!(data_id, group, error = %ce, "nacos: watch_channel 播种失败,close 清理也失败");
                    }
                    return Err(e);
                }
            };

            seed_initial_config(&pushed, &tx, initial);
            Ok((guard, rx))
        }
    }
}

/// 业务作用：`watch_channel` 初值播种:**仅当 listener 还没推过更新时**才用 fetch 初值播种。
/// 否则 listener 注册后、fetch 返回前到达的 push(较新值)会被较旧的 fetch 初值覆盖回去。
/// 用 `swap` 原子地"检查并占位":返回 `false`(此前没 push)才发 initial。
///
/// # 参数
/// - `pushed`: 监听器收到的配置推送内容。
/// - `tx`: 后台任务发送消息的通道或事务句柄。
/// - `initial`: 是否为 watch 建立后的初始快照。
#[cfg(feature = "nacos")]
fn seed_initial_config(
    pushed: &std::sync::atomic::AtomicBool,
    tx: &tokio::sync::watch::Sender<String>,
    initial: String,
) {
    use std::sync::atomic::Ordering;
    if !pushed.swap(true, Ordering::SeqCst) {
        let _ = tx.send(initial);
    }
}

/// 配置 watch 订阅句柄:drop 做 best-effort 注销监听;优雅路径请显式 [`close`](Self::close)。需持有到不再关心该配置变更为止。
#[must_use = "watch 返回的 guard 必须持有;drop 只会 best-effort 注销监听,优雅路径请显式 close().await"]
pub struct WatchGuard {
    #[cfg(feature = "nacos")]
    config: nacos_sdk::api::config::ConfigService,
    #[cfg(feature = "nacos")]
    data_id: String,
    #[cfg(feature = "nacos")]
    group: String,
    #[cfg(feature = "nacos")]
    listener: std::sync::Arc<dyn nacos_sdk::api::config::ConfigChangeListener>,
    #[cfg(feature = "nacos")]
    armed: bool,
    #[cfg(feature = "nacos")]
    handle: tokio::runtime::Handle,
}

impl WatchGuard {
    /// 业务作用：显式注销监听(优雅路径:可 await 等结果)。**成功后**才标记不再 Drop 注销;失败则保留 Drop 兜底。
    /// ⚠️ 担心卡住:用 `tokio::time::timeout(d, guard.close())` 包裹,超时由 Drop best-effort 接力(非阻塞)。
    ///
    #[allow(unused_mut)]
    pub async fn close(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            self.config
                .remove_listener(
                    self.data_id.clone(),
                    self.group.clone(),
                    std::sync::Arc::clone(&self.listener),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "nacos: remove_listener({},{}) failed: {e}",
                        self.data_id,
                        self.group
                    )
                })?;
            self.armed = false;
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for WatchGuard {
    /// 业务作用：释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (config, data_id, group, listener) = (
            self.config.clone(),
            self.data_id.clone(),
            self.group.clone(),
            std::sync::Arc::clone(&self.listener),
        );
        self.handle.spawn(async move {
            if let Err(e) = config.remove_listener(data_id, group, listener).await {
                tracing::warn!("nacos remove_listener on drop failed: {e}");
            }
        });
    }
}

#[cfg(feature = "nacos")]
impl std::fmt::Debug for WatchGuard {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchGuard")
            .field("data_id", &self.data_id)
            .field("group", &self.group)
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

#[cfg(not(feature = "nacos"))]
impl std::fmt::Debug for WatchGuard {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchGuard").finish_non_exhaustive()
    }
}

/// 把用户回调包成 SDK 的 `ConfigChangeListener`:推送时把新原文(String)交给回调。
#[cfg(feature = "nacos")]
struct CbListener<F: Fn(String) + Send + Sync + 'static>(F);

#[cfg(feature = "nacos")]
impl<F: Fn(String) + Send + Sync + 'static> nacos_sdk::api::config::ConfigChangeListener
    for CbListener<F>
{
    /// 业务作用：接收配置变更通知并推进 watch 通道；用于触发订阅方刷新配置。
    ///
    /// # 参数
    /// - `resp`: Nacos SDK 返回的原始响应。
    fn notify(&self, resp: nacos_sdk::api::config::ConfigResponse) {
        tracing::info!(data_id = %resp.data_id(), group = %resp.group(), md5 = %resp.md5(), "nacos config changed, invoking on_change");
        (self.0)(resp.content().to_string());
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 多配置门面(对照):一次拉取/监听一组配置,顺序即覆盖顺序。
//   · fetch_many          按序拉取(optional 缺失跳过、required 缺失报错、保序)。
//   · watch_many_channel  任一变更 → 全量重拉 → 整体发新 bundle(避免混合新旧快照)。
// 仍恪守边界:**只吐裸文本 bundle,绝不认识业务 AppConfig**;merge/apply 由 app(经 nasa::yml)做。
// ════════════════════════════════════════════════════════════════════════════

/// 一条配置引用(支持 `optional:nacos:xxx.yml` 语义)。
///
/// `group = None` → 用 client 默认分组(连接时的 `NacosProps.group`)。
/// `optional` → 缺失时是否可跳过:`false` 缺失即 fail-fast,`true` 缺失 warn+skip。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRef {
    /// 配置 dataId,例如 `application.yml` 或业务服务名对应的配置文件。
    pub data_id: String,
    /// 配置分组;None 表示使用客户端默认 group。
    pub group: Option<String>,
    /// 缺失时是否允许跳过。
    pub optional: bool,
    /// 内容格式 `file_extension`(如 `yaml`/`json`)。中性字符串,nacos **不解释**、只随文档携带,
    /// 供门面层按格式选 parser。门面层拉取前应把它填成具体值(不由 nacos 猜)。
    pub file_extension: Option<String>,
}

impl ConfigRef {
    /// 业务作用：必需引用(缺失即 fail-fast),默认分组。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    pub fn required(data_id: impl Into<String>) -> Self {
        Self {
            data_id: data_id.into(),
            group: None,
            optional: false,
            file_extension: None,
        }
    }

    /// 业务作用：可选引用(缺失 warn+skip),默认分组。
    ///
    /// # 参数
    /// - `data_id`: Nacos 配置 dataId。
    pub fn optional(data_id: impl Into<String>) -> Self {
        Self {
            data_id: data_id.into(),
            group: None,
            optional: true,
            file_extension: None,
        }
    }

    /// 业务作用：指定分组(覆盖默认分组)。
    ///
    /// # 参数
    /// - `group`: Nacos 配置 group。
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// 业务作用：指定内容格式 `file_extension`(门面层按解析优先级填入)。
    ///
    /// # 参数
    /// - `ext`: 内容格式扩展名,如 `yaml` 或 `json`。
    pub fn with_file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = Some(ext.into());
        self
    }
}

/// 一份已拉回的配置文档:`source`(拉取用的 ref,group 已落成具体值、含 `file_extension`)+ 裸文本。
/// 热刷新时门面层按 `source.file_extension` 选 parser 重建(bundle 必须带格式元数据)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDocument {
    /// 拉取该文档使用的配置引用,包含最终 group 与格式元数据。
    pub source: ConfigRef,
    /// 从配置中心读取到的原始文本内容。
    pub content: String,
}

/// 一组按【声明/覆盖顺序】排列的配置文档。下游按顺序叠加(后者覆盖前者同名 key)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigBundle {
    /// 按声明顺序排列的配置文档;下游按该顺序叠加。
    pub documents: Vec<ConfigDocument>,
}

/// 业务作用：分组回退:`Some(非空)` 用它,否则(`None`/空白)回退默认分组。
/// 仅真实后端(`fetch_many`/watch)使用;作为无 feature 依赖被编译时不参与,避免 dead_code。
///
/// # 参数
/// - `group`: 消费组、服务分组或任务分组名称。
/// - `default_group`: 未显式指定 group 时使用的默认分组。
#[cfg(feature = "nacos")]
fn resolved_group(group: &Option<String>, default_group: &str) -> String {
    match group {
        Some(g) if !g.trim().is_empty() => g.clone(),
        _ => default_group.to_string(),
    }
}

/// 业务作用：纯编排:按 `refs` 顺序调用 `fetch_one` 拉取,组装成保序的 [`ConfigBundle`]。
///
/// - `optional=true` 拉取失败：warn + 跳过（兼容策略不细分“配置不存在”与鉴权/网络错误，
///   后续应细分,避免环境故障被静默吞掉)。
/// - `optional=false` 拉取失败:返回 `Err`,启动/重拉 fail-fast。
///
/// 抽成【与 nacos-sdk 无关】的泛型函数,供真实 `fetch_many`/watch 复用，并保持拉取顺序、
/// optional 跳过、required 报错与分组回退的编排边界集中一致。
/// 仅真实后端使用;作为无 feature 依赖被编译时不参与,避免 dead_code。
///
/// # 参数
/// - `default_group`: 未显式指定 group 时使用的默认分组。
/// - `refs`: 需要拉取或监听的配置引用列表。
/// - `fetch_one`: 拉取单个配置文档的异步函数。
#[cfg(feature = "nacos")]
async fn collect_bundle<F, Fut>(
    default_group: &str,
    refs: &[ConfigRef],
    fetch_one: F,
) -> anyhow::Result<ConfigBundle>
where
    F: Fn(String, String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<String>>,
{
    let mut documents = Vec::with_capacity(refs.len());
    for r in refs {
        let group = resolved_group(&r.group, default_group);
        match fetch_one(r.data_id.clone(), group.clone()).await {
            Ok(content) => documents.push(ConfigDocument {
                // source = 解析后的 ref:group 落成具体值,file_extension 保留门面层填好的格式,随文档带走。
                source: ConfigRef {
                    data_id: r.data_id.clone(),
                    group: Some(group),
                    optional: r.optional,
                    file_extension: r.file_extension.clone(),
                },
                content,
            }),
            Err(e) => {
                if r.optional {
                    tracing::warn!(data_id = %r.data_id, group = %group, error = %e, "nacos: 可选配置拉取失败,跳过");
                } else {
                    return Err(e.context(format!("nacos: 必需配置 {} 拉取失败", r.data_id)));
                }
            }
        }
    }
    Ok(ConfigBundle { documents })
}

/// 业务作用：拉单份配置原文(fetch / fetch_many 共用的最小单元:校验 + get_config + 结构化日志)。
///
/// # 参数
/// - `service`: 服务名,用于服务发现或注册中心查询。
/// - `data_id`: 业务标识,用于定位具体对象或记录。
/// - `group`: 消费组、服务分组或任务分组名称。
#[cfg(feature = "nacos")]
async fn get_one(
    service: &nacos_sdk::api::config::ConfigService,
    data_id: &str,
    group: &str,
) -> anyhow::Result<String> {
    validate_cfg(data_id, group)?;
    let resp = service
        .get_config(data_id.to_string(), group.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("nacos: get_config({data_id},{group}) failed: {e}"))?;
    tracing::info!(
        data_id = %resp.data_id(), group = %resp.group(),
        content_type = %resp.content_type(), md5 = %resp.md5(), "nacos config fetched"
    );
    Ok(resp.content().to_string())
}

// ── watch_many 编排(抽成【与 nacos-sdk 无关】的泛型件，统一 guard/fetcher 的关闭顺序)──

/// best-effort 关闭句柄的抽象；真实后端使用 [`WatchGuard`]，调用方无需感知底层句柄类型。
#[cfg(feature = "nacos")]
trait GuardClose: Send + 'static {
    /// 业务作用：关闭 close best effort 流程；用于释放资源并停止后台任务。
    fn close_best_effort(self) -> impl std::future::Future<Output = ()> + Send;
}

#[cfg(feature = "nacos")]
impl GuardClose for WatchGuard {
    /// 业务作用：关闭 close best effort 流程；用于释放资源并停止后台任务。
    async fn close_best_effort(self) {
        if let Err(e) = self.close().await {
            tracing::warn!(error = %e, "nacos: watch_many close guard best-effort 失败");
        }
    }
}

/// 业务作用：逐个 close 全部 guard(注册/初始 reload 失败时清理)。
///
/// # 参数
/// - `guards`: 已注册监听器的生命周期守卫集合。
#[cfg(feature = "nacos")]
async fn close_all<G: GuardClose>(guards: Vec<G>) {
    for g in guards {
        g.close_best_effort().await;
    }
}

/// 业务作用：启动**单后台 reload worker**,并等【初始 reload】发布成功(oneshot ack)后才返回。
///
/// - 所有 reload(初始 + 后续 notify)都由这【一个】 worker 串行执行 `fetch`——天然防并发 fetch 乱序覆盖。
/// - 初始 reload 走的是与后续变更【相同】的 worker 路径(不再函数体内同步播种)。
/// - 初始 fetch 失败 → ack `Err` → 本函数 abort worker、close 全部 guard,返回原始错误(**初始未成功发布前不返回成功**)。
/// - guard drop / 全部 receiver drop 后 worker 自然退出。
///
/// # 参数
/// - `guards`: 已注册监听器的生命周期守卫集合。
/// - `notify`: 触发 reload worker 重新拉取 bundle 的通知器。
/// - `tx`: 后台任务发送消息的通道或事务句柄。
/// - `fetch`: 本次发现或 HTTP 取数操作的结果。
#[cfg(feature = "nacos")]
async fn spawn_reload_worker<G, F, Fut>(
    guards: Vec<G>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    tx: tokio::sync::watch::Sender<ConfigBundle>,
    fetch: F,
) -> anyhow::Result<(Vec<G>, tokio::task::JoinHandle<()>)>
where
    G: GuardClose,
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<ConfigBundle>> + Send,
{
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<anyhow::Result<()>>();
    let task = tokio::spawn(async move {
        // 初始 reload:与后续变更走【同一个 worker】,串行执行。
        match fetch().await {
            Ok(b) => {
                let _ = tx.send(b);
                let _ = ack_tx.send(Ok(()));
            }
            Err(e) => {
                let _ = ack_tx.send(Err(e));
                return; // 初始失败 → 不进后续 loop
            }
        }
        // 后续变更:notify 合并密集信号,串行 fetch;receiver 全 drop → 退出。
        loop {
            notify.notified().await;
            match fetch().await {
                Ok(b) => {
                    if tx.send(b).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "nacos: watch_many 全量重拉失败,保留旧 bundle")
                }
            }
        }
    });

    // 初始 reload 成功发布前不返回成功;失败 → abort worker + close 全部 guard,返回原始错误。
    match ack_rx.await {
        Ok(Ok(())) => Ok((guards, task)),
        Ok(Err(e)) => {
            task.abort();
            close_all(guards).await;
            Err(e)
        }
        Err(_) => {
            task.abort();
            close_all(guards).await;
            anyhow::bail!("nacos: watch_many worker 未 ack 就退出")
        }
    }
}

impl NacosConfigClient {
    /// 业务作用：按 `refs` 顺序拉一组配置，返回保序的 [`ConfigBundle`]，用于对照声明顺序执行覆盖。
    ///
    /// **import 顺序 = 覆盖顺序**:靠前的先加载、被靠后的同名 key 覆盖
    /// (如 `tidb.yml`、`afj-redis.yml` 先于 `${app}.yml`,应用专属配置能覆盖共享配置)。
    /// `ConfigRef.group = None` 回退 client 默认分组。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `refs`: 按声明顺序排列的配置引用列表。
    pub async fn fetch_many(&self, refs: &[ConfigRef]) -> anyhow::Result<ConfigBundle> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = refs;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            collect_bundle(&self.group, refs, |data_id, group| async move {
                get_one(&self.config, &data_id, &group).await
            })
            .await
        }
    }

    /// 业务作用：channel 变体：监听一组配置，任一变更就**全量重拉**后整体发送新的 [`ConfigBundle`]。
    ///
    /// 为什么全量重拉而非只换单个文档:多份配置互相覆盖,只替换变更的那份会让 receiver 看到
    /// 混合的新旧快照;全量重拉保证每次下游拿到的都是【同一时刻、按声明顺序】组装的 bundle。
    ///
    /// 顺序
    ///   1. 建 watch channel + reload 信号(`Notify`)。
    ///   2. **先注册全部 listener**(`register_all`),listener 只投 reload 信号;第 N 个失败 close 前面的。
    ///   3. 启动**单后台 reload worker**(`spawn_reload_worker`):初始 reload 与后续变更走【同一 worker】串行,
    ///      不再函数体内同步播种(避免初始 fetch 与后续变更走两条路径导致的短暂旧快照)。
    ///   4. **初始 reload 成功发布前不返回**(oneshot ack);初始失败 close 全部 guard、abort worker,返回原始错误。
    ///
    /// 返回的 [`MultiWatchGuard`] 与 `rx` 都要持有:guard drop → 注销全部 listener + 中止后台任务。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `refs`: 要监听的一组配置引用;顺序同时决定最终 bundle 的覆盖顺序。
    pub async fn watch_many_channel(
        &self,
        refs: Vec<ConfigRef>,
    ) -> anyhow::Result<(MultiWatchGuard, tokio::sync::watch::Receiver<ConfigBundle>)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = refs;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            use std::sync::Arc;
            use tokio::sync::Notify;

            anyhow::ensure!(
                !refs.is_empty(),
                "nacos: watch_many_channel 的 refs 不能为空"
            );

            let (tx, rx) = tokio::sync::watch::channel(ConfigBundle::default());
            let notify = Arc::new(Notify::new());

            let default_group = self.group.clone();

            // 步骤 2:注册全部 listener(只投 reload 信号);第 N 个失败 close 前面的、返回 Err。
            //
            // 这里刻意写成显式循环而不是 `register_all(async |i| ...)`:async 闭包捕获 `&refs`/`&self`
            // 时,编译器要求其 future 对**任意**生命周期 Send,而调用方(应用运行时的组件 trait object)
            // 恰好需要 `for<'a> Send`——两者相遇就是 "implementation of Send is not general enough"。
            // 显式循环让每次 await 的 future 只需对具体生命周期成立,语义与回滚行为完全不变。
            let mut guards = Vec::with_capacity(refs.len());
            for r in &refs {
                let group = resolved_group(&r.group, &default_group);
                let n = Arc::clone(&notify);
                match self
                    .watch(&r.data_id, &group, move |_yaml| n.notify_one())
                    .await
                {
                    Ok(guard) => guards.push(guard),
                    Err(error) => {
                        close_all(guards).await;
                        return Err(error);
                    }
                }
            }

            // 步骤 3–4:单后台 reload worker。初始 reload 经 worker 串行 + oneshot ack;成功发布前不返回。
            // fetch 闭包 own 克隆(Send+'static),供 worker 初始 + 后续变更复用。
            let service = self.config.clone();
            let refs_for_fetch = refs;
            let group_for_fetch = default_group;
            let fetch = move || {
                let service = service.clone();
                let refs = refs_for_fetch.clone();
                let dg = group_for_fetch.clone();
                async move {
                    collect_bundle(&dg, &refs, |data_id, group| {
                        let service = service.clone();
                        async move { get_one(&service, &data_id, &group).await }
                    })
                    .await
                }
            };
            let (guards, task) = spawn_reload_worker(guards, notify, tx, fetch).await?;

            Ok((MultiWatchGuard { guards, task }, rx))
        }
    }
}

/// 多配置 watch 句柄:持有全部子 [`WatchGuard`] + 后台重拉任务。
/// drop → 中止后台任务 + 各子 guard 各自 best-effort 注销 listener;优雅路径请显式 [`close`](Self::close)。
#[must_use = "watch_many 返回的 guard 必须持有;drop 会注销全部 listener 并中止后台重拉任务"]
pub struct MultiWatchGuard {
    #[cfg(feature = "nacos")]
    guards: Vec<WatchGuard>,
    #[cfg(feature = "nacos")]
    task: tokio::task::JoinHandle<()>,
}

impl MultiWatchGuard {
    /// 业务作用：显式注销全部子 listener + 中止后台重拉任务(优雅路径:可 await 等各子 guard 注销结果)。
    ///
    #[allow(unused_mut)]
    pub async fn close(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            self.task.abort();
            for g in std::mem::take(&mut self.guards) {
                if let Err(e) = g.close().await {
                    tracing::warn!(error = %e, "nacos: MultiWatchGuard.close 子 watch 注销失败");
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for MultiWatchGuard {
    /// 业务作用：释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        // 中止后台任务;子 WatchGuard 由各自的 Drop best-effort 注销 listener。
        self.task.abort();
    }
}

impl std::fmt::Debug for MultiWatchGuard {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("MultiWatchGuard");
        #[cfg(feature = "nacos")]
        s.field("watch_count", &self.guards.len());
        s.finish_non_exhaustive()
    }
}
