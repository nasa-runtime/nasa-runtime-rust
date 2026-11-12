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
    /// 业务作用：展示配置客户端的默认分组摘要。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要或传播格式化写出错误，不展开连接内部状态。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NacosConfigClient");
        #[cfg(feature = "nacos")]
        s.field("group", &self.group);
        s.finish_non_exhaustive()
    }
}

/// 业务作用：在远端配置操作前拒绝空白标识与分组及首尾空白。
///
/// 参数说明：
/// - `data_id`：Nacos 配置文档标识。
/// - `group`：配置所在分组，不能为空且不得有首尾空白。
///
/// 返回：两个标识均合法时成功，否则返回错误，不发起请求。
#[cfg(feature = "nacos")]
fn validate_cfg(data_id: &str, group: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!data_id.trim().is_empty(), "nacos: data_id 不能为空");
    anyhow::ensure!(!group.trim().is_empty(), "nacos: group 不能为空");
    client::ensure_no_outer_ws("data_id", data_id)?;
    client::ensure_no_outer_ws("group", group)?;
    Ok(())
}

impl NacosConfigClient {
    /// 业务作用：建立只用于配置读写和监听的 Nacos ConfigService。
    ///
    /// 参数说明：
    /// - `props`：服务端、命名空间、默认分组和鉴权配置。
    ///
    /// 返回：配置校验和 SDK 构建成功时返回客户端；配置非法、构建失败或 nacos 特性关闭时返回错误。
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

    /// 业务作用：读取指定分组的一份配置原文。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    ///
    /// 返回：成功时返回配置原文，不解析内容；输入非法、配置缺失、SDK 调用失败或 nacos 特性关闭时返回错误。
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

    /// 业务作用：使用连接时的默认分组读取配置原文。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    ///
    /// 返回：成功时返回配置原文，不解析内容；输入非法、配置缺失、SDK 调用失败或 nacos 特性关闭时返回错误。
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

    /// 业务作用：读取配置原文与服务端摘要，为后续 CAS 提供比较基线。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    ///
    /// 返回：成功时返回正文与服务端 md5；输入非法、配置缺失、SDK 调用失败或特性关闭时返回错误。
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

    /// 业务作用：区分明确的远端配置缺失与其它依赖故障。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    ///
    /// 返回：存在时返回正文和 md5，ConfigNotFound、响应码 300 或精确文本分类命中时返回 None；其它错误和特性关闭返回失败，不把鉴权或网络错误视作缺失。
    pub async fn fetch_optional_with_md5(
        &self,
        data_id: &str,
        group: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            match self
                .config
                .get_config(data_id.to_string(), group.to_string())
                .await
            {
                Ok(resp) => Ok(Some((resp.content().to_string(), resp.md5().to_string()))),
                Err(nacos_sdk::api::error::Error::ConfigNotFound(_)) => Ok(None),
                // nacos-sdk 0.8.0 对部分 Nacos 3.x gRPC 响应没有保留 ConfigNotFound
                // variant，而是把同一个协议 error_code=300 包进 ErrResponse/ErrResult。只按精确
                // not-found 错误码降级，鉴权、网络与其它 500 仍保持失败闭合。
                Err(nacos_sdk::api::error::Error::ErrResponse(_, _, 300, _)) => Ok(None),
                Err(nacos_sdk::api::error::Error::ErrResult(message))
                    if is_config_not_found_result(&message) =>
                {
                    Ok(None)
                }
                Err(e) => Err(anyhow::anyhow!(
                    "nacos: get_config({data_id},{group}) failed: {e}"
                )),
            }
        }
    }

    /// 业务作用：预读配置并在未见已有文档时执行普通发布，减少覆盖已存在配置的机会。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    /// - `content`：非空配置正文，不解析格式。
    /// - `content_type`：非空的 Nacos 内容类型。
    ///
    /// 返回：预读发现已有配置时返回 false；否则原样返回 SDK 普通发布结果。预读与发布不原子，true 不证明本调用独占首发；输入、读取、发布或特性错误返回失败，失败不证明远端未写入。
    pub async fn publish_initial(
        &self,
        data_id: &str,
        group: &str,
        content: &str,
        content_type: &str,
    ) -> anyhow::Result<bool> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group, content, content_type);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            anyhow::ensure!(!content.trim().is_empty(), "nacos: content 不能为空");
            anyhow::ensure!(
                !content_type.trim().is_empty(),
                "nacos: content_type 不能为空"
            );
            // Nacos 没有暴露原子 create-only 入口；先读只负责阻止覆盖已经可见的配置。并发首发仍须由
            // 调用方使用唯一发布身份并在返回后复读内容，不能把这次预读当作跨进程互斥。
            if self
                .fetch_optional_with_md5(data_id, group)
                .await?
                .is_some()
            {
                return Ok(false);
            }
            self.config
                .publish_config_param(
                    data_id.to_string(),
                    group.to_string(),
                    content.to_string(),
                    Some(content_type.to_string()),
                    None,
                    std::collections::HashMap::new(),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!("nacos: initial publish({data_id},{group}) failed: {e}")
                })
        }
    }

    /// 业务作用：向 Nacos 请求删除指定配置。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    ///
    /// 返回：原样返回 SDK 删除的布尔确认；参数非法、调用失败或特性关闭时返回错误，不保证已交给订阅方的旧内容被撤回。
    pub async fn remove(&self, data_id: &str, group: &str) -> anyhow::Result<bool> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            self.config
                .remove_config(data_id.to_string(), group.to_string())
                .await
                .map_err(|e| anyhow::anyhow!("nacos: remove_config({data_id},{group}) failed: {e}"))
        }
    }

    /// 业务作用：以服务端 md5 比较作为并发配置更新门禁。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    /// - `content`：待发布的非空配置原文。
    /// - `cas_md5`：之前读取的非空服务端 md5，原样提交作为比较条件。
    ///
    /// 返回：以yaml 类型执行 CAS；SDK 成功时返回其布尔结果，明确的 ConfigQueryConflict 或匹配的 CAS 失配文本返回 false。其它错误及特性关闭返回失败；失败不证明远端未写入。
    pub async fn publish_cas(
        &self,
        data_id: &str,
        group: &str,
        content: &str,
        cas_md5: &str,
    ) -> anyhow::Result<bool> {
        self.publish_cas_typed(data_id, group, content, "yaml", cas_md5)
            .await
    }

    /// 业务作用：以服务端 md5 比较作为并发配置更新门禁。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    /// - `content`：待发布的非空配置原文。
    /// - `content_type`：非空的 Nacos 内容类型。
    /// - `cas_md5`：之前读取的非空服务端 md5，原样提交作为比较条件。
    ///
    /// 返回：以指定类型执行 CAS；SDK 成功时返回其布尔结果，明确的 ConfigQueryConflict 或匹配的 CAS 失配文本返回 false。其它错误及特性关闭返回失败；失败不证明远端未写入。
    pub async fn publish_cas_typed(
        &self,
        data_id: &str,
        group: &str,
        content: &str,
        content_type: &str,
        cas_md5: &str,
    ) -> anyhow::Result<bool> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (data_id, group, content, content_type, cas_md5);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_cfg(data_id, group)?;
            anyhow::ensure!(!content.trim().is_empty(), "nacos: content 不能为空");
            anyhow::ensure!(
                !content_type.trim().is_empty(),
                "nacos: content_type 不能为空"
            );
            anyhow::ensure!(!cas_md5.trim().is_empty(), "nacos: cas_md5 不能为空");
            // 写入判定交给 Nacos 服务端原子比较 md5；本地不得在预读后退化为普通 publish，否则会在
            // 并发编辑窗口覆盖后来者。只有明确的 CAS 失配收敛为 false，其它依赖故障保持失败闭合。
            match self
                .config
                .publish_config_cas(
                    data_id.to_string(),
                    group.to_string(),
                    content.to_string(),
                    Some(content_type.to_string()),
                    cas_md5.to_string(),
                )
                .await
            {
                Ok(published) => Ok(published),
                Err(nacos_sdk::api::error::Error::ConfigQueryConflict(_)) => Ok(false),
                Err(nacos_sdk::api::error::Error::ErrResponse(_, _, 500, Some(message)))
                    if is_config_cas_conflict_result(&message) =>
                {
                    Ok(false)
                }
                Err(nacos_sdk::api::error::Error::ErrResult(message))
                    if is_config_cas_conflict_result(&message) =>
                {
                    Ok(false)
                }
                Err(e) => Err(anyhow::anyhow!(
                    "nacos: publish_config_cas({data_id},{group}) failed: {e}"
                )),
            }
        }
    }

    /// 业务作用：为指定配置注册原文变更回调。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    /// - `on_change`：接收新配置原文的同步回调，应避免阻塞 SDK 通知。
    ///
    /// 返回：成功时返回必须持有的监听句柄；参数非法、SDK 添加监听失败或特性关闭时返回错误。回调同步执行，接收原文；Drop 只尽力投递注销，显式 close 可等待结果。
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

    /// 业务作用：使用默认分组注册配置原文变更回调。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `on_change`：接收新配置原文的同步回调。
    ///
    /// 返回：成功时返回必须持有的监听句柄；参数非法、SDK 添加监听失败或特性关闭时返回错误。回调同步执行，接收原文；Drop 只尽力投递注销，显式 close 可等待结果。
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

    /// 业务作用：先建立配置监听，再尝试用单次拉取结果播种 watch 通道。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    /// - `group`：配置所在分组，不能为空且不得有首尾空白。
    ///
    /// 返回：成功时返回句柄与原文接收端；初始拉取失败则尝试关闭监听并返回原错误。原子标志只决定是否尝试播种，不把标志修改与发送合并为原子操作，因此不保证并发推送与初值的发送顺序；特性关闭时失败。
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

/// 业务作用：识别 SDK 错误文本中明确的配置缺失响应。
///
/// 参数说明：
/// - `message`：待分类的 SDK 错误文本。
///
/// 返回：同时包含 error_code=300 及两种既定缺失短语之一时返回 true；其它文本返回 false，大小写敏感。
#[cfg(feature = "nacos")]
fn is_config_not_found_result(message: &str) -> bool {
    message.contains("error_code=300")
        && (message.contains("config data not exist") || message.contains("config not found"))
}

/// 业务作用：识别 SDK 文本中明确的 CAS 发布摘要失配。
///
/// 参数说明：
/// - `message`：待分类的远端错误文本。
///
/// 返回：同时包含 `Cas publish fail` 和 `md5 may have changed` 时返回 true，其它文本返回 false。
#[cfg(feature = "nacos")]
fn is_config_cas_conflict_result(message: &str) -> bool {
    message.contains("Cas publish fail") && message.contains("md5 may have changed")
}

/// 业务作用：用原子标志决定是否尝试发送单配置初值，避免覆盖已经观察到的推送。
///
/// 参数说明：
/// - `pushed`：监听推送与初值共用的占位标志。
/// - `tx`：配置原文 watch 通道发送端。
/// - `initial`：本次初始拉取的配置原文。
///
/// 返回：标志原为 false 时置为 true 并尝试发送，否则跳过；忽略接收端关闭错误。占位与发送分离，不保证并发推送不会在初值之前发送。
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
    /// 业务作用：消费单配置监听句柄并等待 SDK 移除监听器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：SDK 确认后解除 Drop 责任；失败或取消等待时仍保留尽力注销机会，运行时关闭后不保证完成。特性关闭的空句柄直接成功。
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
            // 只有 SDK 确认移除监听后才解除兜底责任；失败和取消仍由 Drop 尽力接手。
            self.armed = false;
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for WatchGuard {
    /// 业务作用：为尚未解除监听责任的配置句柄尽力投递注销任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未持有责任时直接返回；否则向保存的运行时投递并立即返回，失败只记录警告，不等待完成。
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
    /// 业务作用：展示配置标识、分组和待关闭状态。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要或传播格式化错误，不输出配置正文和 SDK 连接内部状态。
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
    /// 业务作用：展示空监听句柄类型。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要或传播格式化错误，不输出配置正文和 SDK 连接内部状态。
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
    /// 业务作用：将 SDK 配置变更的原文同步交给业务回调。
    ///
    /// 参数说明：
    /// - `resp`：带配置标识、分组、md5 和正文的 SDK 响应。
    ///
    /// 返回：记录变更标识后同步调用回调；不解析正文、不创建异步任务，回调展开向上传播。
    fn notify(&self, resp: nacos_sdk::api::config::ConfigResponse) {
        tracing::info!(data_id = %resp.data_id(), group = %resp.group(), md5 = %resp.md5(), "nacos config changed, invoking on_change");
        (self.0)(resp.content().to_string());
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 多配置门面按声明顺序读取和监听配置，具体合并由上层执行。
//   · fetch_many          按序拉取(optional 缺失跳过、required 缺失报错、保序)。
//   · watch_many_channel 在变更后全量重拉并整体发送，不对跨文档读取提供原子快照。
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
    /// 业务作用：创建要求存在的配置引用。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    ///
    /// 返回：返回使用客户端默认分组、未指定内容格式的引用；不校验标识，也不读取配置。
    pub fn required(data_id: impl Into<String>) -> Self {
        Self {
            data_id: data_id.into(),
            group: None,
            optional: false,
            file_extension: None,
        }
    }

    /// 业务作用：创建允许缺失的配置引用。
    ///
    /// 参数说明：
    /// - `data_id`：Nacos 配置文档标识。
    ///
    /// 返回：返回使用客户端默认分组、未指定内容格式的引用；不校验标识，也不读取配置。
    pub fn optional(data_id: impl Into<String>) -> Self {
        Self {
            data_id: data_id.into(),
            group: None,
            optional: true,
            file_extension: None,
        }
    }

    /// 业务作用：为配置引用设置显式分组。
    ///
    /// 参数说明：
    /// - `group`：拉取时使用的分组；空白值在分组解析时回退默认分组。
    ///
    /// 返回：返回更新后的引用，不发起请求，也不规范非空分组的首尾空白。
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// 业务作用：随配置引用携带下游解析使用的内容格式。
    ///
    /// 参数说明：
    /// - `ext`：内容扩展名标识，如 yaml 或 json。
    ///
    /// 返回：返回更新后的引用；本层不校验或解释格式，不转换正文。
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

/// 业务作用：为批量配置引用解析最终查询分组。
///
/// 参数说明：
/// - `group`：可选的显式配置分组。
/// - `default_group`：未指定或仅空白时使用的默认分组。
///
/// 返回：非空白显式值原样复制，否则复制默认分组；不去除非空值的首尾空白。
#[cfg(feature = "nacos")]
fn resolved_group(group: &Option<String>, default_group: &str) -> String {
    match group {
        Some(g) if !g.trim().is_empty() => g.clone(),
        _ => default_group.to_string(),
    }
}

/// 业务作用：按声明顺序串行拉取文档，并保留最终分组与格式元数据。
///
/// 参数说明：
/// - `default_group`：引用未明确指定分组时使用的默认值。
/// - `refs`：按下游覆盖顺序排列的配置引用。
/// - `fetch_one`：以标识、分组和 optional 标记取正文的异步操作；负责将必需文档缺失转换为错误。
///
/// 返回：返回按顺序收集的文档；取数返回 None 时跳过，不在本函数复验 optional。任一次取数失败即返回带标识上下文的错误；串行拉取不构成跨文档原子快照。
#[cfg(feature = "nacos")]
async fn collect_bundle<F, Fut>(
    default_group: &str,
    refs: &[ConfigRef],
    fetch_one: F,
) -> anyhow::Result<ConfigBundle>
where
    F: Fn(String, String, bool) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<String>>>,
{
    let mut documents = Vec::with_capacity(refs.len());
    for r in refs {
        let group = resolved_group(&r.group, default_group);
        match fetch_one(r.data_id.clone(), group.clone(), r.optional).await {
            Ok(Some(content)) => documents.push(ConfigDocument {
                // source = 解析后的 ref:group 落成具体值,file_extension 保留门面层填好的格式,随文档带走。
                source: ConfigRef {
                    data_id: r.data_id.clone(),
                    group: Some(group),
                    optional: r.optional,
                    file_extension: r.file_extension.clone(),
                },
                content,
            }),
            Ok(None) => tracing::debug!(
                data_id = %r.data_id,
                group = %group,
                "nacos: 可选配置不存在,跳过"
            ),
            Err(e) => return Err(e.context(format!("nacos: 配置 {} 拉取失败", r.data_id))),
        }
    }
    Ok(ConfigBundle {
        documents,
    })
}

/// 业务作用：按引用的缺失策略读取单个配置文档。
///
/// 参数说明：
/// - `service`：实际执行查询的 Nacos 配置服务。
/// - `data_id`：Nacos 配置文档标识。
/// - `group`：配置所在分组，不能为空且不得有首尾空白。
/// - `optional`：是否允许明确的文档缺失。
///
/// 返回：存在时返回正文；optional 为 true 且缺失分类命中时返回 None，必需文档缺失及其它错误均失败。
#[cfg(feature = "nacos")]
async fn get_bundle_document(
    service: &nacos_sdk::api::config::ConfigService,
    data_id: &str,
    group: &str,
    optional: bool,
) -> anyhow::Result<Option<String>> {
    if !optional {
        return get_one(service, data_id, group).await.map(Some);
    }
    validate_cfg(data_id, group)?;
    match service
        .get_config(data_id.to_string(), group.to_string())
        .await
    {
        Ok(response) => Ok(Some(response.content().to_string())),
        Err(nacos_sdk::api::error::Error::ConfigNotFound(_))
        | Err(nacos_sdk::api::error::Error::ErrResponse(_, _, 300, _)) => Ok(None),
        Err(nacos_sdk::api::error::Error::ErrResult(message))
            if is_config_not_found_result(&message) =>
        {
            Ok(None)
        }
        Err(error) => Err(anyhow::anyhow!(
            "nacos: get_config({data_id},{group}) failed: {error}"
        )),
    }
}

/// 业务作用：校验文档身份后读取并记录配置元信息。
///
/// 参数说明：
/// - `service`：实际执行查询的 Nacos 配置服务。
/// - `data_id`：Nacos 配置文档标识。
/// - `group`：配置所在分组，不能为空且不得有首尾空白。
///
/// 返回：成功返回原文并记录标识、类型与 md5，不记录正文；参数非法或 SDK 查询失败时返回错误。
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
    /// 业务作用：约定消费监听句柄并等待尽力关闭的异步操作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回可发送的 future；完成表示关闭尝试已结束，不提供成功确认。
    fn close_best_effort(self) -> impl std::future::Future<Output = ()> + Send;
}

#[cfg(feature = "nacos")]
impl GuardClose for WatchGuard {
    /// 业务作用：等待具体配置监听器关闭并记录关闭失败。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：完成关闭尝试；错误只记录警告，不向调用方返回，具体句柄仍可在 Drop 时尽力注销。
    async fn close_best_effort(self) {
        if let Err(e) = self.close().await {
            tracing::warn!(error = %e, "nacos: watch_many close guard best-effort 失败");
        }
    }
}

/// 业务作用：在监听建立失败或初始拉取失败后逐个等待已建句柄关闭。
///
/// 参数说明：
/// - `guards`：需要消费并关闭的已建监听句柄。
///
/// 返回：依次完成关闭尝试，不汇总错误、不设置超时；某次关闭挂起会延迟后续关闭。
#[cfg(feature = "nacos")]
async fn close_all<G: GuardClose>(guards: Vec<G>) {
    for g in guards {
        g.close_best_effort().await;
    }
}

/// 业务作用：用单个后台任务串行完成首次及后续整组配置拉取，避免并行拉取乱序发布。
///
/// 参数说明：
/// - `guards`：已登记的监听句柄，初始化成功时交回调用方。
/// - `notify`：合并变更通知并触发重拉的通知器。
/// - `tx`：整组配置的 watch 发送端。
/// - `fetch`：每次返回完整配置集合的异步拉取操作。
///
/// 返回：首轮 fetch 成功且收到确认时返回原句柄和任务；确认不证明通道仍有接收端，首次发送错误被忽略。初始化失败或无确认时请求中止任务并逐个关闭句柄；后续失败保留旧值。仅在唤醒并发送时发现接收端关闭，空闲时不主动退出。
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

    // 必须取得首次拉取确认才交付句柄；失败先请求停止任务并关闭监听，避免交付未初始化的资源。
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
    /// 业务作用：按引用顺序读取配置集合，交由上层依次合并。
    ///
    /// 参数说明：
    /// - `refs`：有序配置引用，允许为空。
    ///
    /// 返回：成功返回完整收集结果；可选缺失被跳过，必需缺失或依赖故障返回错误。特性关闭时失败；多文档读取不保证同一时刻快照。
    pub async fn fetch_many(&self, refs: &[ConfigRef]) -> anyhow::Result<ConfigBundle> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = refs;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            collect_bundle(&self.group, refs, |data_id, group, optional| async move {
                get_bundle_document(&self.config, &data_id, &group, optional).await
            })
            .await
        }
    }

    /// 业务作用：先注册全部监听，再用单个任务串行拉取并整体发布配置集合。
    ///
    /// 参数说明：
    /// - `refs`：非空的有序配置引用；顺序决定下游合并优先级。
    ///
    /// 返回：首次拉取成功后返回句柄与接收端；注册或初始化失败时尝试关闭已建监听。后续任一取数失败保留旧集合；整体发送避免局部替换，但不保证跨文档同一时刻，特性关闭时失败。
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

            // 逐个建立监听以明确每次借用的生命周期；后续建立失败时，先关闭已经持有的监听再返回。
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
                    collect_bundle(&dg, &refs, |data_id, group, optional| {
                        let service = service.clone();
                        async move {
                            get_bundle_document(&service, &data_id, &group, optional).await
                        }
                    })
                    .await
                }
            };
            let (guards, task) = spawn_reload_worker(guards, notify, tx, fetch).await?;

            Ok((
                MultiWatchGuard {
                    guards,
                    task,
                },
                rx,
            ))
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
    /// 业务作用：请求中止多配置重拉任务，并依次等待各子监听关闭。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：子监听关闭失败仅记录警告，方法仍成功；不等待重拉任务退出，也无关闭超时，不能作为所有远端监听均已移除的确认。
    #[allow(unused_mut)]
    pub async fn close(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            // 先请求停止发布再关闭监听；abort 不等待在途任务退出。
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
    /// 业务作用：在多配置句柄释放时请求中止重拉任务，子句柄各自承担尽力注销。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发出中止请求后立即返回；不等待后台任务或远端注销完成。
    fn drop(&mut self) {
        // 中止后台任务;子 WatchGuard 由各自的 Drop best-effort 注销 listener。
        self.task.abort();
    }
}

impl std::fmt::Debug for MultiWatchGuard {
    /// 业务作用：展示多配置监听句柄及启用特性时的子监听数量。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要或传播格式化错误，不输出配置正文和 SDK 连接内部状态。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("MultiWatchGuard");
        #[cfg(feature = "nacos")]
        s.field("watch_count", &self.guards.len());
        s.finish_non_exhaustive()
    }
}
