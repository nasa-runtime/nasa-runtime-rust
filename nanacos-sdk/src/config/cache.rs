use crate::api::config::ConfigResponse;
use crate::api::plugin::ConfigFilter;
use crate::api::plugin::ConfigResp;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// 单份配置的服务端缓存和本地通知状态；监听器与过滤器仅驻留内存。
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct CacheData {
    /// 服务端配置标识，与分组和命名空间共同定位配置。
    pub data_id: String,
    /// 配置所属分组，参与缓存键和查询条件。
    pub group: String,
    /// 配置所属命名空间，防止不同隔离范围共享缓存身份。
    pub namespace: String,
    /// 服务端声明的内容类型；新缓存缺省为 text，不据此自动反序列化正文。
    pub content_type: String,
    /// 尚未经过本地过滤器处理的配置正文。
    pub content: String,
    /// 服务端返回的正文摘要，用于判断是否需要投递变更通知。
    pub md5: String,
    /// 服务端提供的加密数据密钥，供配置过滤器处理；空串表示未提供该值。
    pub encrypted_data_key: String,
    /// 服务端返回的最后修改时间戳，原样保存，不以本地接收时间覆盖。
    pub last_modified: i64,

    /// 首次填充前为真；首次填充仅建立基线，不向已有监听器发送变更通知。
    #[serde(skip)]
    pub initializing: bool,

    /// 运行中的监听器及各自已安排通知的摘要；不随配置缓存持久化。
    #[serde(skip)]
    pub listeners: Arc<Mutex<Vec<ListenerWrapper>>>,

    /// 按注册顺序执行的异步响应过滤器；不随配置缓存持久化。
    #[serde(skip)]
    pub config_filters: Arc<Vec<Box<dyn ConfigFilter>>>,
}

impl CacheData {
    /// 业务作用：为指定配置建立未填充的缓存基线，等待服务端内容到达后再判断变更。
    ///
    /// 参数说明：
    /// - `config_filters`：后续读取和通知时按序应用的响应过滤器，本次构造不执行它们。
    /// - `data_id`：配置在服务端的标识，构造时不作存在性校验。
    /// - `group`：配置所属分组，由调用方完成缺省值归一化。
    /// - `namespace`：配置的隔离范围，与标识和分组一同保存。
    ///
    /// 返回：正文与摘要为空、类型为 text、处于初始化状态的缓存，不进行网络访问或通知。
    pub fn new(
        config_filters: Arc<Vec<Box<dyn ConfigFilter>>>,
        data_id: String,
        group: String,
        namespace: String,
    ) -> Self {
        Self {
            config_filters,
            data_id,
            group,
            namespace,
            content_type: "text".to_string(),
            initializing: true,
            ..Default::default()
        }
    }

    /// 业务作用：为当前配置加入一个接收者，避免同一 Arc 实例被重复安排通知。
    ///
    /// 参数说明：
    /// - `listener`：变更接收者；实例身份用于去重，不比较回调类型或业务内容。
    ///
    /// 返回：未注册时追加空摘要的监听状态；已有相同实例或监听器锁中毒时保持原状，不立即调用接收者。
    pub fn add_listener(&mut self, listener: Arc<dyn crate::api::config::ConfigChangeListener>) {
        if let Ok(mut mutex) = self.listeners.lock() {
            if Self::index_of_listener(mutex.deref(), &listener).is_some() {
                return;
            }
            mutex.push(ListenerWrapper::new(Arc::clone(&listener)));
        }
    }

    /// 业务作用：停止为指定接收者安排后续通知，不取消已经启动的回调任务。
    ///
    /// 参数说明：
    /// - `listener`：待移除的原始 Arc 实例，仅移除身份相同的接收者。
    ///
    /// 返回：找到实例时移除其去重状态；不存在或锁中毒时不变，剩余监听器的数组顺序可能改变。
    pub fn remove_listener(&mut self, listener: Arc<dyn crate::api::config::ConfigChangeListener>) {
        if let Ok(mut mutex) = self.listeners.lock()
            && let Some(idx) = Self::index_of_listener(mutex.deref(), &listener)
        {
            mutex.swap_remove(idx);
        }
    }

    /// 业务作用：按共享实例身份定位接收者，为注册去重和取消监听使用同一判断规则。
    ///
    /// 参数说明：
    /// - `listen_warp_vec`：调用方持锁期间借用的监听器列表，本函数不修改列表。
    /// - `listener`：要匹配的接收者实例，Arc 克隆仍被视为同一个接收者。
    ///
    /// 返回：首个身份匹配项的下标；没有匹配项时为 None，不调用任何业务回调。
    fn index_of_listener(
        listen_warp_vec: &[ListenerWrapper],
        listener: &Arc<dyn crate::api::config::ConfigChangeListener>,
    ) -> Option<usize> {
        listen_warp_vec
            .iter()
            .position(|listen_warp| Arc::ptr_eq(&listen_warp.listener, listener))
    }

    /// 业务作用：先过滤当前配置响应，再为摘要变化的监听器安排异步通知。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：过滤器完成并安排通知后返回，不等待业务回调完成；监听器锁中毒时不投递。
    /// 调用方不得持有 DashMap 守卫跨越本次等待，否则过滤器或其它缓存操作可能因分片锁无法取得而停滞。
    pub async fn notify_listener(&mut self) {
        // 过滤器可以等待外部动作，进入此处以前必须已经解除缓存分片锁。
        let config_resp = self.get_config_resp_after_filter().await;
        self.dispatch_notify(config_resp);
    }

    /// 业务作用：依据各监听器的摘要安排配置通知，并同步记录已经安排的摘要以抑制重复投递。
    ///
    /// 参数说明：
    /// - `config_resp`：经过过滤器处理且与当前缓存摘要对应的响应，由调用方保证快照一致性。
    ///
    /// 返回：为摘要不同的接收者启动独立任务后返回；锁中毒时跳过通知，不等待或保证回调成功及完成顺序。
    pub(crate) fn dispatch_notify(&self, config_resp: ConfigResponse) {
        tracing::info!(
            "dispatch_notify, dataId={},group={},namespace={},md5={}",
            self.data_id,
            self.group,
            self.namespace,
            self.md5
        );

        if let Ok(mut mutex) = self.listeners.lock() {
            for listen_wrap in mutex.iter_mut() {
                if listen_wrap.last_md5.eq(&self.md5) {
                    continue;
                }
                // 回调移交独立任务，避免业务代码在监听器锁内执行；此处不等待其执行结果。
                let l_clone = listen_wrap.listener.clone();
                let c_clone = config_resp.clone();
                crate::common::executor::spawn(async move {
                    l_clone.notify(c_clone);
                });
                // 去重状态记录已安排的通知；它不是业务处理成功的确认，回调失败不会自动回退摘要。
                listen_wrap.last_md5 = self.md5.clone();
            }
        }
    }

    /// 业务作用：在读取缓存时取得独立响应输入，使调用方能够释放缓存守卫后再执行异步过滤。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享的过滤器列表及复制的配置身份、正文、加密数据密钥、类型和摘要；不运行过滤器或修改缓存。
    pub(crate) fn resp_snapshot(&self) -> RespSnapshot {
        (
            Arc::clone(&self.config_filters),
            ConfigResp::new(
                self.data_id.clone(),
                self.group.clone(),
                self.namespace.clone(),
                self.content.clone(),
                self.encrypted_data_key.clone(),
            ),
            self.content_type.clone(),
            self.md5.clone(),
        )
    }

    /// 业务作用：按注册顺序转换独立配置响应，使读取和通知共用同一过滤链。
    ///
    /// 参数说明：
    /// - `filters`：快照携带的异步过滤器列表，每个过滤器完成后才执行下一个。
    /// - `conf_resp`：允许过滤器修改的原始响应，包含配置身份、正文及加密数据密钥。
    /// - `content_type`：服务端声明的内容类型，过滤后仍原样附加到响应。
    /// - `md5`：服务端原始内容摘要，不因过滤器改变正文而重新计算。
    ///
    /// 返回：所有过滤器完成后的响应；不写回缓存，不提供过滤器超时或异常恢复，调用方必须先释放缓存守卫。
    pub(crate) async fn filtered_response(
        (filters, mut conf_resp, content_type, md5): RespSnapshot,
    ) -> ConfigResponse {
        // 等待期间不得持有缓存分片锁；过滤器可能再次访问配置，持锁等待会阻塞其依赖。
        for config_filter in filters.iter() {
            config_filter.filter(None, Some(&mut conf_resp)).await;
        }

        ConfigResponse::new(
            conf_resp.data_id,
            conf_resp.group,
            conf_resp.namespace,
            conf_resp.content,
            content_type,
            md5,
        )
    }

    /// 业务作用：取得当前配置的过滤后读取视图，不将过滤结果覆盖到原始缓存。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：从调用时快照生成的响应，保留服务端类型与摘要；等待过滤器完成，调用方不得持有缓存守卫。
    pub(crate) async fn get_config_resp_after_filter(&self) -> ConfigResponse {
        Self::filtered_response(self.resp_snapshot()).await
    }
}

impl std::fmt::Display for CacheData {
    /// 业务作用：输出配置身份与有限长度正文，供调用方展示缓存状态。
    ///
    /// 参数说明：
    /// - `f`：接收格式化文本的目标，内容包含加密数据密钥及正文前缀，不进行脱敏。
    ///
    /// 返回：写入完整展示文本时成功；目标写入失败立即返回格式化错误，不改变缓存。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CacheData(namespace={n},data_id={d},group={g},md5={m},encrypted_data_key={k},content_type={t},content=",
            n = self.namespace,
            d = self.data_id,
            g = self.group,
            m = self.md5,
            k = self.encrypted_data_key,
            t = self.content_type,
        )?;
        // 按字符而非字节截断，避免多字节正文被切断；长度限制不提供敏感内容遮蔽。
        if self.content.chars().count() > 30 {
            for c in self.content.chars().take(30) {
                write!(f, "{}", c)?;
            }
            write!(f, "...")?;
        } else {
            write!(f, "{}", self.content)?;
        }
        write!(f, ")")
    }
}

/// 脱离缓存锁的响应输入，依次包含过滤器、原始响应、内容类型和服务端摘要。
pub(crate) type RespSnapshot = (Arc<Vec<Box<dyn ConfigFilter>>>, ConfigResp, String, String);

/// 单个监听器的通知去重状态，不代表回调已执行成功。
pub(crate) struct ListenerWrapper {
    /// 最近一次已安排回调的配置摘要；初始为空，回调完成时不再更新。
    last_md5: String,
    /// 以 Arc 实例身份区分的变更接收者。
    listener: Arc<dyn crate::api::config::ConfigChangeListener>,
}

impl ListenerWrapper {
    /// 业务作用：为新接收者建立独立去重状态，使后续通知按该接收者的投递进度判断。
    ///
    /// 参数说明：
    /// - `listener`：缓存需要保留的变更接收者，构造期间不会调用它。
    ///
    /// 返回：通知摘要为空的监听状态；若缓存摘要同样为空，后续分派会将其视为无需通知。
    fn new(listener: Arc<dyn crate::api::config::ConfigChangeListener>) -> Self {
        Self {
            last_md5: "".to_string(),
            listener,
        }
    }
}
