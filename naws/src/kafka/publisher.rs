//! 只生成合法固定 headers 的 raw data publisher。

use nafka::{Delivery, NafkaError, ProducerLane, RawPublishBuilder};

use super::headers::{
    PassthroughConfig, SourceIdentityPolicy, HEADER_WS_EVENT, HEADER_WS_EXCLUDE_UID,
    HEADER_WS_FROM_CLIENT, HEADER_WS_FROM_UID, HEADER_WS_GROUP, HEADER_WS_MESSAGE_MODE,
    HEADER_WS_PAYLOAD_LAYOUT, HEADER_WS_ROUTER, HEADER_WS_SINK_ID, HEADER_WS_SOURCE_INCARNATION,
    HEADER_WS_SOURCE_NODE, HEADER_WS_TARGET_NODE, HEADER_WS_UID, NAWS_PUSH_EVENT,
    PAYLOAD_LAYOUT_RAW,
};
use crate::proto::Mode;

/// 构造时绑定物理 data lane 的 typed publisher。
#[derive(Clone)]
pub struct WsKafkaPublisher {
    /// 独占 data producer lane。
    lane: ProducerLane,
    /// 与消费端共享的 header/mode 上限。
    config: PassthroughConfig,
}

impl WsKafkaPublisher {
    /// 业务作用：构造并校验 publisher。
    ///
    /// 参数说明：
    ///
    /// - `lane`: 启动期选定的 data lane。
    /// - `config`: 与 adapter 一致的安全配置。
    ///
    /// # 错误
    ///
    /// 配置非法时返回文本。
    ///
    /// 返回：保存指定生产通道与有效路由策略的发布器；不检查 broker topic、通道角色或业务调用方权限。
    pub fn new(lane: ProducerLane, config: PassthroughConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            lane,
            config,
        })
    }

    /// 业务作用：创建一条借用 raw payload 的安全发布 builder。
    ///
    /// 参数说明：
    ///
    /// - `topic`: 目标 data topic。
    /// - `payload`: 不经过 JSON/协议信封的原始业务字节。
    ///
    /// 返回：借用发布器、topic 与 payload 的未提交构造器，初始没有事件或目标，模式取配置默认值。
    /// 调用方须补充合法路由后再发送；此方法不复制 payload、不执行准入或 broker 请求。
    pub fn publish<'a>(&'a self, topic: &'a str, payload: &'a [u8]) -> WsKafkaPublishBuilder<'a> {
        WsKafkaPublishBuilder {
            publisher: self,
            topic,
            payload,
            events: Vec::new(),
            routers: Vec::new(),
            uids: Vec::new(),
            excludes: Vec::new(),
            target_nodes: Vec::new(),
            group: None,
            from_uid: None,
            from_client: None,
            source_identity: None,
            sink_id: None,
            message_mode: self.config.default_message_mode,
            key: None,
        }
    }
}

/// 长连接 raw 数据发布 builder。
pub struct WsKafkaPublishBuilder<'a> {
    /// 绑定的 publisher 与物理 lane。
    publisher: &'a WsKafkaPublisher,
    /// 目标 topic。
    topic: &'a str,
    /// 原始 payload。
    payload: &'a [u8],
    /// 客户端事件列表。
    events: Vec<String>,
    /// endpoint/router 列表。
    routers: Vec<String>,
    /// 目标用户列表。
    uids: Vec<String>,
    /// 排除用户列表。
    excludes: Vec<String>,
    /// 目标逻辑节点列表。
    target_nodes: Vec<String>,
    /// 可选目标群组。
    group: Option<String>,
    /// 可选来源用户。
    from_uid: Option<String>,
    /// 可选来源会话。
    from_client: Option<String>,
    /// 可选来源节点与 incarnation。
    source_identity: Option<(String, u128)>,
    /// 可选预注册 sink id。
    sink_id: Option<String>,
    /// 最终消息编码模式。
    message_mode: Mode,
    /// 可选分区 key。
    key: Option<String>,
}

impl<'a> WsKafkaPublishBuilder<'a> {
    /// 业务作用：追加一个客户端事件。
    ///
    /// 参数说明：
    /// - `value`: 下游客户端接收的事件名称，将转为拥有型字符串。
    ///
    /// 返回：追加名称后的构造器，保留顺序和重复项；空值与控制字符在最终发送校验时拒绝。
    pub fn client_event(mut self, value: impl Into<String>) -> Self {
        self.events.push(value.into());
        self
    }

    /// 业务作用：追加一个 endpoint/router。
    ///
    /// 参数说明：
    /// - `value`: 用于端点定向或约束其它目标的路由名称。
    ///
    /// 返回：保留该名称的构造器；不查询端点是否在线，不授予向该端点发布的业务权限。
    pub fn router(mut self, value: impl Into<String>) -> Self {
        self.routers.push(value.into());
        self
    }

    /// 业务作用：追加一个目标用户。
    ///
    /// 参数说明：
    /// - `value`: 下游用于定位该用户连接的用户标识。
    ///
    /// 返回：追加用户后的构造器；不去重或验证业务权限，与订阅目标同时存在时最终发送校验拒绝。
    pub fn uid(mut self, value: impl Into<String>) -> Self {
        self.uids.push(value.into());
        self
    }

    /// 业务作用：选择本次投递的单个订阅目标。
    ///
    /// 参数说明：
    /// - `value`: 下游订阅索引使用的名称。
    ///
    /// 返回：保存目标后的构造器，重复调用覆盖旧值；不证明订阅成员有权限接收该业务数据。
    /// 与用户列表同时设置时，最终发送校验拒绝。
    pub fn group(mut self, value: impl Into<String>) -> Self {
        self.group = Some(value.into());
        self
    }

    /// 业务作用：追加一个排除用户。
    ///
    /// 参数说明：
    /// - `value`: 要从已有目标候选中排除的用户标识。
    ///
    /// 返回：追加排除条件后的构造器；只缩小候选范围，不建立新的目标，也不去除重复条件。
    pub fn exclude_uid(mut self, value: impl Into<String>) -> Self {
        self.excludes.push(value.into());
        self
    }

    /// 业务作用：设置来源用户。
    ///
    /// 参数说明：
    /// - `value`: 由业务调用方确认的发送用户标识。
    ///
    /// 返回：覆盖来源用户后的构造器；该字段会写入信封元数据，不认证用户，也不自动排除其连接。
    pub fn from_uid(mut self, value: impl Into<String>) -> Self {
        self.from_uid = Some(value.into());
        self
    }

    /// 业务作用：设置需要排除的来源 session/client。
    ///
    /// 参数说明：
    /// - `value`: 下游应跳过的来源会话 ID。
    ///
    /// 返回：覆盖来源会话后的构造器；由下游按会话 ID 排除回显，不自动扩展为排除同一用户全部连接。
    pub fn from_client(mut self, value: impl Into<String>) -> Self {
        self.from_client = Some(value.into());
        self
    }

    /// 业务作用：追加一个目标逻辑节点。
    ///
    /// 参数说明：
    /// - `value`: 允许执行本次本地投递的逻辑节点名称。
    ///
    /// 返回：追加节点过滤条件后的构造器；不改变 Kafka topic 或分区选择，也不替代用户、端点或订阅目标。
    pub fn target_node(mut self, value: impl Into<String>) -> Self {
        self.target_nodes.push(value.into());
        self
    }

    /// 业务作用：设置受控集群来源身份。
    ///
    /// 参数说明：
    ///
    /// - `node`: 来源逻辑节点。
    /// - `incarnation`: 来源进程的非零实例标识；是否满足集群代际约束由调用方保证。
    ///
    /// 返回：同时覆盖来源节点和实例标识的构造器；最终发送会检查来源策略与非零约束，不验证节点真实性。
    pub fn source_identity(mut self, node: impl Into<String>, incarnation: u128) -> Self {
        self.source_identity = Some((node.into(), incarnation));
        self
    }

    /// 业务作用：选择一个启动期预注册 sink。
    ///
    /// 参数说明：
    /// - `value`: 消费端本地 Sender 白名单中的标识。
    ///
    /// 返回：覆盖 sink 选择后的构造器；发送端不查询白名单，消费端无法解析时按其拒绝策略处理。
    pub fn sink_id(mut self, value: impl Into<String>) -> Self {
        self.sink_id = Some(value.into());
        self
    }

    /// 业务作用：选择 allowlist 内的原生消息编码模式。
    ///
    /// 参数说明：
    /// - `value`: 下游构造消息信封时使用的模式，发送时须在配置允许范围内。
    ///
    /// 返回：覆盖模式后的构造器；不转码原始 payload，非法或未支持模式在最终校验时拒绝。
    pub fn message_mode(mut self, value: Mode) -> Self {
        self.message_mode = value;
        self
    }

    /// 业务作用：设置 producer 分区 key。
    ///
    /// 参数说明：
    /// - `value`: 交给 Kafka 生产通道的记录键。
    ///
    /// 返回：覆盖分区键后的构造器；键不作为路由 header，也不自行建立跨分区的事件顺序保证。
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }

    /// 业务作用：校验并发布 raw value，等待 broker delivery。
    ///
    /// 参数说明: 无。
    ///
    /// # 错误
    ///
    /// 路由组合、容量、mode 或来源策略非法，或底层发布失败时返回 nafka 错误。
    ///
    /// 返回：当前生产通道的 broker 投递报告；校验、准入或投递失败返回对应错误。
    /// 成功不表示客户端已经收到；原生生产者接管后的等待超时不证明消息未被 broker 接收。
    pub async fn send(self) -> nafka::Result<Delivery> {
        self.raw_builder()?.send().await
    }

    /// 业务作用：校验并把 raw value 登记到有界异步投递观察队列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回成功只表示消息已进入 producer 且 delivery observer 已登记，不表示 broker 已确认。
    ///
    /// # 错误
    ///
    /// 路由组合、容量、mode、准入或同步入队失败时返回 nafka 错误。
    ///
    /// 返回：同步校验及生产者接纳完成时为 `Ok(())`；否则返回错误。
    /// 后续投递由 nafka 观察机制跟踪，调用方不能把本次成功当成 broker 或客户端确认。
    pub fn fire(self) -> nafka::Result<()> {
        self.raw_builder()?.fire()
    }

    /// 业务作用：校验路由并构造只借用最终 payload 的 raw builder。
    ///
    /// 参数说明: 无。
    ///
    /// # 错误
    ///
    /// 路由组合、容量、mode 或来源策略非法时返回错误。
    ///
    /// 返回：借用原 payload、携带固定事件和已复制路由 header 的底层构造器；不执行发送。
    /// topic、生产者准入及完整记录限制仍由底层发布入口检查。
    fn raw_builder(&self) -> nafka::Result<RawPublishBuilder<'a>> {
        // 全部路由校验通过后才装配底层记录，防止半完成的目标或来源声明被提交。
        self.validate()?;
        let mut raw = self
            .publisher
            .lane
            .publish_raw(self.topic, self.payload)
            .event(NAWS_PUSH_EVENT);
        if let Some(key) = &self.key {
            raw = raw.key(key.clone());
        }
        raw = append_many(raw, HEADER_WS_EVENT, &self.events);
        raw = append_many(raw, HEADER_WS_ROUTER, &self.routers);
        raw = append_many(raw, HEADER_WS_UID, &self.uids);
        raw = append_many(raw, HEADER_WS_EXCLUDE_UID, &self.excludes);
        raw = append_many(raw, HEADER_WS_TARGET_NODE, &self.target_nodes);
        if let Some(value) = &self.group {
            raw = raw.header(HEADER_WS_GROUP, Some(value.as_bytes()));
        }
        if let Some(value) = &self.from_uid {
            raw = raw.header(HEADER_WS_FROM_UID, Some(value.as_bytes()));
        }
        if let Some(value) = &self.from_client {
            raw = raw.header(HEADER_WS_FROM_CLIENT, Some(value.as_bytes()));
        }
        if let Some((node, incarnation)) = &self.source_identity {
            let incarnation = incarnation.to_string();
            raw = raw
                .header(HEADER_WS_SOURCE_NODE, Some(node.as_bytes()))
                .header(HEADER_WS_SOURCE_INCARNATION, Some(incarnation.as_bytes()));
        }
        if let Some(value) = &self.sink_id {
            raw = raw.header(HEADER_WS_SINK_ID, Some(value.as_bytes()));
        }
        Ok(raw
            .header(
                HEADER_WS_MESSAGE_MODE,
                Some(nafka::mode_wire_name(self.message_mode).as_bytes()),
            )
            .header(
                HEADER_WS_PAYLOAD_LAYOUT,
                Some(PAYLOAD_LAYOUT_RAW.as_bytes()),
            ))
    }

    /// 业务作用：校验 typed builder 的组合、文本和 header 容量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：事件、目标组合、来源策略、模式、文本和路由 header 预算均有效时成功；否则返回首个路由错误。
    /// 不裁剪字符串、不去重、不校验业务权限；topic、key、payload 和其它 Kafka header 不纳入此路由预算。
    fn validate(&self) -> nafka::Result<()> {
        // 缺失事件或目标不能退化为广播，业务含义不完整时禁止发布。
        if self.events.is_empty() {
            return Err(route_error("至少需要一个 client_event"));
        }
        if self.group.is_none() && self.uids.is_empty() && self.routers.is_empty() {
            return Err(route_error("目标 group、uid、router 不能全部为空"));
        }
        // 显式拒绝同时指定订阅和用户，避免消费端的目标优先规则掩盖调用方意图。
        if self.group.is_some() && !self.uids.is_empty() {
            return Err(route_error("typed builder 不允许 group 与 uid 同时设置"));
        }
        if self.message_mode == Mode::FastFixed
            || !self
                .publisher
                .config
                .allowed_message_modes
                .contains(&self.message_mode)
        {
            return Err(route_error("消息 mode 未在 allowlist"));
        }
        // 来源声明受发布器策略控制，普通发布入口不能自行获得集群来源身份。
        match self.publisher.config.source_identity_policy {
            SourceIdentityPolicy::Forbidden if self.source_identity.is_some() => {
                return Err(route_error("当前 publisher 禁止来源身份"));
            }
            SourceIdentityPolicy::Required if self.source_identity.is_none() => {
                return Err(route_error("当前 publisher 要求来源身份"));
            }
            _ => {}
        }
        if self
            .source_identity
            .as_ref()
            .is_some_and(|(_, incarnation)| *incarnation == 0)
        {
            return Err(route_error("来源 incarnation 必须非零"));
        }
        for value in self
            .events
            .iter()
            .chain(&self.routers)
            .chain(&self.uids)
            .chain(&self.excludes)
            .chain(&self.target_nodes)
            .chain(self.group.iter())
            .chain(self.from_uid.iter())
            .chain(self.from_client.iter())
            .chain(self.sink_id.iter())
            .chain(self.source_identity.iter().map(|(node, _)| node))
        {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(route_error("路由值为空或包含控制字符"));
            }
        }

        let singles = usize::from(self.group.is_some())
            + usize::from(self.from_uid.is_some())
            + usize::from(self.from_client.is_some())
            + usize::from(self.sink_id.is_some())
            + self.source_identity.as_ref().map_or(0, |_| 2)
            + 2;
        let count = self
            .events
            .len()
            .checked_add(self.routers.len())
            .and_then(|value| value.checked_add(self.uids.len()))
            .and_then(|value| value.checked_add(self.excludes.len()))
            .and_then(|value| value.checked_add(self.target_nodes.len()))
            .and_then(|value| value.checked_add(singles))
            .ok_or_else(|| route_error("路由 header 数量溢出"))?;
        let bytes = self.estimated_header_bytes()?;
        // 路由元数据有独立预算，不能让许多小目标字段绕过单条记录的处理容量。
        if count > self.publisher.config.max_route_headers
            || bytes > self.publisher.config.max_route_header_bytes
        {
            return Err(route_error("路由 header 超过配置上限"));
        }
        Ok(())
    }

    /// 业务作用：计算本构造器将产生的路由 header 名称与值字节总量，供独立容量校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部路由字段、模式与布局 header 的 UTF-8 字节总和；任何累加溢出返回路由错误。
    /// 不计 Kafka 记录编码开销、topic、key、payload 或底层额外添加的事件与追踪 header。
    fn estimated_header_bytes(&self) -> nafka::Result<usize> {
        let mut total = 0_usize;
        for (name, values) in [
            (HEADER_WS_EVENT, &self.events),
            (HEADER_WS_ROUTER, &self.routers),
            (HEADER_WS_UID, &self.uids),
            (HEADER_WS_EXCLUDE_UID, &self.excludes),
            (HEADER_WS_TARGET_NODE, &self.target_nodes),
        ] {
            for value in values {
                total = checked_header_add(total, name, value.len())?;
            }
        }
        for (name, value) in [
            (HEADER_WS_GROUP, self.group.as_deref()),
            (HEADER_WS_FROM_UID, self.from_uid.as_deref()),
            (HEADER_WS_FROM_CLIENT, self.from_client.as_deref()),
            (HEADER_WS_SINK_ID, self.sink_id.as_deref()),
        ] {
            if let Some(value) = value {
                total = checked_header_add(total, name, value.len())?;
            }
        }
        if let Some((node, incarnation)) = &self.source_identity {
            total = checked_header_add(total, HEADER_WS_SOURCE_NODE, node.len())?;
            total = checked_header_add(
                total,
                HEADER_WS_SOURCE_INCARNATION,
                incarnation.to_string().len(),
            )?;
        }
        total = checked_header_add(
            total,
            HEADER_WS_MESSAGE_MODE,
            nafka::mode_wire_name(self.message_mode).len(),
        )?;
        checked_header_add(total, HEADER_WS_PAYLOAD_LAYOUT, PAYLOAD_LAYOUT_RAW.len())
    }
}

/// 业务作用：向 raw builder 逐项追加同名多值 header。
///
/// 参数说明：
///
/// - `builder`: 当前 raw builder。
/// - `name`: 固定 header 名。
/// - `values`: 已校验值列表。
///
/// 返回：按列表顺序追加同名 header 的构造器，值复制到其字段存储中；空列表保持不变，不去重或发送。
fn append_many<'a>(
    mut builder: RawPublishBuilder<'a>,
    name: &str,
    values: &[String],
) -> RawPublishBuilder<'a> {
    for value in values {
        builder = builder.header(name, Some(value.as_bytes()));
    }
    builder
}

/// 业务作用：checked 累加一条 header 的名和值长度。
///
/// 参数说明：
///
/// - `total`: 已计入的路由 header 名和值字节总和。
/// - `name`: 本次追加的 header 名称。
/// - `value_len`: 本次 header 值的字节长度。
///
/// 返回：加入名称和值后的累计字节数；任何 usize 加法溢出返回路由错误，不以回绕值参与容量判断。
fn checked_header_add(total: usize, name: &str, value_len: usize) -> nafka::Result<usize> {
    total
        .checked_add(name.len())
        .and_then(|value| value.checked_add(value_len))
        .ok_or_else(|| route_error("路由 header 字节数溢出"))
}

/// 业务作用：构造不携带 header value 的路由错误。
///
/// 参数说明：
///
/// - `message`: 稳定安全诊断文本。
///
/// 返回：携带该文本的 `NafkaError::MalformedRoute`；不自行记录日志或过滤输入，调用方不得传入敏感字段值。
fn route_error(message: &str) -> NafkaError {
    NafkaError::MalformedRoute(message.into())
}
