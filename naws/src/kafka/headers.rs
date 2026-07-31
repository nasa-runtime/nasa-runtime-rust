//! 长连接数据面的固定 header 契约、配置与严格借用解析器。

use smallvec::SmallVec;

use crate::proto::Mode;

/// nafka 路由到本适配器的固定业务事件。
pub const NAWS_PUSH_EVENT: &str = "NAWS_PUSH";
/// 客户端事件名，多值。
pub const HEADER_WS_EVENT: &str = "X-Nasa-WS-Event";
/// endpoint/router，多值。
pub const HEADER_WS_ROUTER: &str = "X-Nasa-WS-Router";
/// 目标用户，多值。
pub const HEADER_WS_UID: &str = "X-Nasa-WS-Uid";
/// 目标群组，单值。
pub const HEADER_WS_GROUP: &str = "X-Nasa-WS-Group";
/// 排除用户，多值。
pub const HEADER_WS_EXCLUDE_UID: &str = "X-Nasa-WS-Exclude-Uid";
/// 来源用户，单值。
pub const HEADER_WS_FROM_UID: &str = "X-Nasa-WS-From-Uid";
/// 来源会话，单值。
pub const HEADER_WS_FROM_CLIENT: &str = "X-Nasa-WS-From-Client";
/// 来源节点，单值。
pub const HEADER_WS_SOURCE_NODE: &str = "X-Nasa-WS-Source-Node";
/// 来源节点 incarnation，单值。
pub const HEADER_WS_SOURCE_INCARNATION: &str = "X-Nasa-WS-Source-Incarnation";
/// 目标逻辑节点，多值。
pub const HEADER_WS_TARGET_NODE: &str = "X-Nasa-WS-Target-Node";
/// 预注册本地 Sender 标识，单值。
pub const HEADER_WS_SINK_ID: &str = "X-Nasa-WS-Sink-Id";
/// 原生消息编码模式，单值。
pub const HEADER_WS_MESSAGE_MODE: &str = "X-Nasa-WS-Message-Mode";
/// payload 布局，v1 只允许 raw。
pub const HEADER_WS_PAYLOAD_LAYOUT: &str = "X-Nasa-WS-Payload-Layout";
/// v1 唯一 payload 布局值。
pub const PAYLOAD_LAYOUT_RAW: &str = "raw";

/// fan-out 目标超过硬上限时的处理策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanoutOverflowPolicy {
    /// 暂停分区等待运维处理。
    Halt,
    /// 把原记录转入 DLT 后越过。
    DeadLetter,
}

/// 部分本地 outbox 无法入队时的交付策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// 任一目标丢弃或关闭都重试当前记录。
    RequireAllEnqueued,
    /// 允许明确记录部分丢弃后安全前移。
    BestEffort,
}

/// 数据面消费组模式。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupMode {
    /// 为当前进程实例派生独占广播组。
    EphemeralNode {
        /// 稳定业务 scope；运行时还会追加进程实例标识。
        scope: String,
    },
    /// 使用显式、按节点隔离的稳定消费组。
    Durable {
        /// 非空且不能等于顶层默认组的 group.id。
        group: String,
    },
}

/// 来源节点 header 的接受策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceIdentityPolicy {
    /// 普通业务 producer 不允许携带来源身份。
    Forbidden,
    /// 允许来源身份缺失；存在时必须成对且合法。
    Optional,
    /// 集群 relay 消息必须携带完整来源身份。
    Required,
}

/// 借用式数据面适配配置。
#[derive(Clone, Debug)]
pub struct PassthroughConfig {
    /// 单条记录最多接受的路由 header 数。
    pub max_route_headers: usize,
    /// 路由 header 名和值的总字节上限。
    pub max_route_header_bytes: usize,
    /// 单条消息最多展开的本地唯一 session 数。
    pub max_fanout_targets: usize,
    /// 同步 callback 超过该毫秒数时记录慢调用警告。
    pub callback_warn_ms: u64,
    /// fan-out 超限策略。
    pub fanout_overflow_policy: FanoutOverflowPolicy,
    /// outbox 部分失败策略。
    pub delivery_policy: DeliveryPolicy,
    /// 消息未携带 mode header 时采用的模式。
    pub default_message_mode: Mode,
    /// 允许从 header 选择的模式集合。
    pub allowed_message_modes: Vec<Mode>,
    /// 数据面消费组模式。
    pub group_mode: GroupMode,
    /// 来源身份策略。
    pub source_identity_policy: SourceIdentityPolicy,
}

impl Default for PassthroughConfig {
    /// 返回二进制少拷贝路径的安全默认值。
    fn default() -> Self {
        Self {
            max_route_headers: 256,
            max_route_header_bytes: 16 * 1024,
            max_fanout_targets: 10_000,
            callback_warn_ms: 50,
            fanout_overflow_policy: FanoutOverflowPolicy::Halt,
            delivery_policy: DeliveryPolicy::RequireAllEnqueued,
            default_message_mode: Mode::BitpackTlv,
            allowed_message_modes: vec![Mode::BitpackTlv, Mode::VarintTlv],
            group_mode: GroupMode::EphemeralNode {
                scope: "naws-push".into(),
            },
            source_identity_policy: SourceIdentityPolicy::Forbidden,
        }
    }
}

impl PassthroughConfig {
    /// 校验所有安全和容量不变量。
    ///
    /// # 错误
    ///
    /// 任一上限为零、默认 mode 不在 allowlist、开放未实现 mode 或 group 配置非法时返回文本。
    pub fn validate(&self) -> Result<(), String> {
        if self.max_route_headers == 0
            || self.max_route_header_bytes == 0
            || self.max_fanout_targets == 0
            || self.callback_warn_ms == 0
        {
            return Err("路由、fan-out 与 callback 上限必须大于零".into());
        }
        if self.allowed_message_modes.is_empty()
            || !self
                .allowed_message_modes
                .contains(&self.default_message_mode)
        {
            return Err("默认消息 mode 必须属于非空 allowlist".into());
        }
        if self.allowed_message_modes.contains(&Mode::FastFixed) {
            return Err("FAST_FIXED 尚未开放".into());
        }
        match &self.group_mode {
            GroupMode::EphemeralNode { scope } if scope.trim().is_empty() => {
                Err("ephemeral group scope 不能为空".into())
            }
            GroupMode::Durable { group } if group.trim().is_empty() || group == "default" => {
                Err("durable group 必须显式且不能为 default".into())
            }
            _ => Ok(()),
        }
    }
}

/// 已严格校验但仍借用原 header 字节的路由视图。
pub struct BorrowedWsRoute<'a> {
    /// 客户端事件列表。
    pub events: SmallVec<[&'a str; 2]>,
    /// endpoint/router 过滤列表。
    pub routers: SmallVec<[&'a str; 4]>,
    /// 目标用户列表。
    pub uids: SmallVec<[&'a str; 8]>,
    /// 排除用户列表。
    pub excludes: SmallVec<[&'a str; 4]>,
    /// 目标逻辑节点列表。
    pub target_nodes: SmallVec<[&'a str; 2]>,
    /// 可选目标群组。
    pub group: Option<&'a str>,
    /// 可选来源用户。
    pub from_uid: Option<&'a str>,
    /// 可选来源会话。
    pub from_client: Option<&'a str>,
    /// 可选来源逻辑节点。
    pub source_node: Option<&'a str>,
    /// 可选来源 incarnation。
    pub source_incarnation: Option<u128>,
    /// 可选预注册 sink id。
    pub sink_id: Option<&'a str>,
    /// 最终原生消息编码模式。
    pub message_mode: Mode,
}

/// 严格解析一条借用 header 视图。
///
/// # 参数
///
/// - `headers`: 消息原始有序 header。
/// - `config`: 解析和容量策略。
///
/// # 错误
///
/// 数量/字节超限、未知路由 header、null/空/非法 UTF-8、singleton 重复、来源身份不合规、
/// 空目标或不允许的 mode 都返回安全文本，且不包含 header value。
pub fn parse_route<'a>(
    headers: &nafka::KafkaHeadersRef<'a>,
    config: &PassthroughConfig,
) -> Result<BorrowedWsRoute<'a>, String> {
    let mut route = BorrowedWsRoute {
        events: SmallVec::new(),
        routers: SmallVec::new(),
        uids: SmallVec::new(),
        excludes: SmallVec::new(),
        target_nodes: SmallVec::new(),
        group: None,
        from_uid: None,
        from_client: None,
        source_node: None,
        source_incarnation: None,
        sink_id: None,
        message_mode: config.default_message_mode,
    };
    let mut route_count = 0_usize;
    let mut route_bytes = 0_usize;
    let mut source_incarnation_text = None;
    let mut layout = None;
    let mut explicit_mode = None;

    for header in headers.iter() {
        if !header.name.starts_with("X-Nasa-WS-") {
            continue;
        }
        route_count = route_count
            .checked_add(1)
            .ok_or_else(|| "路由 header 数量溢出".to_string())?;
        let value_len = header.value.map_or(0, <[u8]>::len);
        route_bytes = route_bytes
            .checked_add(header.name.len())
            .and_then(|total| total.checked_add(value_len))
            .ok_or_else(|| "路由 header 字节数溢出".to_string())?;
        if route_count > config.max_route_headers || route_bytes > config.max_route_header_bytes {
            return Err("路由 header 超过配置上限".into());
        }
        let value = header
            .value
            .ok_or_else(|| "路由 header 不允许 null".to_string())?;
        let text = valid_text(value)?;
        match header.name {
            HEADER_WS_EVENT => route.events.push(text),
            HEADER_WS_ROUTER => route.routers.push(text),
            HEADER_WS_UID => route.uids.push(text),
            HEADER_WS_EXCLUDE_UID => route.excludes.push(text),
            HEADER_WS_TARGET_NODE => route.target_nodes.push(text),
            HEADER_WS_GROUP => set_singleton(&mut route.group, text, HEADER_WS_GROUP)?,
            HEADER_WS_FROM_UID => set_singleton(&mut route.from_uid, text, HEADER_WS_FROM_UID)?,
            HEADER_WS_FROM_CLIENT => {
                set_singleton(&mut route.from_client, text, HEADER_WS_FROM_CLIENT)?
            }
            HEADER_WS_SOURCE_NODE => {
                set_singleton(&mut route.source_node, text, HEADER_WS_SOURCE_NODE)?
            }
            HEADER_WS_SOURCE_INCARNATION => set_singleton(
                &mut source_incarnation_text,
                text,
                HEADER_WS_SOURCE_INCARNATION,
            )?,
            HEADER_WS_SINK_ID => set_singleton(&mut route.sink_id, text, HEADER_WS_SINK_ID)?,
            HEADER_WS_MESSAGE_MODE => {
                set_singleton(&mut explicit_mode, text, HEADER_WS_MESSAGE_MODE)?
            }
            HEADER_WS_PAYLOAD_LAYOUT => set_singleton(&mut layout, text, HEADER_WS_PAYLOAD_LAYOUT)?,
            _ => return Err("出现未知 X-Nasa-WS-* header".into()),
        }
    }

    if route.events.is_empty() {
        return Err("至少需要一个客户端事件 header".into());
    }
    if route.group.is_none() && route.uids.is_empty() && route.routers.is_empty() {
        return Err("目标 group、uid、router 不能全部为空".into());
    }
    if layout.is_some_and(|value| value != PAYLOAD_LAYOUT_RAW) {
        return Err("v1 只接受 raw payload layout".into());
    }
    if route.source_node.is_some() != source_incarnation_text.is_some() {
        return Err("来源 node 与 incarnation 必须同时存在".into());
    }
    match config.source_identity_policy {
        SourceIdentityPolicy::Forbidden if route.source_node.is_some() => {
            return Err("当前 route 禁止来源身份 header".into())
        }
        SourceIdentityPolicy::Required if route.source_node.is_none() => {
            return Err("当前 route 缺少来源身份 header".into())
        }
        _ => {}
    }
    route.source_incarnation = match source_incarnation_text {
        Some(value) => Some(
            value
                .parse::<u128>()
                .ok()
                .filter(|value| *value != 0)
                .ok_or_else(|| "来源 incarnation 必须为非零十进制 u128".to_string())?,
        ),
        None => None,
    };
    if let Some(value) = explicit_mode {
        route.message_mode =
            nafka::mode_from_wire_name(value).ok_or_else(|| "消息 mode 未知".to_string())?;
    }
    if route.message_mode == Mode::FastFixed
        || !config.allowed_message_modes.contains(&route.message_mode)
    {
        return Err("消息 mode 未被当前 adapter 允许".into());
    }
    Ok(route)
}

/// 校验 header 值为非空、无控制字符的 UTF-8。
///
/// # 参数
///
/// - `value`: header 原始字节。
fn valid_text(value: &[u8]) -> Result<&str, String> {
    let text = std::str::from_utf8(value).map_err(|_| "路由 header 不是 UTF-8".to_string())?;
    if text.is_empty() || text.chars().any(char::is_control) {
        Err("路由 header 为空或包含控制字符".into())
    } else {
        Ok(text)
    }
}

/// 写入一个严格 singleton 字段。
///
/// # 参数
///
/// - `slot`: 目标字段。
/// - `value`: 已校验文本。
/// - `name`: 仅用于稳定错误分类的 header 名。
fn set_singleton<'a>(slot: &mut Option<&'a str>, value: &'a str, name: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        Err(format!("singleton header 重复: {name}"))
    } else {
        Ok(())
    }
}
