//! 公共基础类型:自有轻量结构替代底层客户端类型,保证 rdkafka 不出现在 pub API。

use crate::error::Result;
use crate::health::GroupHealth;

/// topic + partition 二元组,消费/控制/查询 API 的分区定位单位。
///
/// 使用自有类型而非底层客户端的 TopicPartition:公共 API 红线,
/// 同时让 BTreeMap 键有稳定排序(topic 字典序 → partition 升序),日志与报错输出可复现。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tp {
    /// Kafka topic 名,非空。
    pub topic: String,
    /// 分区号,来自 broker 元数据,>= 0。
    pub partition: i32,
}

impl Tp {
    /// 业务作用：构造分区定位。
    ///
    /// # 参数
    /// - `topic`: Kafka topic 名,调用方保证非空(空值在使用处的 validate 拦截,此处不重复校验)。
    /// - `partition`: 分区号,>= 0。
    ///
    /// 返回：只保存身份的分区值，实际使用入口继续校验合法范围。
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// 单个 Kafka header:名字 + 可空值。
///
/// value 用 `Option<Vec<u8>>` 而非 `Vec<u8>`:Kafka 协议允许 header 值为 null,
/// null 与空字节串语义不同,折叠会丢失生产端意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaHeader {
    /// header 名,大小写敏感(Kafka 协议语义)。
    pub name: String,
    /// header 值;None = Kafka null,Some(空) = 零长度值,两者必须可区分。
    pub value: Option<Vec<u8>>,
}

/// 有序多值 header 集合。
///
/// 为什么不用 Map:Kafka header 是有序列表,同名可重复(socket 路由的 Uid/Router 就靠
/// 重复 header 表达多值),Map 会丢重复项与跨名相对顺序。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KafkaHeaders(std::sync::Arc<Vec<KafkaHeader>>);

impl KafkaHeaders {
    /// 业务作用：从有序 header 列表构造;保留输入顺序与重复项。
    ///
    /// # 参数
    /// - `headers`: 按 wire 出现顺序排列的 header 列表。
    ///
    /// 返回：接管原列表所有权的头集合，不合并重复名称或 null 值。
    pub fn from_vec(headers: Vec<KafkaHeader>) -> Self {
        Self(std::sync::Arc::new(headers))
    }

    /// 业务作用：按 wire 顺序遍历全部 header(含重复名)。
    /// 参数说明：无。
    /// 返回：借用只读迭代器，不复制字节或转成无序映射。
    pub fn iter(&self) -> impl Iterator<Item = &KafkaHeader> {
        self.0.iter()
    }

    /// 业务作用：取指定名字的最后一个 header。
    ///
    /// 框架保留 header(X-Nasa-Event 等)按 Kafka 惯例采用 last-header 语义读取。
    ///
    /// # 参数
    /// - `name`: header 名,大小写敏感精确匹配。
    ///
    /// 返回：最后一个匹配项的借用，缺失时为 None；不执行 singleton 合同校验。
    pub fn last(&self, name: &str) -> Option<&KafkaHeader> {
        self.0.iter().rev().find(|h| h.name == name)
    }

    /// 业务作用：按 wire 顺序遍历指定名字的全部 header。
    ///
    /// 多值路由 header(如 X-Nasa-WS-Uid)必须用本方法读全量；[`Self::last`] 会丢目标。
    ///
    /// # 参数
    /// - `name`: header 名,大小写敏感精确匹配。
    ///
    /// 返回：保留线路相对顺序的全部匹配项，缺失时为空迭代器。
    pub fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a KafkaHeader> + 'a {
        self.0.iter().filter(move |h| h.name == name)
    }

    /// 业务作用：header 总条数(含重复名),用于路由数量上限校验。
    /// 参数说明：无。
    /// 返回：完整条目数，不按名称去重。
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 业务作用：是否没有任何 header。
    /// 参数说明：无。
    /// 返回：只有条目集合为空时为 true，null 值条目仍属于非空集合。
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 业务作用：由 consumer owner 覆盖框架生成的可信 header，消除来源侧伪造的同名值。
    ///
    /// 参数说明：
    /// - `name`: 框架保留 header 名。
    /// - `value`: owner 根据本地状态生成的可信值。
    ///
    /// 返回：无；删除全部同名旧值后在末尾追加唯一可信值。
    pub(crate) fn replace_framework_value(&mut self, name: &str, value: Vec<u8>) {
        // 业务上下文可共享只读头；更新可信次数时保留其它持有者的原快照，不能原地改写。
        let headers = std::sync::Arc::make_mut(&mut self.0);
        headers.retain(|header| !header.name.eq_ignore_ascii_case(name));
        headers.push(KafkaHeader {
            name: name.to_owned(),
            value: Some(value),
        });
    }
}

/// 发布成功后的 broker 落点,send()/delivery 路径的返回值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// 消息实际落入的分区。
    pub partition: i32,
    /// 消息在该分区的 offset。
    pub offset: i64,
}

/// 某 group 在单个分区上的消费滞后快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaPartitionLag {
    /// topic 名。
    pub topic: String,
    /// 分区号。
    pub partition: i32,
    /// 该 group 已提交的 next offset;None = 该分区从未提交过。
    pub committed: Option<i64>,
    /// 分区末尾 offset(下一条待写入位置)。
    pub end: i64,
    /// end - committed(>=0);committed 为 None 时无法计算,同为 None。
    pub lag: Option<i64>,
}

/// assign 固定分区消费的首次起点策略；同一 owner 自动或显式重建使用已保存的绝对恢复坐标。
/// 已确认提交推进恢复点，显式 seek 成功则替换它；内部重读不改变恢复责任。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartOffset {
    /// 从分区最早可用记录开始(受 retention 影响,不是历史第一条)。
    Earliest,
    /// 从分区末尾开始,只消费启动后的新消息。
    Latest,
    /// 从第一条 timestamp >= 该毫秒时间戳的记录开始;查不到时跳到末尾。
    Timestamp(i64),
    /// 从显式 offset 开始;越界行为交给 auto_offset_reset。
    Offset(i64),
    /// 沿用 group 已提交位点;无提交历史的分区交给 auto_offset_reset。
    Committed,
}

/// assign 消费任务句柄:查询与停止的最小控制面。
///
/// stop 幂等;句柄 Drop 不隐式停止消费(显式生命周期原则)。
pub struct AssignmentHandle {
    pub(crate) group: String,
    /// 所属运行时，用于复用统一 owner 控制面。
    pub(crate) proxy: crate::KafkaProxy,
    /// 创建时的 owner 身份，防止同名 group 重建后被旧句柄控制。
    owner: std::sync::Arc<crate::consumer::supervisor::GroupHandle>,
}

impl AssignmentHandle {
    /// 业务作用：构造固定分区任务句柄。
    ///
    /// # 参数
    ///
    /// - `proxy`: 所属运行时。
    /// - `group`: 独占 group.id。
    /// - `owner`: 创建本任务时的实例身份。
    ///
    /// 返回：不具有自动停止副作用的句柄。
    pub(crate) fn new(
        proxy: crate::KafkaProxy,
        group: String,
        owner: std::sync::Arc<crate::consumer::supervisor::GroupHandle>,
    ) -> Self {
        Self {
            group,
            proxy,
            owner,
        }
    }

    /// 业务作用：在发出控制命令前确认句柄仍对应已登记实例。
    /// 参数说明：无。
    /// 返回：身份相同则成功；已停止或被替换返回生命周期错误。
    fn require_current(&self) -> Result<()> {
        let current = self.proxy.group_handle(&self.group)?;
        if !std::sync::Arc::ptr_eq(&current, &self.owner) {
            return Err(crate::error::NafkaError::Lifecycle(
                "assignment handle 已过期".into(),
            ));
        }
        Ok(())
    }

    /// 业务作用：本 assign 任务占用的 group.id(与 subscribe group 共用命名空间,冲突在 assign 时拒绝)。
    /// 参数说明：无。
    /// 返回：创建时固定的组名；该值不代表句柄仍持有活动 owner。
    pub fn group(&self) -> &str {
        &self.group
    }

    /// 业务作用：当前固定分区集合快照。
    /// 参数说明：无。
    /// 返回：本句柄 owner 的当前分配；实例已退出、被替换或控制失败时返回错误。
    pub async fn partitions(&self) -> Result<Vec<Tp>> {
        self.require_current()?;
        match self
            .owner
            .command(crate::consumer::supervisor::OwnerCommandKind::Assignment)
            .await?
        {
            crate::consumer::supervisor::CommandReply::Assignment(values) => Ok(values),
            _ => Err(crate::error::NafkaError::Lifecycle(
                "assignment 返回类型不匹配".into(),
            )),
        }
    }

    /// 业务作用：查询本 assign group 的健康快照。
    /// 参数说明：无。
    /// 返回：本实例的即时观测；注册身份已失效时返回错误，快照不授予提交权威。
    pub async fn health(&self) -> Result<GroupHealth> {
        self.require_current()?;
        Ok(self.owner.health())
    }

    /// 业务作用：停止本 assign 消费并注销 group;幂等,重复调用直接返回 Ok。
    /// 参数说明：无。
    /// 返回：仅本句柄的 owner 退出完成时成功；超时保留登记，同名新 owner 不受影响。
    pub async fn stop(&self) -> Result<()> {
        self.proxy
            .stop_group_instance(&self.group, &self.owner)
            .await
    }
}
