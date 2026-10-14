//! 逐分区控制意图和结果；报告不构成跨 owner 事务或持续分区租约。

use crate::{consumer::supervisor::OwnerCommandKind, NafkaError, Result, Tp};
use std::collections::BTreeMap;

/// 一个目标分区上的显式控制意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionControl {
    /// 增加用户暂停原因，其它保护原因保持有效。
    Pause,
    /// 仅解除用户暂停原因。
    Resume,
    /// 定位到非负绝对 offset。
    Seek(i64),
    /// 定位到当前保留起点。
    Beginning,
    /// 定位到当前可读末尾。
    End,
    /// 定位到首条不早于非负 Unix 毫秒时间的记录，无匹配时取末尾。
    Timestamp(i64),
    /// 定位到 broker 已确认位点；缺少历史位点时拒绝。
    Committed,
}

impl PartitionControl {
    /// 业务作用：在派发任何目标前验证显式定位意图。
    /// 参数说明：tp 为目标分区。
    /// 返回：合法时成功，非法声明返回配置错误且不执行控制。
    pub(crate) fn validate(&self, tp: &Tp) -> Result<()> {
        if tp.topic.trim().is_empty()
            || tp.partition < 0
            || matches!(self, Self::Seek(value) | Self::Timestamp(value) if *value < 0)
        {
            return Err(NafkaError::Config("控制目标和定位值必须有效且非负".into()));
        }
        Ok(())
    }

    /// 业务作用：将单目标意图送入已有 owner 权威和责任门禁。
    /// 参数说明：tp 为经过验证的目标。
    /// 返回：只影响该目标的命令，空集合不能隐式扩展为全部分区。
    pub(crate) fn command(&self, tp: Tp) -> OwnerCommandKind {
        match self {
            Self::Pause => OwnerCommandKind::Pause { tps: vec![tp] },
            Self::Resume => OwnerCommandKind::Resume { tps: vec![tp] },
            Self::Seek(value) => OwnerCommandKind::Seek {
                offsets: BTreeMap::from([(tp, *value)]),
            },
            Self::Beginning => OwnerCommandKind::SeekBeginning { tps: vec![tp] },
            Self::End => OwnerCommandKind::SeekEnd { tps: vec![tp] },
            Self::Timestamp(value) => OwnerCommandKind::SeekTimestamp {
                times: BTreeMap::from([(tp, *value)]),
            },
            Self::Committed => OwnerCommandKind::SeekCommitted { tps: vec![tp] },
        }
    }
}

/// 截止报告生成时该目标的执行证据。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlOutcome {
    /// owner 已确认操作完成；随后仍可能发生 rebalance。
    Applied,
    /// 没有派发，或 owner 在副作用前拒绝/取消。
    NotApplied,
    /// 已可能产生副作用，未获得完整确认，必须查询后决定下一步。
    Unknown,
    /// 目标没有唯一 owner 或冻结代次已失效，操作未执行。
    OwnerChanged,
}

/// 一次控制调用的独立快照，不随晚到命令结果改变。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlReport {
    /// 本次调用绑定的逻辑数据源。
    pub source_id: String,
    /// 最终消费组名称。
    pub group: String,
    /// 每个请求目标的执行结局。
    pub outcomes: BTreeMap<Tp, ControlOutcome>,
}

impl ControlReport {
    /// 业务作用：检查非空控制请求是否逐目标获得成功证据。
    /// 参数说明：无。
    /// 返回：全部 Applied 时为 true；空请求及任一非成功结果为 false。
    pub fn all_applied(&self) -> bool {
        !self.outcomes.is_empty()
            && self
                .outcomes
                .values()
                .all(|value| *value == ControlOutcome::Applied)
    }
}
