//! 组件配置热重应用协议。
//!
//! 候选先完成材料准备，再进入既有配置发布门禁安装。准备不得改变当前运行态；
//! 安装阶段不执行文件或网络 I/O，候选及旧资源由门禁外的 owner 回收。

use serde_json::Value;

use crate::{ApplicationResult, ComponentId};

/// 可热刷组件的候选准备入口，登记后由同一配置流水线调用。
#[cfg_attr(
    not(any(feature = "nacos-config", feature = "config-watch")),
    allow(dead_code)
)]
pub(crate) trait ConfigApplier: Send + Sync {
    /// 业务作用：声明本准备器负责的配置目标。
    /// 参数说明：无。
    /// 返回：用于匹配状态表的组件身份。
    fn component(&self) -> ComponentId;

    /// 业务作用：在发布锁外完成所有可失败的材料准备。
    /// 参数说明：`candidate` 为已经完成结构校验的原始候选。
    /// 返回：拥有未安装资源的候选，或不改变当前运行态的准备错误。
    fn prepare(&self, candidate: &Value) -> ApplicationResult<Box<dyn PreparedConfigApply>>;
}

/// 单个组件的已准备材料及被替换资源 owner。
#[cfg_attr(
    not(any(feature = "nacos-config", feature = "config-watch")),
    allow(dead_code)
)]
pub(crate) trait PreparedConfigApply: Send {
    /// 业务作用：在宿主发布门禁内安装候选，旧资源保留至门禁外回收。
    /// 参数说明：无。
    /// 返回：安装成功或保留当前运行态的拒绝原因；不得执行外部 I/O 或等待后台线程。
    fn install(&mut self) -> ApplicationResult<()>;
}
