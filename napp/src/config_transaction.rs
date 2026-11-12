//! 已准备配置的同步提交和逆序补偿，不参与业务数据面锁竞争。

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 已通过校验与资源准备的配置变更；旧资源须保留到整个事务完成。
pub trait PreparedConfigChange: Send {
    /// 业务作用：在宿主发布门禁内切换已经准备的运行态，不执行文件或网络 I/O。
    ///
    /// 参数说明：无。
    ///
    /// 返回：切换完成时成功；失败后仍须保留足以恢复旧态的材料，不得自行释放旧资源。
    fn install(&mut self) -> ApplicationResult<()>;

    /// 业务作用：撤销本对象已尝试的切换，恢复事务开始前的有效运行态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：旧态恢复或尚未发生切换时成功；无法恢复时返回错误，宿主必须进入保护态。
    /// 本方法不得执行外部 I/O，且须能处理 install 返回错误或展开异常后的部分状态。
    fn rollback(&mut self) -> ApplicationResult<()>;
}

/// 配置事务未提交的原因及逆序补偿结果，不代表新配置已经发布。
#[derive(Debug)]
pub struct ConfigTransactionError {
    cause: ApplicationError,
    rollback_errors: Vec<ApplicationError>,
}

impl ConfigTransactionError {
    /// 业务作用：区分可继续使用旧配置的拒绝与需要宿主关闭业务准入的恢复失败。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有尝试切换的对象均恢复旧态时为真；任一补偿失败或展开异常时为假。
    pub fn rollback_complete(&self) -> bool {
        self.rollback_errors.is_empty()
    }
}

impl std::fmt::Display for ConfigTransactionError {
    /// 业务作用：输出不包含候选内容的事务拒绝摘要。
    ///
    /// 参数说明：
    /// - `formatter`：错误摘要的输出目标。
    ///
    /// 返回：写入事务是否恢复旧态；不输出文件正文或秘密材料。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "configuration transaction rejected; rollback_complete={}",
            self.rollback_complete()
        )
    }
}

impl std::error::Error for ConfigTransactionError {
    /// 业务作用：保留最初拒绝原因供宿主按其脱敏策略归类。
    ///
    /// 参数说明：无。
    ///
    /// 返回：最初安装错误；补偿是否完整通过独立状态判断。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// 业务作用：按准备顺序提交整批配置，任一步拒绝时对已尝试对象逆序恢复旧态。
///
/// 参数说明：
/// - `changes`：已经全部完成校验和资源准备的变更；调用期间宿主须持有同一配置发布权威。
///
/// 返回：全部安装完成时成功，随后宿主须无失败地发布配置视图；失败时不允许发布候选。
/// 补偿继续尝试所有对象，即使其中一个恢复失败。旧资源和候选须在宿主发布锁外释放。
pub fn apply_config_transaction(
    changes: &mut [Box<dyn PreparedConfigChange>],
) -> Result<(), ConfigTransactionError> {
    for index in 0..changes.len() {
        let installed = guarded_change(|| changes[index].install());
        if let Err(cause) = installed {
            let mut rollback_errors = Vec::new();
            // 失败对象也可能已发生部分切换；先撤销它，再恢复更早安装的对象。
            for change in changes[..=index].iter_mut().rev() {
                if let Err(error) = guarded_change(|| change.rollback()) {
                    rollback_errors.push(error);
                }
            }
            return Err(ConfigTransactionError {
                cause,
                rollback_errors,
            });
        }
    }
    Ok(())
}

/// 业务作用：将适配器的展开异常转为可补偿的事务拒绝，不泄露异常载荷中的配置内容。
///
/// 参数说明：
/// - `action`：单个对象的同步安装或恢复动作。
///
/// 返回：动作结果；展开异常转为固定错误，进程终止类故障不在恢复范围内。
fn guarded_change(action: impl FnOnce() -> ApplicationResult<()>) -> ApplicationResult<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).unwrap_or_else(|_| {
        Err(ApplicationError::new(
            ComponentId::Config,
            ApplicationPhase::Running,
            "configuration adapter unwound during transaction",
        ))
    })
}
