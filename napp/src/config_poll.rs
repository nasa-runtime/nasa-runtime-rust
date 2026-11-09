//! 配置候选的周期补读与阻塞工作隔离；发布权威由宿主持有。

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};
use std::time::Duration;
use tokio::time::{Instant, Interval, MissedTickBehavior};

/// 未显式配置时，每秒补读一次配置来源。
pub const DEFAULT_CONFIG_POLL_INTERVAL_MS: u64 = 1_000;

/// 串行配置驱动使用的周期计时器，不拥有候选发布或业务切换权限。
pub struct ConfigPoller {
    timer: Interval,
}

impl ConfigPoller {
    /// 业务作用：在创建计时器前校验运维输入，避免零周期或时间溢出引发运行时异常。
    /// 参数说明：
    /// - `interval_ms`：两次周期检查之间的毫秒数。
    /// 返回：周期处于一毫秒至 365 天且可表示下一检查时间时返回；否则拒绝配置，不创建任务。
    pub fn validate_interval(interval_ms: u64) -> ApplicationResult<Duration> {
        let period = Duration::from_millis(interval_ms);
        if period.is_zero()
            || period > Duration::from_secs(365 * 24 * 60 * 60)
            || Instant::now().checked_add(period).is_none()
        {
            return Err(ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "configuration poll interval must be within 1 millisecond and 365 days",
            ));
        }
        Ok(period)
    }

    /// 业务作用：建立跳过积压检查的配置补读计时器。
    /// 参数说明：
    /// - `interval_ms`：已配置的补读周期，单位毫秒。
    /// 返回：首次检查立即就绪的计时器；非法周期返回错误。须在启用时间驱动的 Tokio 运行时内调用。
    pub fn new(interval_ms: u64) -> ApplicationResult<Self> {
        let mut timer = tokio::time::interval(Self::validate_interval(interval_ms)?);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Ok(Self { timer })
    }

    /// 业务作用：等待下一次补读机会，读取慢于周期时不集中补发历史检查。
    /// 参数说明：无。
    /// 返回：本次检查的计划时刻；取消等待不会消费尚未完成的检查。
    pub async fn tick(&mut self) -> Instant {
        self.timer.tick().await
    }

    /// 业务作用：仅在候选成功提交后更新补读节奏，重复设置同值不延迟当前检查。
    /// 参数说明：
    /// - `interval_ms`：已提交配置的补读周期，单位毫秒。
    /// 返回：周期变更后从当前时刻等待一个完整周期；非法值保持原计时器并返回错误。
    pub fn set_interval(&mut self, interval_ms: u64) -> ApplicationResult<()> {
        let period = Self::validate_interval(interval_ms)?;
        if self.timer.period() != period {
            // 先完成全部校验再替换计时器，失败不得改变正在使用的检查节奏。
            let mut next = tokio::time::interval(period);
            next.set_missed_tick_behavior(MissedTickBehavior::Skip);
            next.reset();
            self.timer = next;
        }
        Ok(())
    }

    /// 业务作用：在阻塞池中构造配置候选，避免同步文件读取占用业务调度线程。
    /// 参数说明：
    /// - `load`：只允许加载、校验和准备候选，不得发布配置或修改正在使用的业务状态。
    /// 返回：读取完成后交还候选；线程异常返回错误。调用方须串行等待并在发布前复验取消与版本权威。
    /// 取消等待不能终止已开始的同步读取，因此闭包不得持有独立发布权限。
    pub async fn read<T, F>(&mut self, load: F) -> ApplicationResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        read_config_candidate(load).await
    }
}

/// 业务作用：为本地与配置中心候选隔离同步读取、资源准备和回收，与周期补读共享阻塞执行边界。
/// 参数说明：
/// - `load`：读取或准备候选、回收未安装或已退出资源的同步动作，不能持有业务配置发布权威。
/// 返回：同步动作结果或工作线程错误；调用方须串行等待并在提交前复验取消和来源权限。
/// 取消 future 不能终止已开始的文件读取，闭包不得安装业务资源或发布配置。
pub async fn read_config_candidate<T, F>(load: F) -> ApplicationResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(load).await.map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Config,
            ApplicationPhase::Running,
            "configuration candidate reader failed",
            error,
        )
    })
}
