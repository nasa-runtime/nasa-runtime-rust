//! Catalog 快照的本地执行资格；动态目录必须持有尚未到期的确认租约。

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

enum Authority {
    Static,
    Unconfirmed,
    Leased(Instant),
}

pub(super) struct CatalogAuthority {
    state: Mutex<(Authority, Arc<()>)>,
    security_source: OnceLock<Arc<dyn CatalogSecurityEpoch>>,
    security_epoch: Mutex<Option<String>>,
}

/// 业务作用：让业务执行资格在配置原子发布或凭据窗口到期时立即失效，无需等待 watcher 调度。
pub(super) trait CatalogSecurityEpoch: Send + Sync {
    /// 业务作用：取得当前安全材料与有效信任集合的不可逆身份。
    /// 参数说明：无。
    /// 返回：完整材料有效时返回稳定摘要；材料缺失时返回空值，不能授予执行权。
    fn epoch(&self) -> Option<String>;
}

/// 一次业务操作冻结的资格；续租不能延长该操作，撤销后也不能借新快照恢复旧操作。
pub(super) struct CatalogPermit<'a> {
    authority: &'a CatalogAuthority,
    epoch: Arc<()>,
    deadline: Option<Instant>,
    security_epoch: Option<String>,
}

impl CatalogPermit<'_> {
    /// 业务作用：在异步等待后复验同一次快照资格，阻止旧操作跨越撤销或到期边界。
    /// 参数说明: 无。
    /// 返回：原期限未到且资格从未撤销时为真；后续续租或重新确认不能复活失效操作。
    pub(super) fn is_valid(&self) -> bool {
        let state = self
            .authority
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.deadline
            .is_none_or(|deadline| Instant::now() < deadline)
            && Arc::ptr_eq(&state.1, &self.epoch)
            && self
                .authority
                .security_matches(self.security_epoch.as_deref())
            && match state.0 {
                Authority::Static => true,
                Authority::Unconfirmed => false,
                Authority::Leased(deadline) => Instant::now() < deadline,
            }
    }
}

impl CatalogAuthority {
    /// 业务作用：为不依赖动态 Catalog 的计划建立静态资格，动态计划须在发布前撤销它。
    /// 参数说明：无。
    /// 返回：静态计划可用、尚未持有动态租约的资格容器。
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new((Authority::Static, Arc::new(()))),
            security_source: OnceLock::new(),
            security_epoch: Mutex::new(None),
        }
    }

    /// 业务作用：关闭动态快照的执行资格，阻止未确认或代际切换中的副本处理新工作。
    /// 参数说明：无。
    /// 返回：后续读取均拒绝，直到完整快照取得新的有效确认。
    pub(super) fn revoke(&self) {
        // 同时更换撤销身份，确保在途操作不能借后续快照确认重新获得执行权。
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            (Authority::Unconfirmed, Arc::new(()));
    }

    /// 业务作用：把已完成 Catalog 确认的安全材料身份与租约一起发布，拒绝等待期间发生的凭据切换。
    /// 参数说明：`deadline` 是数据库确认期限，`epoch` 是开始该轮校验时固定的安全合同摘要。
    /// 返回：期限有效且安全合同仍为当前值时开放；否则保持关闭。
    pub(super) fn confirm_with_security(&self, deadline: Instant, epoch: Option<String>) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 确认事务之后的探测和收敛等待也消耗租期，不能短暂发布已失效的资格。
        if deadline <= Instant::now() || !self.security_matches(epoch.as_deref()) {
            *state = (Authority::Unconfirmed, Arc::new(()));
            return false;
        }
        *self
            .security_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = epoch;
        state.0 = Authority::Leased(deadline);
        true
    }

    /// 业务作用：在动态 watcher 首次授予资格前绑定安全材料来源，重复绑定不能替换控制权威。
    /// 参数说明：`source` 是固定配置引用对应的动态安全快照来源。
    /// 返回：首次绑定成功，已绑定时返回 false。
    pub(super) fn bind_security(&self, source: Arc<dyn CatalogSecurityEpoch>) -> bool {
        self.security_source.set(source).is_ok()
    }

    /// 业务作用：比较操作固定的安全身份与当前材料，配置发布和旧凭据到期都不能延长旧执行权。
    /// 参数说明：`epoch` 是 Catalog 确认时使用的安全摘要。
    /// 返回：无动态来源的静态计划或精确命中的动态材料返回 true，其余拒绝。
    fn security_matches(&self, epoch: Option<&str>) -> bool {
        match self.security_source.get() {
            None => epoch.is_none(),
            Some(source) => source
                .epoch()
                .as_deref()
                .is_some_and(|current| Some(current) == epoch),
        }
    }

    /// 业务作用：在实际业务入口独立复验执行资格，不依赖 watcher 能否继续调度。
    /// 参数说明：无。
    /// 返回：静态计划或未过期的动态确认允许执行；未确认或过期时拒绝。
    pub(super) fn is_authoritative(&self) -> bool {
        self.permit().is_some()
    }

    /// 业务作用：在读取受管快照前冻结本次执行期限与撤销身份。
    /// 参数说明: 无。
    /// 返回：当前资格有效时返回有界凭据；未确认或过期时不给予执行权。
    pub(super) fn permit(&self) -> Option<CatalogPermit<'_>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = match state.0 {
            Authority::Static => None,
            Authority::Leased(deadline) if Instant::now() < deadline => Some(deadline),
            // 缺少有效确认时不得发放凭据，即使 watcher 尚未更新整体 readiness。
            _ => return None,
        };
        let security_epoch = self
            .security_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // 同代凭据必须在每次发放权限时仍然成立，watcher 暂停不能保留旧材料的运行权。
        if !self.security_matches(security_epoch.as_deref()) {
            return None;
        }
        Some(CatalogPermit {
            authority: self,
            epoch: Arc::clone(&state.1),
            deadline,
            security_epoch,
        })
    }
}
