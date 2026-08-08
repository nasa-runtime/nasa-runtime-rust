//! 分流决策指标:所有分流决策都要有 trace/debug 事件 + metrics 计数,
//! 否则线上排查「为什么走了 DNS / 为什么没走注册中心」很困难)。
//!
//! 轻量原子计数(无外部 metrics 依赖);`RestDiscovery::get().metrics()` 取快照。

use std::sync::atomic::{AtomicU64, Ordering};

/// 用字段名一次性生成:原子计数器结构 + `pub(crate)` 自增方法 + `snapshot()` + 快照结构。
macro_rules! rest_metrics {
    ($($field:ident => $doc:literal),* $(,)?) => {
        /// 各分流决策的累计计数(进程级)。
        #[derive(Debug, Default)]
        pub struct RestMetrics {
            $( $field: AtomicU64, )*
        }

	    impl RestMetrics {
	        $(
                /// 递增一次对应分流路径的进程级计数。
                ///
                /// 每次 REST 调用做出 DNS/注册中心/直连等决策时调用,用于后续排查路由选择原因。
	            #[doc = $doc]
	            pub(crate) fn $field(&self) {
                    self.$field.fetch_add(1, Ordering::Relaxed);
                }
            )*

            /// 业务作用：取当前各计数的快照。
            pub fn snapshot(&self) -> RestMetricsSnapshot {
                RestMetricsSnapshot {
                    $( $field: self.$field.load(Ordering::Relaxed), )*
                }
            }
        }

        /// [`RestMetrics::snapshot`] 的不可变快照。
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        #[non_exhaustive]
        pub struct RestMetricsSnapshot {
            $(
                #[doc = $doc]
                pub $field: u64,
            )*
        }
    };
}

rest_metrics! {
    explicit_service  => "档1 显式 service 调用次数",
    lb_scheme         => "档2 lb:// 调用次数",
    heuristic_hit     => "档3 裸 http 启发式命中索引(走 LB)次数",
    external          => "档3 外部直连(未命中/启发式关)次数",
    no_instance       => "内部服务无可用实例(NoAvailableInstance)次数",
    discovery_failure => "discover/watch 发现失败次数",
    watch_fallback    => "watch 不可用、降级 discover+TTL 次数(请求线程实际走 TTL 时计)",
    watch_receiver_closed => "watch receiver 后台关闭/pump 重建次数(与请求无关的事实源断开,排查 flapping 用)",
    retry_attempt     => "GET/HEAD 传输错误跨实例重试次数",
    bulkhead_rejected => "每服务 bulkhead 满载拒绝次数",
    circuit_open_rejected => "服务熔断器打开期间拒绝次数",
    outlier_ejected_skipped => "负载均衡前跳过仍在摘除窗口的实例次数",
    transport_error_refresh => "内部调用传输错误触发该 service 快速刷新/恢复次数",
}
