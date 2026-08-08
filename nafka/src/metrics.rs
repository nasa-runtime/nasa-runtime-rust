//! 后端无关的 nafka 指标出口；运行时只发稳定名字与低基数字段。

/// 指标标签的借用视图；键是框架固定常量，值来自 lane/group/topic 等受控身份。
pub type MetricLabels<'a> = &'a [(&'static str, &'a str)];

/// 应用可注入的指标接收端。
///
/// 实现必须无阻塞、不可 panic；回调可能位于 producer 异步任务或 consumer owner 线程。
pub trait MetricsSink: Send + Sync + 'static {
    /// 业务作用：累加一个单调 counter。
    ///
    /// # 参数
    ///
    /// - `name`: nafka 定义的固定指标名。
    /// - `delta`: 本次增量，始终为正数。
    /// - `labels`: 固定低基数字段，不包含 payload、header value 或错误详情。
    fn counter(&self, name: &'static str, delta: u64, labels: MetricLabels<'_>);

    /// 业务作用：设置一个瞬时 gauge。
    ///
    /// # 参数
    ///
    /// - `name`: nafka 定义的固定指标名。
    /// - `value`: 当前观测值。
    /// - `labels`: 固定低基数字段。
    fn gauge(&self, name: &'static str, value: i64, labels: MetricLabels<'_>);
}

/// 未注入后端时使用的零开销接收端。
pub(crate) struct NoopMetrics;

impl MetricsSink for NoopMetrics {
    /// 业务作用：丢弃 counter。
    fn counter(&self, _name: &'static str, _delta: u64, _labels: MetricLabels<'_>) {}

    /// 业务作用：丢弃 gauge。
    fn gauge(&self, _name: &'static str, _value: i64, _labels: MetricLabels<'_>) {}
}
