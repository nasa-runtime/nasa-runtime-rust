# nametrics-core

`nametrics-core` 是进程级 provider-neutral 指标核心：统一 descriptor catalog、冲突审计、进程内记录、
结构化快照和 Prometheus 文本导出。它不依赖 Web、应用运行时或具体遥测 SDK。

## 核心价值

指标生产者只负责声明低基数 descriptor 并记录领域事实；`nametrics-core` 负责整个进程内名称唯一性、
类型/label 冲突、最坏公开序列预算和快照一致性。Prometheus 与 OTLP adapter 因而读取同一份已验证
事实，不会各自解释或重新拥有领域指标语义。

| 领域组件负责 | `nametrics-core` 负责 | 宿主 adapter 负责 |
| --- | --- | --- |
| 指标名称、类型、固定 label 形状与业务含义 | 唯一 descriptor catalog、冲突拒绝和记录 cell | scrape 路由、OTLP endpoint、导出间隔和传输重试 |
| label 值域封口与最坏序列数 | 原生 cell 与兼容源预留的进程级原子预算 | 把同一结构化快照编码为目标协议 |
| 在业务状态提交后记录累计事实 | 样本形状校验、拒绝计数与共享快照 | listener、鉴权、网络和停机 flush |

## 运行架构

```text
原生 recorder ───────────────┐
                            ├─> MetricHub ─> 结构化 snapshot ─> Prometheus / OTLP adapter
封口后的兼容 source + 预留 ─┘       └─> 固定原因的拒绝诊断
```

`MetricHub::register` 只审计并登记 descriptor，原生记录入口首次遇到具体 label 组合时才创建受预算
管理的 cell；
`register_legacy_source_reserved` 把 descriptor、结构化 source 与最坏公开序列数作为一个事务式登记动作。
任一 descriptor 冲突或容量不足时，catalog、source 和预留计数都保持原状。`register_legacy_source` 只为
无法声明最坏值域的旧源保留，不承诺该源一定获得容量；其结构化快照仍受进程硬限约束。固定值域组件
应使用显式预留，避免被动态余量拒绝。

标准 `nasa::application` 观测组合自动登记已编译的接口、Mapper、连接池和通知指标源，业务不需要
调用 `register_metrics_source`。该 API 只用于应用自定义领域源或独立宿主的显式集成：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "grafana", "web"] }
```

```rust
#[nasa::application("web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    // 自定义领域源由业务声明；框架内建源无需重复登记。
    let _ = app;
    Ok(())
}
```

底层组件可为自己的静态指标族声明 `MetricDescriptor`，再在同一个 `MetricHub` 注册。相同名称但
kind、label、unit、help 或 histogram bounds 不一致时会返回 `MetricConflict`，不能静默合并。

原生 cell 与结构化兼容源按最终公开序列共享 100,000 的硬限；目录自身的拒绝诊断先占位。组件必须在绑定
端口或发布 Ready 前完成登记，不能等首个样本到达后静默丢弃。预留按最终公开序列计算：普通 family
展开全部固定 label 组合，Prometheus histogram 的每个组合还要计算全部 `bucket`、`sum` 和 `count`。

`reserved_series()` 只返回内部诊断与显式源的最坏预留；出口容量检查使用
`committed_series()`，在同一锁定视图中再计入检查时已有的原生 cell 展开占用。
重复记录同一 label 组合不重复占位；initializer 创建的 histogram 同样包含有限桶、`+Inf`、sum 和 count。
此值不预测检查后新增的 label，也不包含无预留兼容源；调用方仍需约束这些动态来源。

未声明静态上限的结构化兼容源只使用原生序列和全部显式预留之外的余量。快照按源登记顺序、源内
family/label 顺序接纳完整样本，超额样本计入 `nametrics_samples_rejected_total{source="legacy",reason="cardinality_limit"}`；
显式预留的源也不能超过自身预算。返回的结构化快照及其文本、OTLP 出口使用同一门禁。
兼容源内部 registry、创建快照前的分配及仅文本旧源的自渲染不受此门禁管理，仍须由源自行限制。

## 领域原子采集

`atomic` 模块提供固定容量 `AtomicHistogram` 与饱和计数操作。领域组件可在静态单元内直接记录，
完成路径不构造标签、不分配桶、不写入 MetricHub；source 快照时再转换为统一 `MetricValue`。
数据库与等待延迟采用公开固定秒桶，流生命周期采用独立固定秒桶，不能通过实例配置改变，
以保证跨进程聚合含义一致。并发快照保证 count 等于同份桶之和，sum 是同期近似读取。

## YML 配置

本 crate 不读取 yml，也不启动 scrape endpoint。`napp` 观测组件负责统一指标出口；
业务若自建出口，可调用 `MetricHub::render_prometheus`。

```yaml
grafana:
  observability:
    enabled: true
    identity:
      environment: local
      cluster: workstation
```

指标默认使用独立 listener；选择 `listener: web` 时自动挂到受管 Web，与 `server.health` 独立。
未编入统一观测能力的 Web 应用使用 health 联动入口。配置归应用层解释，`nametrics-core`
自身不会监听端口。

## 观测与失败语义

family、label 形状、单值资源上限、value kind 或 histogram 结构不满足目录合同时，结构化快照与
Prometheus 文本一致拒绝该样本，并累计
`nametrics_samples_rejected_total{source="native|legacy",reason="..."}`。`reason` 来自固定集合，诊断
不携带被拒样本的动态 label 内容。导出 adapter 的网络失败不回写或清零 `MetricHub`，由宿主按自己的
readiness、退避和停机预算处理。

## 明确边界

- 指标名和 label 名必须来自静态、低基数目录。
- 用户 ID、对象 ID、URL 查询串、错误正文不能作为 label 值。
- 单个 label 值最多 4096 个 UTF-8 字节；该资源上限允许包含完整路由模板和 handler 模块路径的
  静态 `route_id`，但不能替代低基数要求。
- Counter 只能单调增加；Gauge 和 Histogram 必须使用与 descriptor 一致的记录方法。
- `LegacyMetricsSource` 是兼容桥：领域源可直接返回结构化 `snapshot`，文本与 OTLP
  由同一 descriptor 和值渲染。结构化源当前无样本时返回 `Some(Vec::new())`；只有返回 `None`
  的旧源使用 Prometheus 自渲染，不会被反解析或猜测成非文本指标。
- 结构化源的 `worst_case_series` 必须按全部固定 label 组合与 Prometheus histogram
  `bucket + sum + count` 展开计算；用一个内部 histogram cell 冒充一个公开序列会低估预留。
- 本 crate 不启动 listener、不读取 yml、不调度采集，也不提供指标后端、告警规则或跨进程聚合。
