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

`MetricHub::register` 为原生 descriptor 创建受预算管理的 cell；
`register_legacy_source_reserved` 把 descriptor、结构化 source 与最坏公开序列数作为一个事务式登记动作。
任一 descriptor 冲突或容量不足时，catalog、source 和预留计数都保持原状。`register_legacy_source` 只为
无法声明最坏值域的旧源保留，不提供容量保证，稳定组件不应继续使用该入口。

业务项目通常通过 `nasa::application` 注册兼容指标源，不直接依赖本 crate：

```toml
[dependencies]
nasa = { version = "2", features = ["application", "grafana", "web"] }
```

```rust
#[nasa::application("web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register_metrics_source(nasa::grafana::metrics_source())?;
    Ok(())
}
```

底层组件可为自己的静态指标族声明 `MetricDescriptor`，再在同一个 `MetricHub` 注册。相同名称但
kind、label、unit、help 或 histogram bounds 不一致时会返回 `MetricConflict`，不能静默合并。

受预算管理的原生动态 cell 与显式预留总上限为 100,000；目录自身的拒绝诊断先占位。组件必须在绑定
端口或发布 Ready 前完成登记，不能等首个样本到达后静默丢弃。预留按最终公开序列计算：普通 family
展开全部固定 label 组合，Prometheus histogram 的每个组合还要计算全部 `bucket`、`sum` 和 `count`。

## YML 配置

本 crate 不读取 yml，也不启动 scrape endpoint。`napp` Web 组件负责统一 `/metrics` 出口；
业务若自建出口，可调用 `MetricHub::render_prometheus`。

```yaml
server:
  health: true
```

当前 `napp` Web 组件在 `server.health=true` 时统一挂载 `/healthz`、`/readyz` 和 `/metrics`；该开关
归 Web/应用层解释，不是本 crate 的固定配置结构。`nametrics-core` 自身不会监听端口。

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
