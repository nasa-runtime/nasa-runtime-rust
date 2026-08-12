# nametrics-core

`nametrics-core` 是进程级 provider-neutral 指标核心：统一 descriptor catalog、冲突审计、进程内记录、
结构化快照和 Prometheus 文本导出。它不依赖 Web、应用运行时或具体遥测 SDK。

业务项目通常通过 `nasa::application` 注册兼容指标源，不直接依赖本 crate：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "grafana", "web"] }
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

## YML 配置

本 crate 不读取 yml，也不启动 scrape endpoint。`napp` Web 组件负责统一 `/metrics` 出口；
业务若自建出口，可调用 `MetricHub::render_prometheus`。

```yaml
server:
  health: true
```

当前 `napp` Web 组件在 `server.health=true` 时统一挂载 `/healthz`、`/readyz` 和 `/metrics`；该开关
归 Web/应用层解释，不是本 crate 的固定配置结构。`nametrics-core` 自身不会监听端口。

## 主要边界

- 指标名和 label 名必须来自静态、低基数目录。
- 用户 ID、对象 ID、URL 查询串、错误正文不能作为 label 值。
- 单个 label 值最多 4096 个 UTF-8 字节；该资源上限允许包含完整路由模板和 handler 模块路径的
  静态 `route_id`，但不能替代低基数要求。
- Counter 只能单调增加；Gauge 和 Histogram 必须使用与 descriptor 一致的记录方法。
- `LegacyMetricsSource` 是兼容桥：领域源可直接返回结构化 `snapshot`，文本与 OTLP
  由同一 descriptor 和值渲染。结构化源当前无样本时返回 `Some(Vec::new())`；只有返回 `None`
  的旧源使用 Prometheus 自渲染，不会被反解析或猜测成非文本指标。
- family、label 形状、单值资源上限、value kind 或 histogram 结构不满足目录合同时，两个结构化
  出口一致拒绝该样本，并累计
  `nametrics_samples_rejected_total{source="native|legacy",reason="..."}`；该诊断不携带被拒样本的
  动态 label 内容。
