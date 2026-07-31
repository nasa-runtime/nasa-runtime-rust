# nafana Dashboard 部署清单

本目录提供 Prometheus 抓取模板和 Grafana 集群接口墙。Dashboard 仅使用 Grafana 内置 Text、Stat、
Time series、RowsLayout 和 AutoGridLayout，不需要安装第三方插件。

完整的业务接入、`#[grafana]` 全参数示例、配置驱动隔离、指标语义和安全说明见
[`../README.md`](../README.md)。

## 1. 文件用途

| 文件 | 用途 | 是否直接覆盖现有配置 |
|---|---|---|
| `nafana-interfaces.json` | Grafana V2 Dashboard resource | 标准接入无需修改；通过 V2 API 创建或更新 |
| `prometheus.yml` | 通用双实例 scrape 示例 | 已有 Prometheus 时只合并该 job，并修改 target `host:port` |
| `grafana-datasource.yml` | Grafana Prometheus 数据源 provisioning | 可直接挂载；无法解析 `prometheus` 时只改 `url` 的 `host:port` |
| `grafana.env.example` | Grafana 12.1.0 feature 示例 | 否；合并到已有 feature 列表 |

模板不包含生产账号、密码、token、私网 IP 或部署域名。

Dashboard 的速率和最近状态查询使用 10 秒窗口，因此目标 job 的 `scrape_interval` **必须不大于 5 秒**。
间隔更大时，窗口内可能不足两个样本，接口卡会间歇性显示 0 或 `—`。

## 2. 兼容性

Dashboard 在 Grafana **12.1.0** 验证，使用该版本的实验性
`dashboard.grafana.app/v2alpha1` schema。Grafana 12.1.0 需要启用：

```text
GF_FEATURE_TOGGLES_ENABLE=kubernetesDashboards,dashboardNewLayouts
```

该文件不是对所有更高版本的兼容承诺。目标 Grafana 不再提供 `v2alpha1` 时，应在目标版本重新导出 V2
Resource，并同步修改 JSON 的 `apiVersion` 与导入 API 路径。

## 3. 标准接入只改 `host:port`

本目录使用与具体业务工程无关的默认约定：

- Prometheus job：`nafana-app`
- 指标入口：`/metrics`
- Grafana 数据源：显示名 `Prometheus`，UID `prometheus`
- Dashboard：自动发现 job、instance、group 和 command，不写死节点地址

因此复制后只需要：

1. 在 `prometheus.yml` 中把两个 target 改成 Prometheus 能访问的业务实例 `host:port`。
2. 如果 Grafana 不能通过 `http://prometheus:9090` 访问 Prometheus，再修改
   `grafana-datasource.yml` 中 `url` 的 `host:port`。

不需要修改 Dashboard JSON、PromQL、变量、卡片或聚合表达式。只运行一个实例时删掉第二个 target；实例更多时
继续向同一个 target 列表追加地址。

只有业务服务修改了默认 context path、指标路径、Grafana 数据源 UID，或者希望自定义页面标题时，才需要处理
后面的高级定制项。

### 3.1 业务应用

业务应用按推荐方式接入时确认：

1. 开启 `nasa` 的 `grafana` feature。
2. 至少给一个 handler 添加 `#[grafana]`，或初始化配置驱动隔离。
3. 推荐在根路由暴露 `/metrics`；放在 context path 下时同步修改 Prometheus `metrics_path`。
4. 有 context path 时把指标入口挂在该前缀下，并同步修改 Prometheus `metrics_path`。

### 3.2 `prometheus.yml`

模板默认演示“Prometheus 在 Docker、宿主机运行两个独立业务进程”的情况。标准接入只改
`targets`：

| 字段 | 示例 | 含义 |
|---|---|---|
| `job_name` | `nafana-app` | 中性默认名称；Dashboard 会自动发现，通常不改 |
| `targets` | `host.docker.internal:8080` | 必改为 Prometheus 能访问的实例 `host:port` |
| `metrics_path` | `/metrics` | 推荐的根指标入口，放入 context path 时才改 |
| `scheme` | `http` 或 `https` | 采集协议 |
| `scrape_interval` | `5s` | 必须 `<= 5s`，供 Dashboard 的 10 秒窗口查询使用 |
| `labels.name` | `nafana-app` | 默认可直接使用；按组织规范定制时才改 |
| `labels.env` | `local` | 可选部署标签，不参与 Dashboard 聚合主键 |

同一应用的多个实例应放在同一个 `job_name` 下：

```yaml
scrape_configs:
  - job_name: "nafana-app"
    scrape_interval: 5s
    scrape_timeout: 4s
    metrics_path: "/metrics"
    scheme: "http"
    static_configs:
      - targets:
          - "10.0.0.11:18081"
          - "10.0.0.12:18081"
        labels:
          name: "nafana-app"
          env: "production"
```

每个 target 必须对应一个独立进程、容器或 Pod。不同地址不自动代表不同实例：如果一个进程监听
`0.0.0.0:8080`，同时填写 `127.0.0.1:8080` 和 `<同一主机的网卡地址>:8080` 会把同一份指标抓取两次，
导致 Hosts、QPS 和延迟 histogram 重复聚合。单机双实例应使用两个独立进程和不同端口，例如
`host.docker.internal:8081` 与 `host.docker.internal:8082`。

已有 Prometheus 时只复制这个 job，不要用模板覆盖整份 `prometheus.yml`。

### 3.3 `nafana-interfaces.json`

标准接入不修改该文件。它默认引用显示名 `Prometheus`、UID `prometheus`，与
`grafana-datasource.yml` 完全一致。

只有共享 Grafana 中已经占用了资源名、使用其它数据源 UID，或者需要品牌化标题时，才检查以下四项：

1. `metadata.name`：当前 Grafana namespace 内唯一。
2. `spec.title`：页面标题。
3. `DS_PROMETHEUS.current.text`：Grafana 数据源显示名。
4. `DS_PROMETHEUS.current.value`：Grafana 数据源 UID，不是 URL。

Dashboard 查询统一引用 `${DS_PROMETHEUS}`，普通接入不需要逐条修改 PromQL。

## 4. Grafana 添加 Prometheus 数据源

本目录已经提供可直接挂载的 [`grafana-datasource.yml`](grafana-datasource.yml)：

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

`url` 是 Grafana 服务端访问 Prometheus 的地址。Docker Compose 服务名为 `prometheus` 时无需修改；使用
其它部署方式时只改 URL 的 `host:port`。不要修改 `name` 和 `uid`，这样 Dashboard JSON 可以零修改导入。

## 5. 创建 Dashboard

创建最小权限 Grafana service account token，并通过环境变量提供。不要把真实 token 写入脚本或提交到 Git：

```bash
export GRAFANA_URL="https://grafana.example.com"
export GRAFANA_TOKEN="<SERVICE_ACCOUNT_TOKEN>"
export GRAFANA_NAMESPACE="default"

curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  --header "Content-Type: application/json" \
  --request POST \
  "${GRAFANA_URL}/apis/dashboard.grafana.app/v2alpha1/namespaces/${GRAFANA_NAMESPACE}/dashboards" \
  --data-binary @nafana-interfaces.json
```

## 6. 更新已有 Dashboard

不能只把 POST 改为 PUT。更新时先 GET 当前资源，用模板 `spec` 替换当前 `spec`，并保留服务端
`metadata.resourceVersion`：

```bash
export DASHBOARD_NAME="nafana-interfaces"
export DASHBOARD_API="${GRAFANA_URL}/apis/dashboard.grafana.app/v2alpha1/namespaces/${GRAFANA_NAMESPACE}/dashboards"

curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  "${DASHBOARD_API}/${DASHBOARD_NAME}" \
  --output current-dashboard.json

jq --slurpfile template nafana-interfaces.json \
  '.spec = $template[0].spec' \
  current-dashboard.json > dashboard-update.json

curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  --header "Content-Type: application/json" \
  --request PUT \
  "${DASHBOARD_API}/${DASHBOARD_NAME}" \
  --data-binary @dashboard-update.json
```

两个中间 JSON 文件可能包含线上资源 metadata，不应提交到 Git。替换整个 `spec` 会覆盖线上手工修改，执行
PUT 前应 review diff。

## 7. 面板含义

- 顶部：Total QPS、TPS、Hosts、Error、Global P99。
- 每张接口卡：Grafana 原生 Time series，显示 Cluster QPS、Error %、P99 三条主要趋势。
- 鼠标移入折线会出现十字定位和时间，并同时展示 TPS、成功、失败、超时、拒绝、取消、降级、Inflight、
  Hosts、Mean、P50、P90、P99、P99.5 和状态数值。
- `State (0/1/2)`：0 表示没有在线实例，1 表示在线且最近十秒无拒绝/超时/取消，2 表示最近十秒出现保护事件。
- 取消请求计入 QPS、Error 和保护状态，并以 `Canceled /s` 单列。
- 接口卡固定宽度为 320px、高度为 280px；AutoGrid 根据浏览器可用宽度自动决定每行列数。

`Protecting` 不是 circuit breaker Open；`nafana` 当前不实现熔断状态机。

## 8. 多实例与集群口径

同一应用的所有实例必须使用相同 `job_name`，每个 target 保留各自的 `instance`。`$instance` 默认选择 `All`，
这时卡片内所有序列都代表所选实例集合：

- 一个有效实例必须对应独立的指标状态，也就是独立进程、容器或 Pod。
- 同一进程的回环地址、网卡地址、域名别名或代理地址不能当成多个实例。
- `sum(up{job="..."})` 统计的是可抓取 target 数，不足以证明这些 target 背后是不同进程。

- QPS、TPS、各结局、fallback 和 inflight 按实例求和。
- Error 使用全集群非成功速率除以全集群总速率。
- Hosts 只统计存在该接口指标且 Prometheus `up == 1` 的实例。
- 延迟使用各实例的 `nafana_latency_seconds` histogram 合桶后计算，不取单个节点的最大值，也不平均节点分位。

选择一个具体 `$instance` 时，同样的查询自动变成单实例视图。浏览器不直接请求业务节点，面板也没有任何
单节点图片来源。

建议用已知速率做一次验算。假设两个实例对同一接口各收到 `9/s`：

```promql
sum by (instance) (
  rate(nafana_requests_total{job="nafana-app",command="/orders"}[10s])
)
```

结果应为两个 instance 各约 `9/s`，卡片的 Cluster QPS 应约 `18/s`。若两个 instance 各约 `18/s`、集群
约 `36/s`，说明两路流量进入了同一个进程，而该进程又被两个 target 重复抓取。

## 9. 验收顺序

1. 调用一次 `#[grafana]` 业务接口，使命令完成首次注册。
2. 直接访问业务 `/metrics`，确认存在 `nafana_` 指标。
3. Prometheus Targets 中确认目标为 `UP`，并确认每个 target 对应独立进程，而不是同一端口的地址别名。
4. 确认 Targets 页面显示该 job 的实际抓取间隔不大于 5 秒，再查询 `nafana_requests_total`。
5. Grafana 选择正确的数据源与 `job`。
6. 鼠标移入接口折线，确认出现十字、时间以及完整指标列表。
7. 给每个实例发送已知速率流量，确认单实例 QPS 与压测器发送速率一致，Cluster QPS 等于各实例之和。
8. `$instance=All` 时对照 Prometheus 验证 QPS、Error、Hosts 和 histogram 分位是集群聚合值。
9. 停止全部业务实例，确认空状态不出现 `Field not found`。
