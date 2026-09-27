use super::{DatasourceMode, Discovery, Identity, ObservabilityConfig, Scheme};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type DashboardPanels<'a> = (&'a str, bool, Vec<(String, String)>);

const INSTANCE_LABELS: &str = "deployment_environment,cluster,service_name,service_instance_id";

/// 业务作用：选择覆盖心跳判定阈值的有限读取窗口，避免低频推送早于阈值退出瞬时 lookback。
/// 参数说明：`config` 提供采样周期与规则求值周期。
/// 返回：至少五分钟，且严格覆盖三个采样周期的毫秒窗口；窗口本身不承担期望实例库存职责。
fn heartbeat_window(config: &ObservabilityConfig) -> u64 {
    (config.prometheus.remote_write.interval_ms * 3
        + config
            .provisioning
            .grafana
            .alert_rules
            .evaluation_interval_ms)
        .max(300_000)
}

/// 业务作用：读取外部平台持续提供的期望实例事实，框架不登记或生成此指标。
/// 参数说明：`config` 只提供指标名引用，`selector` 限定环境、集群与服务。
/// 返回：值为一且实例身份合法的期望库存，额外平台 label 与重复来源按业务主键归并；缺失保持空向量。
fn expected_instances(config: &ObservabilityConfig, selector: &str) -> String {
    let metric = &config
        .provisioning
        .grafana
        .alert_rules
        .instance_down
        .expected_instances_metric;
    format!("max by ({INSTANCE_LABELS}) ({metric}{{{selector},service_instance_id=~\"[A-Za-z0-9_.-]{{1,128}}\"}} == 1)")
}

/// 业务作用：只为外部平台认定应在线的实例读取最新心跳，旧进程来源不能覆盖新来源的在线证据。
/// 参数说明：`config` 提供外部指标引用与读取窗口，`selector` 限定环境、集群与服务。
/// 返回：保留业务实例主键的最后心跳；外部库存或历史心跳缺失时保持空向量，不推断健康。
pub(super) fn availability_heartbeat(config: &ObservabilityConfig, selector: &str) -> String {
    let expected = expected_instances(config, selector);
    // 告警与 Grafana 健康基线共同依赖外部期望权威，库存失效时不能用残留心跳补成健康零值。
    format!("(max by ({INSTANCE_LABELS}) (last_over_time(napp_observability_heartbeat_unixtime_seconds{{{selector}}}[{}ms]))) and on ({INSTANCE_LABELS}) ({expected})", heartbeat_window(config))
}

/// 业务作用：同时识别陈旧心跳与期望实例完全缺失，长期停写不会自动解释为恢复。
/// 参数说明：`config` 引用外部平台期望实例指标，`selector` 限定环境、集群与服务。
/// 返回：陈旧样本保留年龄，期望库存仍存在但无历史心跳时返回一；库存失效时不伪造实例或健康状态。
fn remote_write_availability(config: &ObservabilityConfig, selector: &str) -> String {
    let heartbeat = availability_heartbeat(config, selector);
    let expected = expected_instances(config, selector);
    let threshold = config.prometheus.remote_write.interval_ms as f64 * 3.0 / 1000.0;
    // 缺失与陈旧两条路径保留同一业务主键，历史心跳过期不能重置仍被外部平台期望的实例告警。
    format!("((time() - ({heartbeat})) > {threshold}) or (({expected}) unless on ({INSTANCE_LABELS}) ({heartbeat}))")
}

/// 业务作用：使所有业务聚合使用同一组唯一且抓取认证成功的实例。
/// 参数说明：`selector` 限定当前环境、集群与业务筛选范围。
/// 返回：同身份多 target 或最近抓取失败的实例均被排除；remote write 不要求存在 up。
pub(super) fn eligible_instances(selector: &str) -> String {
    format!("((count by ({INSTANCE_LABELS}) (napp_instance_info{{{selector}}}) == 1) unless on ({INSTANCE_LABELS}) count by ({INSTANCE_LABELS}) (napp_instance_info{{{selector}}} and on (job,instance) (up == 0)))")
}

/// 业务作用：在聚合之前隔离不可信或重复实例，保留原指标的维度与数值。
/// 参数说明：`expression` 为单实例瞬时向量，`eligible` 为统一身份门禁。
/// 返回：只有身份门禁通过的原始向量，不以零填充缺失数据。
pub(super) fn scoped(expression: String, eligible: &str) -> String {
    format!("(({expression}) and on ({INSTANCE_LABELS}) {eligible})")
}

/// 业务作用：丢弃与平台授权部署范围不一致的样本，避免 honor_labels 接受伪装的身份。
/// 参数说明：`identity` 为 controller 的可信部署身份，`operator` 选择 Operator 字段命名。
/// 返回：逐字段 keep 规则；丢弃数量由 scrape 样本差值规则告警，不改写业务声明。
fn identity_relabelings(identity: &Identity, operator: bool) -> Vec<Value> {
    identity
        .labels
        .iter()
        .filter(|(key, _)| ["deployment_environment", "cluster", "service_name"].contains(key))
        .map(|(key, value)| {
            let regex = value
                .chars()
                .flat_map(|character| {
                    if ".+*?()|[]{}^$\\".contains(character) {
                        vec!['\\', character]
                    } else {
                        vec![character]
                    }
                })
                .collect::<String>();
            let mut rule = json!({"action":"keep","regex":regex});
            rule[if operator {
                "sourceLabels"
            } else {
                "source_labels"
            }] = json!([key]);
            rule
        })
        .chain(std::iter::once(if operator {
            json!({"action":"keep","sourceLabels":["service_instance_id"],"regex":".+"})
        } else {
            json!({"action":"keep","source_labels":["service_instance_id"],"regex":".+"})
        }))
        .collect()
}

/// 业务作用：以服务、环境、集群和资源种类派生稳定归属标识。
/// 参数说明：`identity` 为冻结部署身份，`kind` 为模板种类。
/// 返回：不含副本身份的稳定 UID，滚动发布不会新建重复资源。
pub fn owner_id(identity: &Identity, kind: &str) -> String {
    let mut hash = Sha256::new();
    for key in ["service_name", "deployment_environment", "cluster"] {
        hash.update(
            identity
                .labels
                .get(key)
                .map(String::as_bytes)
                .unwrap_or_default(),
        );
        hash.update([0]);
    }
    hash.update(kind.as_bytes());
    format!("nasa-{:x}", hash.finalize())[..37].to_owned()
}

/// 业务作用：生成默认单环境、单集群并可逐实例下钻的内建 Dashboard。
/// 参数说明：`config` 控制模板开关，`identity` 约束部署范围。
/// 返回：按 UID 引用 datasource 的完整 Dashboard，不把无数据转换成零。
pub fn dashboards(config: &ObservabilityConfig, identity: &Identity) -> Vec<Value> {
    let ds = datasource_uid(config, identity);
    let selector = "deployment_environment=~\"$environment\",cluster=~\"$cluster\",service_name=~\"$service\",service_version=~\"$version\",service_instance_id=~\"$instance\"";
    let sql = format!("{selector},driver=~\"$driver\",datasource=~\"$datasource\",operation=~\"$operation\",method=~\"$method\"");
    let db = format!("{selector},driver=~\"$driver\",datasource=~\"$datasource\"");
    let stream = format!("{db},method=~\"$method\"");
    let eligible = eligible_instances("deployment_environment=~\"$environment\",cluster=~\"$cluster\",service_name=~\"$service\",service_instance_id=~\"$instance\"");
    let vector = |metric: &str, labels: &str| scoped(format!("{metric}{{{labels}}}"), &eligible);
    let rates = |metric: &str, labels: &str| {
        scoped(
            format!("rate({metric}{{{labels}}}[$__rate_interval])"),
            &eligible,
        )
    };
    let rate = |metric: &str, labels: &str| {
        format!("sum by (deployment_environment,cluster,service_name,driver,datasource,operation,method) ({})", rates(metric, labels))
    };
    let hist = |metric: &str, labels: &str, q: f64| {
        format!("histogram_quantile({q}, sum by (le,deployment_environment,cluster,service_name,driver,datasource,method) ({}))", rates(&format!("{metric}_bucket"), labels))
    };
    let mut groups: Vec<DashboardPanels<'_>> = Vec::new();
    groups.push((
        "mapper",
        config.provisioning.grafana.dashboards.mapper,
        vec![
            (
                "Mapper 完成速率".into(),
                rate("namapper_method_calls_total", &sql),
            ),
            (
                "Mapper 失败率".into(),
                format!(
                    "{} / clamp_min({}, 1e-12)",
                    rate(
                        "namapper_method_calls_total",
                        &format!("{sql},outcome=~\"error|panic\"")
                    ),
                    rate("namapper_method_calls_total", &sql)
                ),
            ),
            (
                "Mapper 取消率".into(),
                format!(
                    "{} / clamp_min({}, 1e-12)",
                    rate(
                        "namapper_method_calls_total",
                        &format!("{sql},outcome=\"cancelled\"")
                    ),
                    rate("namapper_method_calls_total", &sql)
                ),
            ),
            (
                "Mapper 成功调用 P99".into(),
                hist(
                    "namapper_method_duration_seconds",
                    &format!("{sql},status=\"success\""),
                    0.99,
                ),
            ),
            (
                "Mapper 在途调用".into(),
                format!(
                    "sum by (method) ({})",
                    vector("namapper_method_in_flight", &sql)
                ),
            ),
            (
                "数据库调用速率".into(),
                rate("namapper_db_client_operations_total", &sql),
            ),
            (
                "数据库失败率".into(),
                format!(
                    "{} / clamp_min({}, 1e-12)",
                    rate(
                        "namapper_db_client_operations_total",
                        &format!("{sql},outcome!~\"ok|not_found|cancelled\"")
                    ),
                    rate("namapper_db_client_operations_total", &sql)
                ),
            ),
            (
                "成功调用 P50".into(),
                hist(
                    "namapper_db_client_duration_seconds",
                    &format!("{sql},status=\"success\""),
                    0.5,
                ),
            ),
            (
                "成功调用 P95".into(),
                hist(
                    "namapper_db_client_duration_seconds",
                    &format!("{sql},status=\"success\""),
                    0.95,
                ),
            ),
            (
                "成功调用 P99".into(),
                hist(
                    "namapper_db_client_duration_seconds",
                    &format!("{sql},status=\"success\""),
                    0.99,
                ),
            ),
            (
                "缓存与数据库路径".into(),
                format!(
                    "sum by (path) ({})",
                    rates("namapper_method_calls_total", &sql)
                ),
            ),
            (
                "数据库在途调用".into(),
                format!(
                    "sum by (method) ({})",
                    vector("namapper_db_client_in_flight", &sql)
                ),
            ),
            (
                "慢操作速率".into(),
                rate("namapper_slow_operations_total", &sql),
            ),
            (
                "返回与影响行数".into(),
                format!(
                    "sum by (kind,method) ({})",
                    rates("namapper_rows_total", &sql)
                ),
            ),
            (
                "Stream 首行 P99".into(),
                hist("namapper_stream_time_to_first_row_seconds", &stream, 0.99),
            ),
            (
                "Stream 生命周期 P99".into(),
                hist("namapper_stream_lifetime_seconds", &stream, 0.99),
            ),
            (
                "Stream 打开数量".into(),
                format!(
                    "sum by (method) ({})",
                    vector("namapper_stream_open", &stream)
                ),
            ),
            (
                "Stream 终态".into(),
                format!(
                    "sum by (method,outcome) ({})",
                    rates("namapper_streams_total", &stream)
                ),
            ),
            (
                "实例调用 Top K".into(),
                format!(
                    "topk(10,sum by (service_instance_id) ({}))",
                    rates("namapper_db_client_operations_total", &sql)
                ),
            ),
        ],
    ));
    groups.push(("datasource",config.provisioning.grafana.dashboards.datasource,vec![
        ("连接池近似连接数".into(),format!("sum by (driver,datasource,state) ({})",vector("natx_pool_connections",&db))),
        ("连接池容量".into(),format!("sum by (driver,datasource) ({})",vector("natx_pool_max_connections",&db))),
        ("集群连接池使用率".into(),format!("sum by (driver,datasource) ({}) / clamp_min(sum by (driver,datasource) ({}),1)",vector("natx_pool_connections",&format!("{db},state=\"in_use\"")),vector("natx_pool_max_connections",&db))),
        ("单实例最高使用率".into(),format!("max by (driver,datasource) ({} / ignoring(state) clamp_min({},1))",vector("natx_pool_connections",&format!("{db},state=\"in_use\"")),vector("natx_pool_max_connections",&db))),
        ("成功获取连接 P99".into(),hist("natx_connection_acquire_duration_seconds",&format!("{db},status=\"success\""),0.99)),
        ("连接获取超时".into(),rate("natx_connection_acquires_total",&format!("{db},outcome=\"timeout\""))),
        ("连接获取在途".into(),format!("sum by (datasource,purpose) ({})",vector("natx_connection_acquire_in_flight",&db))),
        ("事务连接槽等待 P99".into(),hist("natx_transaction_slot_wait_duration_seconds",&format!("{db},status=\"success\""),0.99)),
        ("事务连接槽等待并发".into(),format!("sum by (datasource) ({})",vector("natx_transaction_slot_wait_in_flight",&db))),
        ("事务连接槽取消".into(),rate("natx_transaction_slot_waits_total",&format!("{db},outcome=\"cancelled\""))),
    ]));
    groups.push((
        "notifications",
        config.provisioning.grafana.dashboards.notifications,
        vec![
            (
                "通知入队速率".into(),
                format!(
                    "sum by (provider,event) ({})",
                    rates("nanotify_enqueued_total", selector)
                ),
            ),
            (
                "通知丢弃原因".into(),
                format!(
                    "sum by (provider,event,reason) ({})",
                    rates("nanotify_dropped_total", selector)
                ),
            ),
            (
                "通知队列深度".into(),
                vector("nanotify_queue_depth", selector),
            ),
            (
                "通知队列容量".into(),
                vector("nanotify_queue_capacity", selector),
            ),
            (
                "最拥塞实例队列".into(),
                format!(
                    "max({} / clamp_min({},1))",
                    vector("nanotify_queue_depth", selector),
                    vector("nanotify_queue_capacity", selector)
                ),
            ),
            (
                "Provider 投递结果".into(),
                format!(
                    "sum by (provider,outcome) ({})",
                    rates("nanotify_deliveries_total", selector)
                ),
            ),
            (
                "Provider 成功率".into(),
                format!(
                    "sum by (provider) ({}) / clamp_min(sum by (provider) ({}),1e-12)",
                    rates(
                        "nanotify_deliveries_total",
                        &format!("{selector},outcome=\"accepted\"")
                    ),
                    rates("nanotify_deliveries_total", selector)
                ),
            ),
            (
                "Provider 投递耗时 P99".into(),
                format!(
                    "histogram_quantile(0.99, sum by (le,provider) ({}))",
                    rates("nanotify_delivery_duration_seconds_bucket", selector)
                ),
            ),
        ],
    ));
    groups.push(("interfaces",config.provisioning.grafana.dashboards.interfaces,vec![
        ("实例库存与部署版本".into(),format!("napp_instance_info{{{selector}}}")),
        ("抓取目标可用性".into(),"up{deployment_environment=~\"$environment\",cluster=~\"$cluster\",service_name=~\"$service\"}".into()),
        ("远程指标心跳年龄".into(),format!("time() - napp_observability_heartbeat_unixtime_seconds{{{selector}}}")),
        ("接口调用速率".into(),format!("sum by (command,group) ({})",rates("nafana_requests_total",selector))),
    ]));
    groups.into_iter().filter(|(_,enabled,_)| *enabled).map(|(name,_,panels)| {
        let variables = [("environment","deployment_environment"),("cluster","cluster"),("service","service_name"),("version","service_version"),("driver","driver"),("datasource","datasource"),("operation","operation"),("method","method"),("instance","service_instance_id")].into_iter().map(|(name,label)| {
            let filter = match name { "environment" => String::new(), "cluster" => "deployment_environment=~\"$environment\"".into(), _ => "deployment_environment=~\"$environment\",cluster=~\"$cluster\"".into() };
            let source = if ["driver","datasource","operation","method"].contains(&name) { "namapper_method_calls_total" } else { "napp_instance_info" };
            let value = match name { "environment" => identity.labels.get("deployment_environment").cloned().unwrap_or_default(), "cluster" => identity.labels.get("cluster").cloned().unwrap_or_default(), _ => "$__all".into() };
            json!({"name":name,"type":"query","datasource":{"type":"prometheus","uid":ds},"query":format!("label_values({source}{{{filter}}}, {label})"),"refresh":1,"multi":!["environment","cluster"].contains(&name),"includeAll":!["environment","cluster"].contains(&name),"allValue":".*","current":{"text":value,"value":value}})
        }).collect::<Vec<_>>();
        let panels = panels.into_iter().enumerate().map(|(index,(title,expr))| json!({"id":index+1,"title":title,"type":"timeseries","datasource":{"type":"prometheus","uid":ds},"gridPos":{"x":(index%2)*12,"y":(index/2)*8,"w":12,"h":8},"targets":[{"refId":"A","expr":expr}],"fieldConfig":{"defaults":{"noValue":"无数据"}}})).collect::<Vec<_>>();
        json!({"uid":owner_id(identity,name),"title":format!("NASA {name}"),"tags":[format!("nasa-owner:{}",owner_id(identity,"owner"))],"schemaVersion":39,"version":0,"editable":false,"timezone":"browser","refresh":"30s","time":{"from":"now-1h","to":"now"},"templating":{"list":variables},"panels":panels})
    }).collect()
}

/// 业务作用：生成保持环境与集群维度的聚合告警，避免按实例均值稀释异常。
/// 参数说明：`config` 为规则阈值，`identity` 限定当前部署。
/// 返回：Prometheus 规则组模型，供 Grafana 或 Operator 适配。
pub fn prometheus_rules(config: &ObservabilityConfig, identity: &Identity) -> Value {
    let a = &config.provisioning.grafana.alert_rules;
    let select = identity
        .labels
        .iter()
        .filter(|(k, _)| ["service_name", "deployment_environment", "cluster"].contains(k))
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect::<Vec<_>>()
        .join(",");
    let group = "deployment_environment,cluster,service_name,driver,datasource,method";
    let pool_group = "deployment_environment,cluster,service_name,driver,datasource";
    let eligible = eligible_instances(&select);
    let vector =
        |metric: &str, extra: &str| scoped(format!("{metric}{{{select}{extra}}}"), &eligible);
    let increases = |metric: &str, extra: &str, window: u64| {
        scoped(
            format!("increase({metric}{{{select}{extra}}}[{window}ms])"),
            &eligible,
        )
    };
    let sum = |metric: &str, extra: &str, window: u64| {
        format!(
            "sum by ({group}) ({})",
            scoped(
                format!("rate({metric}{{{select}{extra}}}[{window}ms])"),
                &eligible
            )
        )
    };
    let quantile = |metric: &str, window: u64| {
        format!(
            "histogram_quantile(0.99,sum by (le,{group}) ({}))",
            scoped(
                format!("rate({metric}_bucket{{{select},status=\"success\"}}[{window}ms])"),
                &eligible
            )
        )
    };
    let mut rules = Vec::new();
    let mut push = |name: &str, enabled: bool, expr: String, hold: u64| {
        if a.enabled && enabled {
            rules.push(json!({"alert":name,"expr":expr,"for":format!("{hold}ms"),"labels":{"severity":"warning","nasa_owner":owner_id(identity,"owner"),"notification_policy_ref":a.notification_policy_ref},"annotations":{"summary":name}}));
        }
    };
    push(
        "MapperExecutionErrorRate",
        a.error_rate.enabled,
        format!(
            "({} / clamp_min({},1e-12)) > {}",
            sum(
                "namapper_db_client_operations_total",
                ",outcome!~\"ok|not_found|cancelled\"",
                a.error_rate.window_ms
            ),
            sum(
                "namapper_db_client_operations_total",
                "",
                a.error_rate.window_ms
            ),
            a.error_rate.threshold_ratio
        ),
        a.error_rate.for_ms,
    );
    push(
        "MapperSuccessfulP99",
        a.p99.enabled,
        format!(
            "{} > {}",
            quantile("namapper_db_client_duration_seconds", a.p99.window_ms),
            a.p99.threshold_ms as f64 / 1000.0
        ),
        a.p99.for_ms,
    );
    let pool = vector("natx_pool_connections", ",state=\"in_use\"");
    let capacity = vector("natx_pool_max_connections", "");
    push("DatasourcePoolSaturation",a.pool_saturation.enabled,format!("(sum by ({pool_group}) ({pool}) / clamp_min(sum by ({pool_group}) ({capacity}),1) > {}) or (max by ({pool_group}) ({pool} / ignoring(state) clamp_min({capacity},1)) > {})",a.pool_saturation.threshold_ratio,a.pool_saturation.threshold_ratio),a.pool_saturation.for_ms);
    push(
        "DatasourceAcquireTimeout",
        a.acquire_timeout.enabled,
        format!(
            "sum by ({pool_group}) ({}) >= {}",
            increases(
                "natx_connection_acquires_total",
                ",outcome=\"timeout\"",
                a.acquire_timeout.window_ms
            ),
            a.acquire_timeout.threshold_count
        ),
        a.acquire_timeout.for_ms,
    );
    push(
        "TransactionSlotWaitP99",
        a.transaction_slot_wait.enabled,
        format!(
            "{} > {}",
            quantile(
                "natx_transaction_slot_wait_duration_seconds",
                a.transaction_slot_wait.window_ms
            ),
            a.transaction_slot_wait.threshold_ms as f64 / 1000.0
        ),
        a.transaction_slot_wait.for_ms,
    );
    let sw = a.stream_cancel_rate.window_ms;
    let total = format!(
        "sum by ({group}) ({})",
        increases(
            "namapper_streams_total",
            ",outcome!=\"cancelled_before_poll\"",
            sw
        )
    );
    push(
        "MapperStreamCancellation",
        a.stream_cancel_rate.enabled,
        format!(
            "(sum by ({group}) ({}) / clamp_min({total},1) > {}) and ({total} >= {})",
            increases("namapper_streams_total", ",outcome=\"cancelled\"", sw),
            a.stream_cancel_rate.threshold_ratio,
            a.stream_cancel_rate.minimum_calls
        ),
        a.stream_cancel_rate.for_ms,
    );
    push(
        "NotificationQueueSaturation",
        a.notification_queue.enabled,
        format!(
            "max by (deployment_environment,cluster,service_name) ({} / clamp_min({},1)) > {}",
            vector("nanotify_queue_depth", ""),
            vector("nanotify_queue_capacity", ""),
            a.notification_queue.threshold_ratio
        ),
        a.notification_queue.for_ms,
    );
    let pw = a.provider_failure_rate.window_ms;
    let pg = "deployment_environment,cluster,service_name,provider";
    let deliveries = format!(
        "sum by ({pg}) ({})",
        increases("nanotify_deliveries_total", "", pw)
    );
    push(
        "NotificationProviderFailureRate",
        a.provider_failure_rate.enabled,
        format!(
            "(sum by ({pg}) ({}) / clamp_min({deliveries},1) > {}) and ({deliveries} >= {})",
            // 通知的成功合同是 provider 接受，不等同于 SQL 的 ok，也不承诺用户已阅读消息。
            increases("nanotify_deliveries_total", ",outcome!=\"accepted\"", pw),
            a.provider_failure_rate.threshold_ratio,
            a.provider_failure_rate.minimum_deliveries
        ),
        a.provider_failure_rate.for_ms,
    );
    let availability = if config.primary_discovery() == Discovery::RemoteWrite {
        remote_write_availability(config, &select)
    } else {
        format!("up{{{select}}} == 0")
    };
    push(
        "ObservabilityInstanceDown",
        a.instance_down.enabled,
        availability,
        a.instance_down.for_ms,
    );
    push("ObservabilityIdentityCollision",a.identity_collision.enabled,format!("(count by ({INSTANCE_LABELS}) (napp_instance_info{{{select}}}) > 1) or ((scrape_samples_scraped{{{select}}} - scrape_samples_post_metric_relabeling{{{select}}}) > 0)"),a.identity_collision.for_ms);
    json!({"groups":[{"name":owner_id(identity,"rules"),"interval":format!("{}ms",a.evaluation_interval_ms),"rules":rules}]})
}

/// 业务作用：为逐容器 DNS 发现或既有平台生成确定的 Prometheus job。
/// 参数说明：`config/identity` 为同一 YAML，`dns_name` 为平台注入的容器服务 DNS，`token_file` 为平台可读凭据文件。
/// 返回：抓取每个 DNS A/AAAA 地址的配置，永不使用负载均衡 HTTP URL。
pub fn discovery_config(
    config: &ObservabilityConfig,
    identity: &Identity,
    dns_name: &str,
    token_file: Option<&str>,
) -> Result<Value, String> {
    if config.primary_discovery() == Discovery::RemoteWrite {
        return Ok(json!({"scrape_configs":[]}));
    }
    if dns_name.is_empty()
        || dns_name
            .chars()
            .any(|c| c.is_whitespace() || "/?#".contains(c))
    {
        return Err("docker DNS binding is invalid".into());
    }
    let port = config
        .bind()
        .parse::<std::net::SocketAddr>()
        .map_err(|_| "invalid scrape address")?
        .port();
    let p = &config.provisioning.prometheus;
    let mut job = json!({"job_name":if p.job_name.is_empty(){owner_id(identity,"job")}else{p.job_name.clone()},"honor_labels":true,"scheme":if p.scheme==Scheme::Http{"http"}else{"https"},"metrics_path":config.prometheus.scrape.path,"scrape_interval":format!("{}ms",p.scrape_interval_ms),"scrape_timeout":format!("{}ms",p.scrape_timeout_ms),"dns_sd_configs":[{"names":[dns_name],"type":"A","port":port}],"relabel_configs":identity.labels.iter().filter(|(key,_)| ["deployment_environment","cluster","service_name"].contains(key)).map(|(key,value)| json!({"target_label":key,"replacement":value})).collect::<Vec<_>>()});
    job["metric_relabel_configs"] = json!(identity_relabelings(identity, false));
    if config.prometheus.scrape.auth.mode == super::AuthMode::Bearer {
        job["authorization"] = json!({"type":"Bearer","credentials_file":token_file.ok_or("platform scrape token file binding is required")?});
    }
    Ok(json!({"scrape_configs":[job]}))
}

/// 业务作用：生成只匹配业务 Pod 的 PodMonitor，避免抓取 Service VIP。
/// 参数说明：`namespace` 限定平台授权范围，`token_secret` 为监控命名空间中的凭据 Secret 名。
/// 返回：带确定归属标签的 Operator 资源。
pub fn pod_monitor(
    config: &ObservabilityConfig,
    identity: &Identity,
    namespace: &str,
    token_secret: Option<&str>,
) -> Result<Value, String> {
    let p = &config.provisioning.prometheus;
    let mut endpoint = json!({"port":"metrics","path":config.prometheus.scrape.path,"scheme":if p.scheme==Scheme::Http{"http"}else{"https"},"honorLabels":true,"interval":format!("{}ms",p.scrape_interval_ms),"scrapeTimeout":format!("{}ms",p.scrape_timeout_ms),"relabelings":identity.labels.iter().filter(|(key,_)| ["deployment_environment","cluster","service_name"].contains(key)).map(|(key,value)| json!({"targetLabel":key,"replacement":value})).collect::<Vec<_>>()});
    endpoint["metricRelabelings"] = json!(identity_relabelings(identity, true));
    if config.prometheus.scrape.auth.mode == super::AuthMode::Bearer {
        endpoint["authorization"] = json!({"type":"Bearer","credentials":{"name":token_secret.ok_or("platform scrape Secret binding is required")?,"key":"token"}});
    }
    Ok(
        json!({"apiVersion":"monitoring.coreos.com/v1","kind":"PodMonitor","metadata":{"name":owner_id(identity,"podmonitor"),"namespace":namespace,"labels":{"nasa-runtime-owner":owner_id(identity,"owner")}},"spec":{"namespaceSelector":{"matchNames":[namespace]},"selector":{"matchLabels":{"app.kubernetes.io/name":identity.labels.get("service_name")}},"podMetricsEndpoints":[endpoint]}}),
    )
}

/// 业务作用：确定 Dashboard 与规则共同引用的 datasource UID。
/// 参数说明：`config` 为显式 UID，`identity` 用于缺省派生。
/// 返回：managed 与 existing 模式使用同一确定引用。
pub(crate) fn datasource_uid(config: &ObservabilityConfig, identity: &Identity) -> String {
    let ds = &config.provisioning.grafana.datasource;
    if ds.uid.is_empty() && ds.mode == DatasourceMode::Managed {
        owner_id(identity, "datasource")
    } else {
        ds.uid.clone()
    }
}
