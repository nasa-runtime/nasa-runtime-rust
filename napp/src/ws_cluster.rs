//! WS 集群适配器的命名来源装配、健康观测和关闭责任。

use crate::{
    ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult, ComponentId,
    ReadyContext, ShutdownAction, ShutdownContext,
};
use naws::cluster::Notifier;
use std::{sync::Arc, time::Instant};

pub(crate) enum WsClusterPlan {
    #[cfg(feature = "ws-redis")]
    Redis {
        source: String,
        node: String,
        stream: String,
        incarnation: naws::cluster::Incarnation,
    },
    #[cfg(feature = "ws-kafka")]
    Kafka {
        source: String,
        config: naws::kafka::WsKafkaRuntimeConfig,
        contract: naws::kafka::WsKafkaTopicContract,
        incarnation: naws::cluster::Incarnation,
    },
}

impl WsClusterPlan {
    /// 业务作用：冻结 Redis 广播流与节点身份，不建立连接。
    /// 参数说明：`source` 为 qualifier，`node` 为稳定节点，`stream` 为广播流，`incarnation` 为持久单调节点代次。
    /// 返回：字段有界且非空时返回计划，否则拒绝。
    #[cfg(feature = "ws-redis")]
    pub(crate) fn redis(
        source: &str,
        node: &str,
        stream: &str,
        incarnation: naws::cluster::Incarnation,
    ) -> ApplicationResult<Self> {
        for value in [source, node, stream] {
            if value.is_empty() || value.len() > 256 || value.trim() != value {
                return Err(error("invalid WS Redis plan identity"));
            }
        }
        Ok(Self::Redis {
            source: source.into(),
            node: node.into(),
            stream: stream.into(),
            incarnation,
        })
    }

    /// 业务作用：冻结 Kafka client 归属和 WS 协议策略，避免业务持有关闭责任。
    /// 参数说明：`source` 为 qualifier，`config` 和 `contract` 是领域配置与 topic 合同，`incarnation` 为持久单调节点代次。
    /// 返回：名称有效时返回计划；完整协议校验由领域构造器完成。
    #[cfg(feature = "ws-kafka")]
    pub(crate) fn kafka(
        source: &str,
        config: naws::kafka::WsKafkaRuntimeConfig,
        contract: naws::kafka::WsKafkaTopicContract,
        incarnation: naws::cluster::Incarnation,
    ) -> ApplicationResult<Self> {
        if source.is_empty() || source.len() > 128 || source.trim() != source {
            return Err(error("invalid WS Kafka source"));
        }
        Ok(Self::Kafka {
            source: source.into(),
            config,
            contract,
            incarnation,
        })
    }
}

#[derive(Clone)]
pub(crate) enum ClusterRuntime {
    #[cfg(feature = "ws-redis")]
    Redis(Arc<naws::cluster::RedisNotifier>),
    #[cfg(feature = "ws-kafka")]
    Kafka(naws::kafka::WsKafkaRuntime),
}

impl ClusterRuntime {
    /// 业务作用：在监听器发布前关闭协议准入，覆盖启动失败和停机放弃路径。
    /// 参数说明：无。
    /// 返回：已发出停止信号，完成证明由异步等待提供。
    fn close(&self) {
        match self {
            #[cfg(feature = "ws-redis")]
            Self::Redis(runtime) => runtime.shutdown(),
            #[cfg(feature = "ws-kafka")]
            Self::Kafka(runtime) => runtime.notifier().shutdown(),
        }
    }

    /// 业务作用：把本地发送器与来源围栏交给 Kafka 数据面，保持业务 listener 未开放。
    /// 参数说明：`server` 为已构建但未绑定的 WS server。
    /// 返回：数据面绑定成功或 Redis 无需绑定时成功；重复或缺少围栏时拒绝。
    pub(crate) fn bind(&self, server: &naws::Server) -> ApplicationResult<()> {
        match self {
            #[cfg(feature = "ws-redis")]
            Self::Redis(_) => {
                let _ = server;
                Ok(())
            }
            #[cfg(feature = "ws-kafka")]
            Self::Kafka(runtime) => runtime
                .bind_data_plane(
                    naws::kafka::SenderSlot::new(server.sender().clone()),
                    server.kafka_source_fence(),
                )
                .map_err(|_| error("WS Kafka data plane binding failed")),
        }
    }

    /// 业务作用：提供实际协议运行态的健康结论，不把构造成功记作远端可用。
    /// 参数说明：无。
    /// 返回：Redis 两侧连接正常；Kafka 还须同时拥有有效 assignment 和近期 poll 证据。
    pub(crate) async fn healthy(&self) -> bool {
        match self {
            #[cfg(feature = "ws-redis")]
            Self::Redis(runtime) => runtime.is_healthy(),
            #[cfg(feature = "ws-kafka")]
            Self::Kafka(runtime) => {
                let health = runtime.health();
                if health.state != "Running" || health.last_contract_error.is_some() {
                    return false;
                }
                // 运行态名称不能证明 consumer 仍持有 assignment；两侧快照必须同时满足近期进展门禁。
                let observed = tokio::time::timeout(std::time::Duration::from_millis(500), async {
                    tokio::try_join!(runtime.control_group_health(), runtime.data_group_health())
                })
                .await;
                match observed {
                    Ok(Ok((control, data))) => [control, data].iter().all(|group| {
                        group.state == nafka::GroupState::Running
                            && !group.assignment.is_empty()
                            && group.ready_assignment_epoch.is_some()
                            && group
                                .last_poll_at
                                .and_then(|time| time.elapsed().ok())
                                .is_some_and(|age| age <= std::time::Duration::from_secs(15))
                    }),
                    _ => false,
                }
            }
        }
    }
}

/// 业务作用：装配唯一 WS 集群策略，并在可能启动任务之前移交完整关闭责任。
/// 参数说明：`context` 提供受管依赖和清理栈，`builder` 已应用业务鉴权与 endpoint 策略。
/// 返回：更新后的 builder 与内部观测句柄；无计划时不访问任何后端。
pub(crate) async fn prepare(
    context: &mut ReadyContext<'_>,
    builder: naws::ServerBuilder,
) -> ApplicationResult<(naws::ServerBuilder, Option<ClusterRuntime>)> {
    let application = context.application().clone();
    let Some(plan) = application.take_ws_cluster_plan() else {
        return Ok((builder, None));
    };
    // 两条入口竞争会覆盖既有通知器的关闭权，必须在建立任何新任务之前拒绝。
    if builder.has_cluster() {
        return Err(error(
            "standard WS cluster plan conflicts with configure_ws cluster customization",
        ));
    }
    let (builder, runtime) = match plan {
        #[cfg(feature = "ws-redis")]
        WsClusterPlan::Redis {
            source,
            node,
            stream,
            incarnation,
        } => {
            let client = application.redis(&source).await?;
            let config = naws::cluster::RedisNotifierConfig {
                uri: client.config().url.clone(),
                stream_key: stream,
                ..Default::default()
            };
            // BLOCK 读取使用专用连接，认证与地址仍来自同一个受管 Redis 来源。
            let runtime = naws::cluster::RedisNotifier::new(config)
                .map_err(|_| error("WS Redis notifier configuration failed"))?;
            (
                builder
                    .cluster(node, runtime.clone())
                    .cluster_incarnation(incarnation),
                ClusterRuntime::Redis(runtime),
            )
        }
        #[cfg(feature = "ws-kafka")]
        WsClusterPlan::Kafka {
            source,
            config,
            contract,
            incarnation,
        } => {
            let proxy = application.kafka(&source)?.ws_runtime_proxy()?;
            let node = config.local_node.clone();
            let ready_timeout = std::time::Duration::from_millis(config.ready_timeout_ms);
            let runtime = naws::kafka::WsKafkaRuntime::new(proxy, config, contract)
                .map_err(|_| error("WS Kafka runtime configuration failed"))?;
            let builder = builder
                .cluster(node, Arc::new(runtime.notifier()))
                .cluster_data_publisher(runtime.cluster_data_publisher())
                .cluster_ready_timeout(ready_timeout)
                .cluster_incarnation(incarnation);
            (builder, ClusterRuntime::Kafka(runtime))
        }
    };
    context.activate(Box::new(ClusterShutdown(runtime.clone())));
    Ok((builder, Some(runtime)))
}

struct ClusterShutdown(ClusterRuntime);

impl Drop for ClusterShutdown {
    /// 业务作用：清理动作尚未执行就被放弃时关闭子能力准入。
    /// 参数说明：无。
    /// 返回：同步请求停止，不把未等待任务记为已排空。
    fn drop(&mut self) {
        self.0.close();
    }
}

impl ShutdownAction for ClusterShutdown {
    /// 业务作用：提供固定停机归因，不携带业务节点或来源字段。
    /// 参数说明：无。
    /// 返回：WS 集群清理动作名称。
    fn label(&self) -> &'static str {
        "ws-cluster"
    }

    /// 业务作用：在共享截止点前等待协议任务真实退出，再允许来源组件释放。
    /// 参数说明：`context` 携带宿主剩余停机预算。
    /// 返回：全部任务结束时成功，超时或领域关闭失败时返回未完成错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.0.close();
            let deadline = tokio::time::Instant::from_std(Instant::now() + context.remaining());
            match &self.0 {
                #[cfg(feature = "ws-redis")]
                ClusterRuntime::Redis(runtime) => {
                    if let Some(tasks) = runtime.task_tracker() {
                        tasks.close();
                        tokio::time::timeout_at(deadline, tasks.wait())
                            .await
                            .map_err(|_| {
                                ApplicationError::new(
                                    ComponentId::Ws,
                                    ApplicationPhase::Stopping,
                                    "WS Redis shutdown incomplete",
                                )
                            })?;
                    }
                }
                #[cfg(feature = "ws-kafka")]
                ClusterRuntime::Kafka(runtime) => {
                    tokio::time::timeout_at(deadline, runtime.shutdown())
                        .await
                        .map_err(|_| {
                            ApplicationError::new(
                                ComponentId::Ws,
                                ApplicationPhase::Stopping,
                                "WS Kafka shutdown incomplete",
                            )
                        })?
                        .map_err(|_| {
                            ApplicationError::new(
                                ComponentId::Ws,
                                ApplicationPhase::Stopping,
                                "WS Kafka shutdown failed",
                            )
                        })?;
                }
            }
            Ok(())
        })
    }
}

/// 业务作用：生成不含后端地址或凭据的 WS 集群阶段错误。
/// 参数说明：`message` 为固定失败原因。
/// 返回：WS 组件的领域装配错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Ws, ApplicationPhase::Ready, message)
}
