use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::RwLock,
    time::{self, sleep},
};
use tracing::{Instrument, debug, debug_span, instrument};

use crate::api::error::Result;
use crate::common::{executor, remote::grpc::NacosGrpcClient};

pub(crate) mod automatic_request;

pub(crate) struct RedoTaskExecutor {
    /// 以任务键索引当前重放意图，周期循环与管理操作共享。
    map: Arc<RwLock<HashMap<String, Arc<dyn RedoTask>>>>,
    /// 执行器所属客户端的诊断标识。
    id: String,
}

impl RedoTaskExecutor {
    /// 业务作用：创建命名操作重放目录，并启动周期检查活跃任务的执行循环。
    ///
    /// 参数说明：
    /// - `id`：任务调度诊断使用的客户端实例标识。
    ///
    /// 返回：空任务目录的执行器；后台循环已启动，首轮约三秒后运行，不等待任何重放完成。
    pub(crate) fn new(id: String) -> Self {
        let executor = Self {
            id,
            map: Arc::new(RwLock::new(HashMap::new())),
        };
        executor.start_schedule();
        executor
    }

    /// 业务作用：启动每三秒检查活跃重放任务的循环，依次执行当前选中的任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务提交后立即结束；循环没有独立停止信号，任务遍历顺序不固定。目录读锁在本轮执行期间保留，添加和移除可能等待远端请求完成。
    fn start_schedule(&self) {
        let _span_enter = debug_span!("RedoTaskExecutor-task", id = self.id).entered();
        debug!("start schedule automatic request task.");
        let map = self.map.clone();
        executor::spawn(
            async move {
                sleep(Duration::from_millis(3000)).await;
                let mut interval = time::interval(Duration::from_millis(3000));
                loop {
                    interval.tick().await;

                    let map = map.read().await;
                    let active_tasks: Vec<Arc<dyn RedoTask>> = map
                        .iter()
                        .filter(|(_, v)| v.is_active())
                        .map(|(_, v)| v.clone())
                        .collect();
                    if !active_tasks.is_empty() {
                        debug!("automatic request task triggered!");
                    }
                    for task in active_tasks {
                        debug!("automatic request task: {:?}", task.task_key());
                        task.run().await;
                    }
                }
            }
            .in_current_span(),
        );
    }

    /// 业务作用：登记当前命名操作的重放意图，以任务键覆盖已有同类任务。
    ///
    /// 参数说明：
    /// - `task`：可共享的任务对象，其任务键决定去重和替换范围。
    ///
    /// 返回：目录更新后结束；不会自动激活任务，也不取消已被执行循环取得的旧任务引用。
    #[instrument(skip_all)]
    pub(crate) async fn add_task(&self, task: Arc<dyn RedoTask>) {
        let mut map = self.map.write().await;
        let task_key = task.task_key();
        map.insert(task_key, task);
    }

    /// 业务作用：撤销目录中的一项重放意图，使后续调度不再选择它。
    ///
    /// 参数说明：
    /// - `task_key`：任务提供的精确身份键。
    ///
    /// 返回：存在时移除，不存在时保持原状；取得写锁可能等待本轮执行，不直接取消已发出的请求。
    #[instrument(fields(task_key = task_key), skip_all)]
    pub(crate) async fn remove_task(&self, task_key: &str) {
        let mut map = self.map.write().await;
        map.remove(task_key);
    }

    /// 业务作用：连接恢复后激活目录中的全部命名意图，让周期任务重新提交注册及订阅。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前目录任务被逐个标记活跃，不立即执行或等待远端恢复完成。
    #[instrument(fields(client_id = &self.id), skip_all)]
    pub(crate) async fn on_grpc_client_reconnect(&self) {
        // 新连接需要重新建立服务端会话状态，曾经成功而冻结的意图也必须重新激活。
        let map = self.map.read().await;
        for v in map.values() {
            v.active()
        }
    }

    /// 业务作用：断连后冻结当前重放意图，避免周期调度继续选择不可用会话上的操作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前目录任务被逐个冻结；已经选中或执行中的任务不由此中断，其完成回调仍可能更新活跃状态。
    #[instrument(fields(client_id = &self.id), skip_all)]
    pub(crate) async fn on_grpc_client_disconnect(&self) {
        // 只冻结调度资格并保留意图，待明确观察到重新连接时再恢复周期提交。
        let map = self.map.read().await;
        for v in map.values() {
            v.frozen()
        }
    }
}

#[async_trait]
pub(crate) trait RedoTask: Send + Sync + 'static {
    /// 业务作用：提供任务在重放目录中的身份，供登记去重及撤销使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由任务实现定义的键，同键登记将替换既有任务，不代表远端请求唯一执行。
    fn task_key(&self) -> String;

    /// 业务作用：撤销任务的周期选择资格，保留意图供之后重新激活。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务进入实现定义的冻结状态；不要求取消已经发出的网络请求。
    fn frozen(&self);

    /// 业务作用：使任务具备后续周期重放资格，用于连接恢复后的状态重建。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务被标记可选择，不在此要求立即执行或保证服务端接受操作。
    fn active(&self);

    /// 业务作用：向调度器提供任务当前是否需要重放的状态快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可选择时为真；读取不保留执行权，状态可能在任务执行前改变。
    fn is_active(&self) -> bool;

    /// 业务作用：执行一次已被调度器选中的重放任务，并按实现更新后续重试状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次执行步骤完成后结束，具体失败通过实现处理；不要求调用前再次检查活跃状态。
    async fn run(&self);
}

type CallBack = Box<dyn Fn(Result<()>) + Send + Sync + 'static>;

#[async_trait]
pub(crate) trait AutomaticRequest: Send + Sync + 'static {
    /// 业务作用：通过指定客户端重放一次命名请求，把结果交给任务状态回调。
    ///
    /// 参数说明：
    /// - `grpc_client`：执行请求的共享连接与认证入口。
    /// - `call_back`：接收本次结果并决定后续重试状态的回调。
    ///
    /// 返回：请求与回调步骤完成后结束，具体成功判定由实现合同决定，不在 trait 层保证业务状态已恢复。
    async fn run(&self, grpc_client: Arc<NacosGrpcClient>, call_back: CallBack);

    /// 业务作用：定义自动请求的重放去重身份，用于关联同一业务意图。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：实现生成的稳定任务键；哪些请求字段参与身份由实现决定，不能假设完整请求正文都参与。
    fn name(&self) -> String;
}

pub(crate) struct NamingRedoTask {
    /// 周期调度是否仍需选择此任务，不是执行互斥或取消令牌。
    active: Arc<AtomicBool>,
    /// 保存待恢复业务意图并定义成功合同的自动请求。
    automatic_request: Arc<dyn AutomaticRequest>,
    /// 重放使用的共享认证连接入口。
    grpc_client: Arc<NacosGrpcClient>,
}

impl NamingRedoTask {
    /// 业务作用：把自动命名请求与客户端绑定为可冻结、可激活的重放任务。
    ///
    /// 参数说明：
    /// - `grpc_client`：后续重放使用的共享客户端。
    /// - `automatic_request`：保留注册或订阅意图的请求实现。
    ///
    /// 返回：初始冻结的任务，不登记到执行器或立即发送请求。
    pub(crate) fn new(
        grpc_client: Arc<NacosGrpcClient>,
        automatic_request: Arc<dyn AutomaticRequest>,
    ) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            automatic_request,
            grpc_client,
        }
    }
}

#[async_trait]
impl RedoTask for NamingRedoTask {
    /// 业务作用：沿用自动请求定义的身份，使任务登记与业务意图撤销使用同一键。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：自动请求生成的任务名称，不附加连接 ID 或新的请求序号。
    fn task_key(&self) -> String {
        self.automatic_request.name()
    }

    /// 业务作用：关闭本任务的周期选择标记，保留原请求以便重连后恢复。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：活跃标记置为假；不取消已发出的请求，其回调仍可能再次写入标记。
    fn frozen(&self) {
        self.active.store(false, Ordering::Release)
    }

    /// 业务作用：标记当前命名意图需要重新提交，供周期执行器后续选中。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：活跃标记置为真，不立即调用远端或修改请求内容。
    fn active(&self) {
        self.active.store(true, Ordering::Release)
    }

    /// 业务作用：读取本任务是否仍等待重放，供调度器筛选任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前原子活跃标记；读取不锁定任务，也不保证执行时状态未变化。
    fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// 业务作用：执行一次命名请求重放，并根据回调结果更新下轮重试资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：自动请求执行完成后结束；回调成功置为冻结，失败置为活跃。不在入口复验活跃标记，也不区分执行中发生的断连代次。
    #[instrument(skip_all)]
    async fn run(&self) {
        let active = self.active.clone();
        self.automatic_request
            .run(
                self.grpc_client.clone(),
                Box::new(move |ret| {
                    // 成功后停止周期重放，失败保留意图；该结果只按自动请求实现的成功合同判定。
                    if ret.is_ok() {
                        active.store(false, Ordering::Release);
                    } else {
                        active.store(true, Ordering::Release);
                    }
                }),
            )
            .await;
    }
}
