use std::{
    any::Any,
    collections::{BTreeMap, HashSet},
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 子排空为报告生成与后续逆序清理保留的最小尾部预算。
const MIN_SHUTDOWN_TAIL_RESERVE: Duration = Duration::from_millis(100);
/// 尾部预算上限，避免长停机窗口过度压缩当前子系统的正常排空时间。
const MAX_SHUTDOWN_TAIL_RESERVE: Duration = Duration::from_secs(1);

/// 运行期统一识别的进程终止信号。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// 终端 Ctrl-C 或等价中断信号。
    CtrlC,
    /// 进程终止信号。
    Terminate,
    /// 显式中断信号。
    Interrupt,
}

impl ShutdownSignal {
    /// 业务作用：返回信号对应的进程退出码。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值遵循常用的 `128 + signal number` 约定。
    pub fn exit_code(self) -> u8 {
        match self {
            Self::CtrlC | Self::Interrupt => 130,
            Self::Terminate => 143,
        }
    }

    /// 业务作用：返回适合固定诊断标记使用的信号名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不包含业务输入。
    pub fn name(self) -> &'static str {
        match self {
            Self::CtrlC => "SIGINT",
            Self::Terminate => "SIGTERM",
            Self::Interrupt => "SIGINT",
        }
    }
}

/// 传递给资源和 action 的首次停机原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownReason {
    /// 管理入口或内部控制面主动请求停机。
    Requested,
    /// 操作系统信号触发停机。
    Signal(ShutdownSignal),
    /// Batch 应用正常完成。
    BatchCompleted,
    /// 关键受管任务异常退出。
    CriticalTaskFailed,
    /// 应用启动过程失败。
    StartupFailed,
    /// 内置组件在运行或停机阶段失败。
    ComponentFailed,
}

/// 将业务 shutdown task 的常见返回形状统一转换为框架错误链。
#[doc(hidden)]
pub trait ShutdownTaskOutput {
    /// 业务作用：把任务的业务返回值归一为可脱敏、可汇总的停机结果。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回 `Ok(())` 表示该任务完成；返回错误时只影响该任务的终态，后续任务仍会执行。
    fn into_shutdown_task_result(self) -> Result<(), anyhow::Error>;
}

impl ShutdownTaskOutput for () {
    /// 业务作用：把无返回值的业务停机任务归类为成功完成。
    ///
    /// 参数说明：无。
    ///
    /// 返回：始终返回 `Ok(())`，表示任务已完成。
    fn into_shutdown_task_result(self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl<E> ShutdownTaskOutput for Result<(), E>
where
    E: Into<anyhow::Error>,
{
    /// 业务作用：把可失败业务停机任务的错误转换为统一错误链。
    ///
    /// 参数说明：无。
    ///
    /// 返回：原结果成功时返回 `Ok(())`；失败时保留转换后的底层错误供停机报告脱敏输出。
    fn into_shutdown_task_result(self) -> Result<(), anyhow::Error> {
        self.map_err(Into::into)
    }
}

/// 已完成类型擦除的业务停机任务；Runner 只负责 poll、限时和归一化结果。
pub(crate) type ShutdownTaskFuture =
    Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'static>>;

/// 从登记接管到释放持续持有任务所有权，移交注册表或执行器时不撤销析构隔离。
pub(crate) struct ShutdownTaskCleanup {
    task: Option<ShutdownTaskFuture>,
}

impl ShutdownTaskCleanup {
    /// 业务作用：接管已通过登记门的 future，为尚未执行和执行中的任务建立相同释放边界。
    ///
    /// 参数说明：`task` 为已完成类型擦除、尚未执行的业务停机任务。
    ///
    /// 返回：持有唯一释放权的守卫，不执行或移动已固定的 future 本体。
    fn new(task: ShutdownTaskFuture) -> Self {
        Self { task: Some(task) }
    }

    /// 业务作用：在独立展开边界内一次性释放任务，供正常停机路径累计析构失败。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本次析构发生 panic 时返回 true；正常或重复释放返回 false，不读取异常正文。
    pub(crate) fn release(&mut self) -> bool {
        // 先撤销所有权再执行业务析构，确保异常后的守卫 Drop 不会重复释放同一 future。
        self.task.take().is_some_and(release_shutdown_task)
    }
}

impl Future for ShutdownTaskCleanup {
    type Output = Result<(), anyhow::Error>;

    /// 业务作用：借用守卫内的固定 future 执行业务任务，poll 期间仍由守卫承担取消时的释放责任。
    ///
    /// 参数说明：`context` 为当前执行器提供的唤醒上下文。
    ///
    /// 返回：透传业务任务的 Pending 或完成结果；显式释放后再次 poll 属于内部状态错误。
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .task
            .as_mut()
            .expect("shutdown task ownership is retained")
            .as_mut()
            .poll(context)
    }
}

impl Drop for ShutdownTaskCleanup {
    /// 业务作用：在注册表或执行器直接释放时隔离任务析构，允许其余已接管任务继续释放。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；无法纳入退出报告的异常同步输出固定告警，不伪造或追改停机摘要。
    fn drop(&mut self) {
        if self.release() {
            crate::report::report_shutdown(&shutdown_task_error(
                ApplicationPhase::Stopping,
                "business shutdown task panicked while being released",
            ));
        }
    }
}

/// 业务作用：在独立的展开边界内释放业务 future，防止取消或放弃时的析构异常截断后续清理。
///
/// 参数说明：
/// - `task`：已经停止 poll、需要释放其捕获资源的业务 future。
///
/// 返回：析构发生 panic 时返回 `true`，否则返回 `false`；异常对象在新的展开边界内释放，不读取其正文。
pub(crate) fn release_shutdown_task(task: ShutdownTaskFuture) -> bool {
    match catch_unwind(AssertUnwindSafe(|| drop(task))) {
        Ok(()) => false,
        Err(payload) => {
            release_shutdown_panic_payload(payload);
            true
        }
    }
}

/// 业务作用：释放已捕获的生命周期异常对象，使正常析构的 payload 不会永久占用业务资源。
///
/// 参数说明：
/// - `payload`：业务 poll、错误报告或对象析构时产生的异常对象，其正文不进入诊断通道。
///
/// 返回：无返回值；尝试析构首次对象。若析构再次 panic，则只保留第二个异常对象，避免无限展开。
pub(crate) fn release_shutdown_panic_payload(payload: Box<dyn Any + Send>) {
    // 已退出首次展开才执行 payload 的析构；独立边界防止业务析构异常截断后续停机步骤。
    if let Err(nested_payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // 不递归运行任意层数的异常析构；仅此二次异常对象及其持有资源可能保留至进程结束。
        std::mem::forget(nested_payload);
    }
}

/// 单个 Application 内业务停机任务登记的最大数量。
pub(crate) const MAX_GRACEFUL_SHUTDOWN_TASKS: usize = 256;
/// 业务停机任务名称允许占用的最大 UTF-8 字节数。
pub(crate) const MAX_GRACEFUL_SHUTDOWN_TASK_NAME_BYTES: usize = 128;

/// 业务停机任务集合的同步生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownTaskPhase {
    Open,
    Sealed,
    Running,
    Closed,
}

/// 一项已经通过登记门、等待 Runner 执行的业务停机任务。
pub(crate) struct ShutdownTaskEntry {
    pub(crate) name: Arc<str>,
    pub(crate) priority: i32,
    pub(crate) task: ShutdownTaskCleanup,
}

struct ShutdownTaskRegistryState {
    phase: ShutdownTaskPhase,
    next_sequence: u64,
    names: HashSet<Arc<str>>,
    tasks: BTreeMap<(i32, u64), ShutdownTaskEntry>,
}

/// 由 Application 唯一持有的业务停机任务注册表。
pub(crate) struct ShutdownTaskRegistry {
    state: Mutex<ShutdownTaskRegistryState>,
}

impl ShutdownTaskRegistry {
    /// 业务作用：创建只接受 UserHook 登记的空业务停机任务集合。
    ///
    /// 参数说明：无。
    ///
    /// 返回：处于 `Open` 阶段、尚未持有任何业务 future 的注册表。
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ShutdownTaskRegistryState {
                phase: ShutdownTaskPhase::Open,
                next_sequence: 0,
                names: HashSet::new(),
                tasks: BTreeMap::new(),
            }),
        }
    }

    /// 业务作用：按稳定优先级和登记序号原子接收一项业务停机任务。
    ///
    /// 参数说明：
    /// - `priority`：业务任务集合内的执行优先级，数值越小越早执行。
    /// - `name`：应用内唯一、已转换为共享字符串的任务身份。
    /// - `task`：已经完成类型擦除的待移交任务槽；只有登记成功才取走所有权。
    ///
    /// 返回：任务完整写入集合时成功；阶段已封口、名称非法、重名、超出数量上限、序号耗尽或任务槽为空时失败。
    pub(crate) fn register(
        &self,
        priority: i32,
        name: Arc<str>,
        task: &mut Option<ShutdownTaskFuture>,
    ) -> ApplicationResult<()> {
        let name = normalize_shutdown_task_name(name)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.phase != ShutdownTaskPhase::Open {
            return Err(shutdown_task_error(
                ApplicationPhase::UserHook,
                "graceful shutdown task registration is closed",
            ));
        }
        if state.names.contains(&name) {
            return Err(shutdown_task_error(
                ApplicationPhase::UserHook,
                "graceful shutdown task name is already registered",
            ));
        }
        if state.tasks.len() >= MAX_GRACEFUL_SHUTDOWN_TASKS {
            return Err(shutdown_task_error(
                ApplicationPhase::UserHook,
                "graceful shutdown task limit has been reached",
            ));
        }
        let sequence = state.next_sequence;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            shutdown_task_error(
                ApplicationPhase::UserHook,
                "graceful shutdown task registration sequence is exhausted",
            )
        })?;
        // 拒绝登记时 future 仍由调用方持有，不能在外层登记门尚未释放时运行其析构代码。
        let task = task.take().ok_or_else(|| {
            shutdown_task_error(
                ApplicationPhase::UserHook,
                "graceful shutdown task ownership is missing",
            )
        })?;
        // 从接管开始持续保护析构；任务移交待执行列表或当前 poll 项时仍保留同一个守卫。
        let task = ShutdownTaskCleanup::new(task);
        state.next_sequence = next_sequence;

        // 名称、序号和 future 在同一把锁内一起发布；调用方收到成功后，任务一定能被停机路径取得。
        state.names.insert(name.clone());
        state.tasks.insert(
            (priority, sequence),
            ShutdownTaskEntry {
                name,
                priority,
                task,
            },
        );
        Ok(())
    }

    /// 业务作用：冻结登记集合，使 UserHook 结束后没有新任务可以进入停机计划。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首次从 `Open` 进入 `Sealed` 或重复调用时成功；执行已经开始时保持幂等。
    pub(crate) fn seal(&self) -> ApplicationResult<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.phase == ShutdownTaskPhase::Open {
            state.phase = ShutdownTaskPhase::Sealed;
        }
        Ok(())
    }

    /// 业务作用：一次性取得按 `(priority ASC, registration_sequence ASC)` 排列的任务所有权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首次执行取得冻结集合；重复执行返回空集合；未封口时返回阶段错误。锁在返回前释放，
    /// 业务 future 的 poll 与析构都不会阻塞注册表状态读取。
    pub(crate) fn take_for_shutdown(&self) -> ApplicationResult<Vec<ShutdownTaskEntry>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.phase {
            ShutdownTaskPhase::Open => Err(shutdown_task_error(
                ApplicationPhase::Stopping,
                "graceful shutdown task registry must be sealed before execution",
            )),
            ShutdownTaskPhase::Sealed => {
                state.phase = ShutdownTaskPhase::Running;
                Ok(std::mem::take(&mut state.tasks).into_values().collect())
            }
            ShutdownTaskPhase::Running | ShutdownTaskPhase::Closed => Ok(Vec::new()),
        }
    }

    /// 业务作用：结束任务集合生命周期并释放仍由注册表持有的 future。
    ///
    /// 参数说明：无。
    ///
    /// 返回：收集仍持有任务的析构异常；任何阶段重复调用都安全，future 在离开同步锁后逐项释放。
    pub(crate) fn close(&self) -> Vec<ApplicationError> {
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.phase = ShutdownTaskPhase::Closed;
            state.names.clear();
            std::mem::take(&mut state.tasks)
        };
        let mut failures = Vec::new();
        for mut entry in tasks.into_values() {
            if entry.task.release() {
                failures.push(shutdown_task_error(
                    ApplicationPhase::Stopping,
                    format!(
                        "business shutdown task `{}` panicked while being released",
                        entry.name
                    ),
                ));
            }
        }
        failures
    }
}

/// 业务停机任务单项结果的稳定分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownTaskOutcome {
    Completed,
    Failed,
    TimedOut,
    Panicked,
    Abandoned,
}

/// 业务停机任务执行的低基数计数报告。
#[derive(Debug, Default)]
pub(crate) struct ShutdownTaskReport {
    pub(crate) registered: usize,
    pub(crate) attempted: usize,
    pub(crate) completed: usize,
    pub(crate) failed: usize,
    pub(crate) timed_out: usize,
    pub(crate) panicked: usize,
    pub(crate) abandoned: usize,
}

impl ShutdownTaskReport {
    /// 业务作用：记录一个业务停机任务已经取得 poll 机会后的稳定终态。
    ///
    /// 参数说明：
    /// - `outcome`：Runner 归一化后的任务终态。
    ///
    /// 返回：无返回值；计数保持各终态之和等于实际尝试数。
    pub(crate) fn record(&mut self, outcome: ShutdownTaskOutcome) {
        self.record_count(outcome, 1);
    }

    /// 业务作用：记录尚未取得 poll 机会、因业务任务组预算耗尽而释放的任务数量。
    ///
    /// 参数说明：
    /// - `count`：未执行任务的数量。
    ///
    /// 返回：无返回值；放弃项只进入 `abandoned`，不会伪装为已尝试。
    pub(crate) fn record_abandoned(&mut self, count: usize) {
        self.record_count(ShutdownTaskOutcome::Abandoned, count);
    }

    /// 业务作用：按任务终态批量更新报告，并维持“实际尝试数 + 放弃数 = 登记数”的计数边界。
    ///
    /// 参数说明：
    /// - `outcome`：需要累计的稳定终态。
    /// - `count`：该终态对应的任务数量。
    ///
    /// 返回：无返回值；`Abandoned` 不增加 attempted，因为对应 future 从未被 poll。
    fn record_count(&mut self, outcome: ShutdownTaskOutcome, count: usize) {
        match outcome {
            ShutdownTaskOutcome::Completed => {
                self.attempted += count;
                self.completed += count;
            }
            ShutdownTaskOutcome::Failed => {
                self.attempted += count;
                self.failed += count;
            }
            ShutdownTaskOutcome::TimedOut => {
                self.attempted += count;
                self.timed_out += count;
            }
            ShutdownTaskOutcome::Panicked => {
                self.attempted += count;
                self.panicked += count;
            }
            ShutdownTaskOutcome::Abandoned => self.abandoned += count,
        }
    }
}

/// 业务作用：规范化并校验业务停机任务的稳定诊断名称。
///
/// 参数说明：
/// - `name`：业务传入的任务名称；首尾空白会被去除。
///
/// 返回：返回可安全放入名称集合的共享字符串；空白、超长、跨行、含控制/不可见格式字符或明显敏感/高基数结构时返回
/// UserHook 错误。
fn normalize_shutdown_task_name(name: Arc<str>) -> ApplicationResult<Arc<str>> {
    // 跨行、双向控制和不可见格式会破坏单行事件身份；在 trim 和比较归一之前拒绝，不能静默隐藏。
    if name.chars().any(|character| {
        character.is_control()
            || matches!(character, '\u{2028}' | '\u{2029}')
            || crate::report::is_invisible_diagnostic_character(character)
    }) {
        return Err(shutdown_task_error(
            ApplicationPhase::UserHook,
            "graceful shutdown task name cannot contain control, line separator, or invisible format characters",
        ));
    }
    let name = name.trim();
    if name.is_empty() {
        return Err(shutdown_task_error(
            ApplicationPhase::UserHook,
            "graceful shutdown task name cannot be empty",
        ));
    }
    if name.len() > MAX_GRACEFUL_SHUTDOWN_TASK_NAME_BYTES {
        return Err(shutdown_task_error(
            ApplicationPhase::UserHook,
            "graceful shutdown task name exceeds 128 UTF-8 bytes",
        ));
    }
    // 任务名会进入诊断事件和失败摘要；地址、身份/凭据语义及动态内容不能进入低基数诊断面。
    if shutdown_task_name_has_unstable_content(name) {
        return Err(shutdown_task_error(
            ApplicationPhase::UserHook,
            "graceful shutdown task name contains URL, credential, or high-cardinality content",
        ));
    }
    Ok(Arc::from(name))
}

/// 业务作用：识别会把业务停机事件变成敏感或高基数诊断数据的明显名称形态。
///
/// 参数说明：
/// - `name`：已经完成空白、长度和控制字符校验的候选任务名。
///
/// 返回：包含 URL/地址/赋值结构、凭据语义词、明确身份字段值、长数字或随机标识时返回 `true`；
/// 普通业务词不依赖封闭的操作词白名单。
fn shutdown_task_name_has_unstable_content(name: &str) -> bool {
    use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

    let (comparison, _) = crate::report::diagnostic_comparison(name);
    let name = comparison.as_str();
    let lowercase = name.to_ascii_lowercase();
    if name
        .chars()
        .any(|character| matches!(character, '=' | '＝' | '?' | '#' | '@'))
        || name.contains("://")
        || crate::report::is_sensitive_diagnostic_name(name)
    {
        return true;
    }

    // 地址会把节点身份带入诊断字段；候选必须保持完整词边界，不能从普通命名空间中截取十六进制子串。
    if shutdown_task_name_has_address(name) {
        return true;
    }

    let words = crate::report::diagnostic_words(name);
    // 显式身份字段后携带值时拒绝；不能仅凭普通单词前缀或未知操作词猜测业务身份。
    if words
        .windows(2)
        .any(|pair| matches!(pair[0].as_str(), "id" | "uid" | "uuid") && !pair[1].is_empty())
    {
        return true;
    }

    // 大写身份键拼接值采用保守门禁；TitleCase 只补充数字和非 ASCII 字母数字边界，不能把 Identity 拆成 Id + entity。
    // 不按任意子串或英文词表建立例外；未携带值的身份键仍可用于稳定名称。
    for (index, _) in name.char_indices() {
        let at_boundary = index == 0
            || name[..index].chars().next_back().is_some_and(|previous| {
                !previous.is_alphanumeric() || previous.is_lowercase() || previous.is_numeric()
            });
        if at_boundary
            && ["ID", "UID", "UUID", "Id", "Uid", "Uuid"]
                .iter()
                .any(|marker| {
                    name[index..]
                        .strip_prefix(marker)
                        .and_then(|suffix| suffix.chars().next())
                        .is_some_and(|character| {
                            character.is_numeric()
                                || (!character.is_ascii() && character.is_alphanumeric())
                                || (marker.bytes().all(|byte| byte.is_ascii_uppercase())
                                    && character.is_ascii_lowercase())
                        })
                })
        {
            return true;
        }
    }

    // Unicode 十进制数字统一计数，混合书写系统不能绕过动态形态门禁；短编号仍可用于固定协议或角色。
    let mut digits = 0;
    for character in name.chars() {
        digits = if character.general_category() == GeneralCategory::DecimalNumber {
            digits + 1
        } else {
            0
        };
        if digits >= 4 {
            return true;
        }
    }
    if name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|part| {
            part.len() >= 16
                && part.bytes().filter(u8::is_ascii_digit).count() >= 4
                && part.bytes().any(|byte| byte.is_ascii_alphabetic())
        })
    {
        return true;
    }

    shutdown_task_name_contains_uuid(&lowercase)
        || shutdown_task_name_contains_ulid(&lowercase)
        || shutdown_task_name_contains_long_hex(&lowercase)
}

/// 业务作用：识别名称中的完整地址候选，避免节点地址进入稳定诊断身份。
///
/// 参数说明：
/// - `name`：已完成长度及控制字符校验的候选任务名。
///
/// 返回：包含四段十进制 IPv4 形态或完整可解析 IPv6 候选时返回 `true`；IPv4 允许前导零，
/// IPv6 允许非空 zone 标识，但不会从普通单词中截取十六进制子串。
fn shutdown_task_name_has_address(name: &str) -> bool {
    name.split(|character: char| !character.is_ascii_digit() && character != '.')
        .any(|part| {
            let octets = part.trim_matches('.').split('.');
            octets.clone().count() == 4
                && octets.into_iter().all(|octet| {
                    !octet.is_empty()
                        && octet.len() <= 3
                        && octet.bytes().all(|byte| byte.is_ascii_digit())
                        && octet.parse::<u8>().is_ok()
                })
        })
        || name
            .split(|character: char| {
                !character.is_alphanumeric() && !matches!(character, ':' | '.' | '%')
            })
            .any(|part| {
                let address = match part.split_once('%') {
                    Some((address, zone)) if !zone.is_empty() => address,
                    Some(_) => return false,
                    None => part,
                };
                address.parse::<std::net::Ipv6Addr>().is_ok()
            })
}

/// 业务作用：在完整任务名中识别带连字符的 UUID，防止前缀、后缀或括号遮蔽动态标识。
///
/// 参数说明：
/// - `name`：已经转换为 ASCII 小写的候选任务名。
///
/// 返回：任意位置出现标准五段 UUID 结构时返回 `true`；否则返回 `false`。
fn shutdown_task_name_contains_uuid(name: &str) -> bool {
    name.as_bytes().windows(36).any(|window| {
        [8, 13, 18, 23]
            .into_iter()
            .all(|index| window[index] == b'-')
            && window
                .iter()
                .enumerate()
                .all(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) || byte.is_ascii_hexdigit())
    })
}

/// 业务作用：识别规范 ULID 的 26 字符编码，避免时间前缀与随机部分组合成高基数任务名。
///
/// 参数说明：
/// - `name`：已经转换为 ASCII 小写的候选任务名。
///
/// 返回：任意位置出现首字符为 `0` 至 `7`、其余字符符合 Crockford Base32 的 26 字符 ULID 时返回
/// `true`；否则返回 `false`。
fn shutdown_task_name_contains_ulid(name: &str) -> bool {
    name.as_bytes().windows(26).any(|window| {
            matches!(window[0], b'0'..=b'7')
            && window[1..].iter().all(|byte| {
                byte.is_ascii_digit()
                    || matches!(*byte, b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z')
            })
    })
}

/// 业务作用：识别名称中嵌入的长十六进制串，覆盖无分隔符拼接或被普通前后缀包裹的随机标识。
///
/// 参数说明：
/// - `name`：已经转换为 ASCII 小写的候选任务名。
///
/// 返回：连续十六进制字符达到 16 个时返回 `true`；否则返回 `false`。
fn shutdown_task_name_contains_long_hex(name: &str) -> bool {
    let mut run_length = 0;
    for byte in name.bytes() {
        if byte.is_ascii_hexdigit() {
            run_length += 1;
            if run_length >= 16 {
                return true;
            }
        } else {
            run_length = 0;
        }
    }
    false
}

/// 业务作用：创建业务停机注册表使用的稳定阶段错误。
///
/// 参数说明：
/// - `phase`：错误被观察到的生命周期阶段。
/// - `message`：不包含业务名称、配置值或底层错误文本的固定摘要。
///
/// 返回：带有 Application 组件归属的公开生命周期错误。
fn shutdown_task_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, phase, message)
}

/// 传给资源和组件的统一停机预算。子步骤只能消费剩余时间，不能重置 deadline。
#[derive(Debug, Clone)]
pub struct ShutdownContext {
    deadline: Instant,
    reason: ShutdownReason,
}

impl ShutdownContext {
    /// 业务作用：创建共享同一绝对截止时间的停机上下文。
    ///
    /// # 参数
    ///
    /// - `deadline`：所有清理步骤都不能突破的绝对时间点。
    /// - `reason`：由首次终止意图映射出的稳定停机原因。
    pub fn new(deadline: Instant, reason: ShutdownReason) -> Self {
        Self { deadline, reason }
    }

    /// 业务作用：返回全局停机截止时间。
    ///
    /// # 参数
    ///
    /// 本方法无参数；子步骤不得据此创建新的完整预算。
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// 业务作用：返回首次停机原因。
    ///
    /// # 参数
    ///
    /// 本方法无参数；后续清理错误不会改变该值。
    pub fn reason(&self) -> &ShutdownReason {
        &self.reason
    }

    /// 业务作用：计算当前步骤还能消费的剩余预算。
    ///
    /// # 参数
    ///
    /// 本方法无参数；过期后饱和为零而不会回绕。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// 业务作用：为可能用满自身预算的子排空计算提前收口时长，避免与全局 deadline 同时到期。
    ///
    /// 参数说明：
    /// - `requested`: 当前子系统按自身合同允许消费的最长时长。
    ///
    /// 返回：不超过请求值与全局剩余预算；通常保留余量的 10%，且保留值处于 100ms 到 1s
    /// 之间。全局余量不足 100ms 时全部留给收尾，调用方应立即进入有损或强制收口。
    pub fn child_budget(&self, requested: Duration) -> Duration {
        let remaining = self.remaining();
        let reserve = (remaining / 10)
            .max(MIN_SHUTDOWN_TAIL_RESERVE)
            .min(MAX_SHUTDOWN_TAIL_RESERVE)
            .min(remaining);
        requested.min(remaining.saturating_sub(reserve))
    }

    /// 业务作用：为单个 Runner action 创建共享原因但提前截止的子上下文，隔离后续逆序清理预算。
    ///
    /// 参数说明：
    /// - `requested`: 当前 action 按自身合同允许消费的最长时长。
    ///
    /// 返回：deadline 不晚于父上下文安全子预算、停机原因保持不变的新上下文。
    pub(crate) fn child_context(&self, requested: Duration) -> Self {
        Self::new(
            Instant::now() + self.child_budget(requested),
            self.reason.clone(),
        )
    }

    /// 业务作用：在已经完成组级尾部预留的停机上下文内创建公平任务子预算。
    ///
    /// 参数说明：
    /// - `requested`：当前任务按剩余任务数分得的最大时长。
    ///
    /// 返回：不晚于当前上下文截止时间的子上下文；该派生不会再次扣除尾部预算，避免多个公平任务
    /// 叠加保留同一段收尾时间而在短停机窗口内全部失去执行机会。
    pub(crate) fn fair_child_context(&self, requested: Duration) -> Self {
        let now = Instant::now();
        let requested_deadline = now.checked_add(requested).unwrap_or(self.deadline);
        Self::new(requested_deadline.min(self.deadline), self.reason.clone())
    }

    /// 业务作用：判断全局清理预算是否已经耗尽。
    ///
    /// # 参数
    ///
    /// 本方法无参数；结果来自同一绝对 deadline。
    pub fn is_expired(&self) -> bool {
        self.remaining().is_zero()
    }
}
