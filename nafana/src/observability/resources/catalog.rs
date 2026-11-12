use nametrics_core::{MetricDescriptor, MetricKind};

macro_rules! catalog {
    ($(($id:ident, $name:literal, $help:literal, $kind:ident, $labels:expr)),* $(,)?) => {
        $(pub(super) static $id: MetricDescriptor = MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: MetricKind::$kind,
            label_names: $labels,
            histogram_bounds: &[],
        };)*
        pub(super) static DESCRIPTORS: &[&MetricDescriptor] = &[$(&$id,)* &DURATION];
    };
}
catalog!(
    (
        CPU,
        "napp_process_cpu_microseconds_total",
        "当前进程累计 CPU 时间，微秒。",
        Counter,
        &["mode"]
    ),
    (
        MEMORY,
        "napp_process_memory_bytes",
        "当前进程内存；RSS、虚拟地址和峰值分别报告。",
        Gauge,
        &["kind"]
    ),
    (
        THREADS,
        "napp_process_threads",
        "当前进程线程数。",
        Gauge,
        &[]
    ),
    (
        FDS,
        "napp_process_fds",
        "进程实际打开描述符及有限软上限。",
        Gauge,
        &["kind"]
    ),
    (
        FAULTS,
        "napp_process_page_faults_total",
        "当前进程累计缺页次数。",
        Counter,
        &["kind"]
    ),
    (
        SWITCHES,
        "napp_process_context_switches_total",
        "当前进程累计上下文切换次数。",
        Counter,
        &["kind"]
    ),
    (
        IO_BYTES,
        "napp_process_io_bytes_total",
        "区分系统调用字节与实际存储字节。",
        Counter,
        &["direction", "kind"]
    ),
    (
        IO_OPERATIONS,
        "napp_process_io_operations_total",
        "当前进程累计读写系统调用次数。",
        Counter,
        &["direction"]
    ),
    (
        CG_CPU,
        "napp_cgroup_cpu_microseconds_total",
        "当前进程所属可见 cgroup 累计 CPU 时间，微秒。",
        Counter,
        &["mode"]
    ),
    (
        CG_THROTTLED,
        "napp_cgroup_cpu_throttled_microseconds_total",
        "可见 cgroup 累计 CPU 节流时间，微秒。",
        Counter,
        &[]
    ),
    (
        CG_PERIODS,
        "napp_cgroup_cpu_periods_total",
        "可见 cgroup CPU 调度周期与节流周期。",
        Counter,
        &["kind"]
    ),
    (
        CG_CORES,
        "napp_cgroup_cpu_limit_cores",
        "可见 cgroup 本级有限 CPU 配额核数；无限时不产生样本，祖先约束可能更紧。",
        Gauge,
        &[]
    ),
    (
        CG_MEMORY,
        "napp_cgroup_memory_bytes",
        "可见 cgroup 内存分项与有限上限，分项不能相加为占用。",
        Gauge,
        &["kind"]
    ),
    (
        CG_EVENTS,
        "napp_cgroup_memory_events_total",
        "可见 cgroup 内存压力和 OOM 事件累计次数。",
        Counter,
        &["kind"]
    ),
    (
        CG_PIDS,
        "napp_cgroup_pids",
        "可见 cgroup 进程线程数量与有限上限。",
        Gauge,
        &["kind"]
    ),
    (
        CG_IO_BYTES,
        "napp_cgroup_io_bytes_total",
        "可见 cgroup 白名单设备累计读写字节。",
        Counter,
        &["device", "direction"]
    ),
    (
        CG_IO_OPERATIONS,
        "napp_cgroup_io_operations_total",
        "可见 cgroup 白名单设备累计读写次数。",
        Counter,
        &["device", "direction"]
    ),
    (
        CG_PRESSURE,
        "napp_cgroup_pressure_microseconds_total",
        "可见 cgroup 压力累计等待时间，微秒。",
        Counter,
        &["resource", "kind"]
    ),
    (
        FS_BYTES,
        "napp_filesystem_bytes",
        "白名单文件系统的总量、空闲与当前调用者可用空间。",
        Gauge,
        &["mount", "kind"]
    ),
    (
        FS_INODES,
        "napp_filesystem_inodes",
        "白名单文件系统的 inode 总量与可用数量。",
        Gauge,
        &["mount", "kind"]
    ),
    (
        FS_READONLY,
        "napp_filesystem_read_only",
        "白名单文件系统实际只读挂载状态。",
        Gauge,
        &["mount"]
    ),
    (
        NET_BYTES,
        "napp_network_bytes_total",
        "当前网络命名空间白名单接口累计字节。",
        Counter,
        &["interface", "direction"]
    ),
    (
        NET_PACKETS,
        "napp_network_packets_total",
        "当前网络命名空间白名单接口累计报文数。",
        Counter,
        &["interface", "direction"]
    ),
    (
        NET_ERRORS,
        "napp_network_errors_total",
        "当前网络命名空间白名单接口累计错误数。",
        Counter,
        &["interface", "direction"]
    ),
    (
        NET_DROPS,
        "napp_network_dropped_total",
        "当前网络命名空间白名单接口累计丢弃数。",
        Counter,
        &["interface", "direction"]
    ),
    (
        AVAILABLE,
        "napp_resource_collection_available",
        "资源来源具有未过期成功样本时为一，缺失不表示资源值为零。",
        Gauge,
        &["collector"]
    ),
    (
        STATE,
        "napp_resource_collection_state",
        "资源采集状态，区分未初始化、不支持、不可用、过期与正常。",
        Gauge,
        &["collector", "state"]
    ),
    (
        ERRORS,
        "napp_resource_collection_errors_total",
        "资源来源采样失败累计次数。",
        Counter,
        &["collector"]
    ),
    (
        LAST_SUCCESS,
        "napp_resource_collection_last_success_timestamp_seconds",
        "资源来源最近成功采样的 Unix 秒时间戳。",
        Gauge,
        &["collector"]
    ),
);

pub(super) static DURATION: MetricDescriptor = MetricDescriptor {
    name: "napp_resource_collection_duration_seconds",
    help: "资源采样实际耗时，不在抓取请求内执行文件扫描。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["collector"],
    histogram_bounds: &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0],
};
