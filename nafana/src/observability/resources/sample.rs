use super::{catalog::*, CollectorsConfig};
use nametrics_core::{MetricDescriptor, MetricSample, MetricValue};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Failure {
    Unsupported,
    Unavailable,
    Duplicate,
}
pub(super) type Samples = Result<Vec<MetricSample>, Failure>;
pub(super) struct Collection {
    pub id: String,
    pub started: Instant,
    pub duration: Duration,
    pub samples: Samples,
}

/// 业务作用：把采集值投影到静态指标目录，不允许外部文件生成新指标或标签名。
///
/// 参数说明：
/// - `descriptor`：固定指标合同。
/// - `labels`：按合同排列的有限标签值。
/// - `value`：本次读取的真实指标值。
///
/// 返回：仅包含声明标签的结构化样本。
pub(super) fn metric(
    descriptor: &'static MetricDescriptor,
    labels: &[&str],
    value: MetricValue,
) -> MetricSample {
    MetricSample {
        name: descriptor.name,
        labels: descriptor
            .label_names
            .iter()
            .zip(labels)
            .map(|(key, value)| (*key, (*value).into()))
            .collect(),
        value,
    }
}

/// 业务作用：记录资源的当前量，不用不可用的字段补零。
///
/// 参数说明：
/// - `output`：当前来源的样本。
/// - `descriptor`：静态 gauge 合同。
/// - `labels`：固定标签值。
/// - `value`：实际观测量。
///
/// 返回：追加一个 gauge。
pub(super) fn gauge(
    output: &mut Vec<MetricSample>,
    descriptor: &'static MetricDescriptor,
    labels: &[&str],
    value: f64,
) {
    output.push(metric(descriptor, labels, MetricValue::Gauge(value)));
}

/// 业务作用：保留内核累计量，速率由查询端处理重置后计算。
///
/// 参数说明：
/// - `output`：当前来源的样本。
/// - `descriptor`：静态 counter 合同。
/// - `labels`：固定标签值。
/// - `value`：实际累计量。
///
/// 返回：追加一个 counter。
pub(super) fn counter(
    output: &mut Vec<MetricSample>,
    descriptor: &'static MetricDescriptor,
    labels: &[&str],
    value: u64,
) {
    output.push(metric(descriptor, labels, MetricValue::Counter(value)));
}

/// 业务作用：为独立来源保存耗时与错误，单项不可读不抹掉其它来源。
///
/// 参数说明：
/// - `id`：启动配置确定的来源别名。
/// - `read`：只执行一次的受限读取。
///
/// 返回：包含真实执行时间的来源结果。
fn collect(id: impl Into<String>, read: impl FnOnce() -> Samples) -> Collection {
    let started = Instant::now();
    let samples = read();
    Collection {
        id: id.into(),
        started,
        duration: started.elapsed(),
        samples,
    }
}

/// 业务作用：在唯一阻塞工作中读取白名单资源，磁盘按独立较慢周期采样。
///
/// 参数说明：
/// - `config`：已经验证并冻结的来源及容量。
/// - `filesystem_due`：是否到达文件系统采样周期。
///
/// 返回：本轮有限来源集合；不执行网络请求、命令或递归目录扫描。
pub(super) fn sample(config: &CollectorsConfig, filesystem_due: bool) -> Vec<Collection> {
    let mut output = Vec::new();
    if config.process {
        output.push(collect("process", process));
        output.push(collect("process_memory", || memory(config)));
        output.push(collect("process_fds", || fds(config)));
        output.push(collect("process_io", || process_io(config)));
    }
    #[cfg(target_os = "linux")]
    {
        if config.cgroup {
            super::linux::cgroup(config, &mut output);
        }
        for (alias, name) in &config.network_interfaces {
            output.push(collect(format!("network:{alias}"), || {
                super::linux::network(config, alias, name)
            }));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        if config.cgroup {
            for name in [
                "cgroup_cpu",
                "cgroup_memory",
                "cgroup_pids",
                "cgroup_io",
                "cgroup_pressure",
            ] {
                output.push(collect(name, || Err(Failure::Unsupported)));
            }
        }
        for alias in config.network_interfaces.keys() {
            output.push(collect(format!("network:{alias}"), || {
                Err(Failure::Unsupported)
            }));
        }
    }
    if filesystem_due {
        let mut devices = BTreeSet::new();
        for (alias, path) in &config.filesystems {
            output.push(collect(format!("filesystem:{alias}"), || {
                filesystem(alias, path, &mut devices)
            }));
        }
    }
    output
}

/// 业务作用：读取当前进程累计 CPU、缺页与上下文切换，不借用宿主机总量。
///
/// 参数说明：无。
///
/// 返回：系统支持且调用成功时的累计事实；不可读时显式失败。
#[cfg(unix)]
fn process() -> Samples {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(Failure::Unavailable);
    }
    let usage = unsafe { usage.assume_init() };
    let mut output = Vec::new();
    for (mode, time) in [("user", usage.ru_utime), ("system", usage.ru_stime)] {
        let micros = u64::try_from(time.tv_sec)
            .map_err(|_| Failure::Unavailable)?
            .saturating_mul(1_000_000)
            .saturating_add(u64::try_from(time.tv_usec).map_err(|_| Failure::Unavailable)?);
        counter(&mut output, &CPU, &[mode], micros);
    }
    for (kind, value) in [("minor", usage.ru_minflt), ("major", usage.ru_majflt)] {
        counter(
            &mut output,
            &FAULTS,
            &[kind],
            value.try_into().map_err(|_| Failure::Unavailable)?,
        );
    }
    for (kind, value) in [
        ("voluntary", usage.ru_nvcsw),
        ("involuntary", usage.ru_nivcsw),
    ] {
        counter(
            &mut output,
            &SWITCHES,
            &[kind],
            value.try_into().map_err(|_| Failure::Unavailable)?,
        );
    }
    #[cfg(target_os = "linux")]
    let peak = usage.ru_maxrss as f64 * 1024.0;
    #[cfg(not(target_os = "linux"))]
    let peak = usage.ru_maxrss as f64;
    gauge(&mut output, &MEMORY, &["peak_rss"], peak);
    Ok(output)
}

/// 业务作用：在不支持进程资源调用的平台上声明缺少采集能力。
///
/// 参数说明：无。
///
/// 返回：不产生资源零值。
#[cfg(not(unix))]
fn process() -> Samples {
    Err(Failure::Unsupported)
}

/// 业务作用：读取当前进程驻留、虚拟内存和线程数。
///
/// 参数说明：
/// - `_config`：受限文件读取预算。
///
/// 返回：平台实际字段；不把虚拟地址量解释为物理占用。
fn memory(_config: &CollectorsConfig) -> Samples {
    #[cfg(target_os = "linux")]
    {
        super::linux::memory(_config)
    }
    #[cfg(target_os = "macos")]
    {
        let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::uninit();
        let size = std::mem::size_of::<libc::proc_taskinfo>() as i32;
        let read = unsafe {
            libc::proc_pidinfo(
                std::process::id() as i32,
                libc::PROC_PIDTASKINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if read != size {
            return Err(Failure::Unavailable);
        }
        let info = unsafe { info.assume_init() };
        let mut output = Vec::new();
        gauge(
            &mut output,
            &MEMORY,
            &["rss"],
            info.pti_resident_size as f64,
        );
        gauge(
            &mut output,
            &MEMORY,
            &["virtual"],
            info.pti_virtual_size as f64,
        );
        gauge(&mut output, &THREADS, &[], info.pti_threadnum as f64);
        Ok(output)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(Failure::Unsupported)
}

/// 业务作用：有界枚举当前打开描述符，并独立读取有限软上限。
///
/// 参数说明：
/// - `_config`：目录项容量，超过容量不报告被截断的数量。
///
/// 返回：实际打开量与可用上限；扫描失败不以零值代替。
fn fds(_config: &CollectorsConfig) -> Samples {
    #[cfg(unix)]
    {
        #[cfg(target_os = "linux")]
        let path = "/proc/self/fd";
        #[cfg(not(target_os = "linux"))]
        let path = "/dev/fd";
        let mut count = 0usize;
        for entry in std::fs::read_dir(path).map_err(|_| Failure::Unavailable)? {
            entry.map_err(|_| Failure::Unavailable)?;
            count += 1;
            if count > _config.max_entries {
                return Err(Failure::Unavailable);
            }
        }
        let mut output = Vec::new();
        // 枚举本身占用一个描述符，扣除它后才是采集前进程的近似瞬时量。
        gauge(&mut output, &FDS, &["open"], count.saturating_sub(1) as f64);
        let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0 {
            let limit = unsafe { limit.assume_init() };
            if limit.rlim_cur != libc::RLIM_INFINITY {
                gauge(&mut output, &FDS, &["soft_limit"], limit.rlim_cur as f64);
            }
        }
        Ok(output)
    }
    #[cfg(not(unix))]
    Err(Failure::Unsupported)
}

/// 业务作用：读取内核区分的实际存储和系统调用 I/O 计数。
///
/// 参数说明：
/// - `_config`：受限文件读取预算。
///
/// 返回：支持时的累计事实；其它平台显式不支持。
fn process_io(_config: &CollectorsConfig) -> Samples {
    #[cfg(target_os = "linux")]
    {
        super::linux::process_io(_config)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Failure::Unsupported)
    }
}

/// 业务作用：读取白名单挂载的空间及 inode，按实际设备避免同一文件系统重复相加。
///
/// 参数说明：
/// - `_alias`：固定对外名称，不公开文件路径。
/// - `_path`：配置的绝对路径。
/// - `_seen`：本轮已经读取的设备标识。
///
/// 返回：实际文件系统容量；重复别名明确标为 duplicate，读取失败不输出零容量。
fn filesystem(_alias: &str, _path: &str, _seen: &mut BTreeSet<u64>) -> Samples {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let device = std::fs::metadata(_path)
            .map_err(|_| Failure::Unavailable)?
            .dev();
        if _seen.contains(&device) {
            return Err(Failure::Duplicate);
        }
        let path = std::ffi::CString::new(_path).map_err(|_| Failure::Unavailable)?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(Failure::Unavailable);
        }
        let stat = unsafe { stat.assume_init() };
        _seen.insert(device);
        let mut output = Vec::new();
        for (kind, blocks) in [
            ("size", stat.f_blocks),
            ("free", stat.f_bfree),
            ("available", stat.f_bavail),
        ] {
            gauge(
                &mut output,
                &FS_BYTES,
                &[_alias, kind],
                blocks as f64 * stat.f_frsize as f64,
            );
        }
        for (kind, value) in [
            ("size", stat.f_files),
            ("free", stat.f_ffree),
            ("available", stat.f_favail),
        ] {
            gauge(&mut output, &FS_INODES, &[_alias, kind], value as f64);
        }
        gauge(
            &mut output,
            &FS_READONLY,
            &[_alias],
            if stat.f_flag & libc::ST_RDONLY != 0 {
                1.0
            } else {
                0.0
            },
        );
        Ok(output)
    }
    #[cfg(not(unix))]
    Err(Failure::Unsupported)
}

/// 业务作用：由平台采集器复用相同的来源结局与耗时规则。
///
/// 参数说明：
/// - `id`：固定来源标识。
/// - `read`：受限平台读取。
///
/// 返回：可合并的单来源观测。
#[cfg(target_os = "linux")]
pub(super) fn platform_collect(id: &str, read: impl FnOnce() -> Samples) -> Collection {
    collect(id, read)
}
