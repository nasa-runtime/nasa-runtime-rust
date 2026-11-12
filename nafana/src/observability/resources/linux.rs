use super::{
    catalog::*,
    sample::{counter, gauge, platform_collect, Collection, Failure, Samples},
    CollectorsConfig,
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
};

/// 业务作用：限定内核文本读取体积，异常挂载不能制造无界内存增长。
///
/// 参数说明：
/// - `path`：只由固定内核路径或已经验证的 cgroup 路径生成。
/// - `config`：单文件读取上限。
///
/// 返回：完整 UTF-8 文本；截断、权限或格式错误均拒绝。
fn read(path: impl AsRef<Path>, config: &CollectorsConfig) -> Result<String, Failure> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| Failure::Unavailable)?
        .take(config.max_file_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::Unavailable)?;
    if bytes.len() > config.max_file_bytes {
        return Err(Failure::Unavailable);
    }
    String::from_utf8(bytes).map_err(|_| Failure::Unavailable)
}

/// 业务作用：区分内核未提供的可选统计文件与读取权限故障。
///
/// 参数说明：
/// - `path`：当前 cgroup 的固定统计文件。
/// - `config`：单文件容量上限。
///
/// 返回：不存在时 None，其它读取失败保留 unavailable。
fn optional(path: impl AsRef<Path>, config: &CollectorsConfig) -> Result<Option<String>, Failure> {
    match std::fs::metadata(path.as_ref()) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Failure::Unavailable),
        Ok(_) => read(path, config).map(Some),
    }
}

/// 业务作用：严格读取内核十进制累计量，拒绝负数或非数值字段。
///
/// 参数说明：
/// - `value`：单个内核字段。
///
/// 返回：完整非负整数。
fn number(value: &str) -> Result<u64, Failure> {
    value.parse().map_err(|_| Failure::Unavailable)
}

/// 业务作用：解析有界键值统计文本，重复键不可覆盖原始证据。
///
/// 参数说明：
/// - `text`：完整内核文本。
///
/// 返回：借用原文本的唯一键值映射。
fn fields(text: &str) -> Result<BTreeMap<&str, &str>, Failure> {
    let mut output = BTreeMap::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let key = fields
            .next()
            .ok_or(Failure::Unavailable)?
            .trim_end_matches(':');
        let value = fields.next().ok_or(Failure::Unavailable)?;
        if output.insert(key, value).is_some() {
            return Err(Failure::Unavailable);
        }
    }
    Ok(output)
}

/// 业务作用：取得当前进程的实际内存分项及线程数。
///
/// 参数说明：
/// - `config`：单文件容量限制。
///
/// 返回：RSS 和虚拟地址按内核 KiB 换算，线程按数量报告。
pub(super) fn memory(config: &CollectorsConfig) -> Samples {
    let text = read("/proc/self/status", config)?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if !["VmRSS", "VmSize", "Threads"].contains(&key) {
            continue;
        }
        let mut parts = value.split_whitespace();
        let value = parts.next().ok_or(Failure::Unavailable)?;
        if key != "Threads" && parts.next() != Some("kB") {
            return Err(Failure::Unavailable);
        }
        if parts.next().is_some() || values.insert(key, value).is_some() {
            return Err(Failure::Unavailable);
        }
    }
    let mut output = Vec::new();
    for (key, kind) in [("VmRSS", "rss"), ("VmSize", "virtual")] {
        gauge(
            &mut output,
            &MEMORY,
            &[kind],
            number(values.get(key).ok_or(Failure::Unavailable)?)? as f64 * 1024.0,
        );
    }
    gauge(
        &mut output,
        &THREADS,
        &[],
        number(values.get("Threads").ok_or(Failure::Unavailable)?)? as f64,
    );
    Ok(output)
}

/// 业务作用：保留系统调用与实际存储 I/O 的独立计数范围。
///
/// 参数说明：
/// - `config`：单文件容量限制。
///
/// 返回：实际内核累计量，不从缓存读取字节推导设备操作数。
pub(super) fn process_io(config: &CollectorsConfig) -> Samples {
    let text = read("/proc/self/io", config)?;
    let values = fields(&text)?;
    let mut output = Vec::new();
    for (key, direction, kind) in [
        ("rchar", "read", "syscall"),
        ("wchar", "write", "syscall"),
        ("read_bytes", "read", "storage"),
        ("write_bytes", "write", "storage"),
    ] {
        counter(
            &mut output,
            &IO_BYTES,
            &[direction, kind],
            number(values.get(key).ok_or(Failure::Unavailable)?)?,
        );
    }
    for (key, direction) in [("syscr", "read"), ("syscw", "write")] {
        counter(
            &mut output,
            &IO_OPERATIONS,
            &[direction],
            number(values.get(key).ok_or(Failure::Unavailable)?)?,
        );
    }
    Ok(output)
}

/// 业务作用：解码 mountinfo 路径，不接受穿越或相对路径。
///
/// 参数说明：
/// - `value`：mountinfo 中的已转义绝对路径。
///
/// 返回：规范组件路径；异常转义和父目录组件拒绝。
fn mount_path(value: &str) -> Result<PathBuf, Failure> {
    let mut bytes = Vec::new();
    let mut input = value.bytes();
    while let Some(byte) = input.next() {
        if byte == b'\\' {
            let digits = [input.next(), input.next(), input.next()];
            let mut escaped = 0u16;
            for digit in digits {
                let digit = digit.ok_or(Failure::Unavailable)?;
                if !(b'0'..=b'7').contains(&digit) {
                    return Err(Failure::Unavailable);
                }
                escaped = escaped * 8 + (digit - b'0') as u16;
            }
            bytes.push(u8::try_from(escaped).map_err(|_| Failure::Unavailable)?);
        } else {
            bytes.push(byte);
        }
    }
    let value = String::from_utf8(bytes).map_err(|_| Failure::Unavailable)?;
    let path = PathBuf::from(&value);
    if !path.is_absolute()
        || value.chars().any(char::is_control)
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(Failure::Unavailable);
    }
    Ok(path)
}

/// 业务作用：定位当前进程所属可见 cgroup v2，不回退为宿主机容量。
///
/// 参数说明：
/// - `config`：内核文本读取预算。
///
/// 返回：唯一可解释的当前组目录；旧布局或没有统一层级明确不支持。
fn directory(config: &CollectorsConfig) -> Result<PathBuf, Failure> {
    let membership = read("/proc/self/cgroup", config)?;
    let memberships: Vec<_> = membership
        .lines()
        .filter_map(|line| line.strip_prefix("0::"))
        .collect();
    if memberships.is_empty() {
        return Err(Failure::Unsupported);
    }
    if memberships.len() != 1 {
        return Err(Failure::Unavailable);
    }
    let relative = mount_path(memberships[0])?;
    let mounts = read("/proc/self/mountinfo", config)?;
    let mut candidates = Vec::new();
    for line in mounts.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            return Err(Failure::Unavailable);
        };
        if right.split_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let fields: Vec<_> = left.split_whitespace().collect();
        if fields.len() < 6 {
            return Err(Failure::Unavailable);
        }
        let root = mount_path(fields[3])?;
        let mounted = mount_path(fields[4])?;
        // namespace 内的根路径是 /；非根挂载必须能以真实 membership 去掉 mount root。
        let Ok(suffix) = relative.strip_prefix(&root) else {
            continue;
        };
        let path = mounted.join(suffix);
        let resolved = path.canonicalize().map_err(|_| Failure::Unavailable)?;
        let mounted = mounted.canonicalize().map_err(|_| Failure::Unavailable)?;
        if !resolved.starts_with(mounted) {
            return Err(Failure::Unavailable);
        }
        if !candidates.contains(&resolved) {
            candidates.push(resolved);
        }
    }
    if candidates.len() != 1 {
        return Err(Failure::Unavailable);
    }
    Ok(candidates.remove(0))
}

/// 业务作用：按控制器分别记录 cgroup 可用性，单个内核能力缺失不伪造其它控制器数据。
///
/// 参数说明：
/// - `config`：固定采集预算及设备白名单。
/// - `output`：接收本轮各控制器独立结果。
///
/// 返回：追加五个固定来源；只读取当前统一 cgroup。
pub(super) fn cgroup(config: &CollectorsConfig, output: &mut Vec<Collection>) {
    let directory = directory(config);
    for (name, reader) in [
        ("cgroup_cpu", cpu as fn(&Path, &CollectorsConfig) -> Samples),
        ("cgroup_memory", cgroup_memory),
        ("cgroup_pids", pids),
        ("cgroup_io", io),
        ("cgroup_pressure", pressure),
    ] {
        output.push(platform_collect(name, || match &directory {
            Ok(path) => reader(path, config),
            Err(error) => Err(*error),
        }));
    }
}

/// 业务作用：读取统一 cgroup CPU 计数与本级有限配额，保留无限配额语义。
///
/// 参数说明：
/// - `path`：当前可见 cgroup。
/// - `config`：文件读取预算。
///
/// 返回：累计微秒、调度周期及可读的有限配额；不把本级配额冒充祖先约束后的有效上限。
fn cpu(path: &Path, config: &CollectorsConfig) -> Samples {
    let text = optional(path.join("cpu.stat"), config)?.ok_or(Failure::Unsupported)?;
    let values = fields(&text)?;
    let mut output = Vec::new();
    for (key, kind) in [
        ("usage_usec", "total"),
        ("user_usec", "user"),
        ("system_usec", "system"),
    ] {
        if let Some(value) = values.get(key) {
            counter(&mut output, &CG_CPU, &[kind], number(value)?);
        }
    }
    if output.is_empty() {
        return Err(Failure::Unsupported);
    }
    for (key, kind) in [("nr_periods", "total"), ("nr_throttled", "throttled")] {
        if let Some(value) = values.get(key) {
            counter(&mut output, &CG_PERIODS, &[kind], number(value)?);
        }
    }
    if let Some(value) = values.get("throttled_usec") {
        counter(&mut output, &CG_THROTTLED, &[], number(value)?);
    }
    if let Some(value) = optional(path.join("cpu.max"), config)? {
        let values: Vec<_> = value.split_whitespace().collect();
        if values.len() != 2 {
            return Err(Failure::Unavailable);
        }
        if values[0] != "max" {
            let period = number(values[1])?;
            if period == 0 {
                return Err(Failure::Unavailable);
            }
            gauge(
                &mut output,
                &CG_CORES,
                &[],
                number(values[0])? as f64 / period as f64,
            );
        }
    }
    Ok(output)
}

/// 业务作用：读取 cgroup 内存占用、分项及 OOM 事件，分项不会重复累加到 current。
///
/// 参数说明：
/// - `path`：当前可见 cgroup。
/// - `config`：文件读取预算。
///
/// 返回：实际存在字段与有限上限；缺少内存控制器明确不支持。
fn cgroup_memory(path: &Path, config: &CollectorsConfig) -> Samples {
    let current = optional(path.join("memory.current"), config)?.ok_or(Failure::Unsupported)?;
    let mut output = Vec::new();
    gauge(
        &mut output,
        &CG_MEMORY,
        &["current"],
        number(current.trim())? as f64,
    );
    for (file, kind) in [
        ("memory.max", "limit"),
        ("memory.swap.current", "swap"),
        ("memory.swap.max", "swap_limit"),
    ] {
        if let Some(value) = optional(path.join(file), config)? {
            if value.trim() != "max" {
                gauge(
                    &mut output,
                    &CG_MEMORY,
                    &[kind],
                    number(value.trim())? as f64,
                );
            }
        }
    }
    if let Some(text) = optional(path.join("memory.stat"), config)? {
        let values = fields(&text)?;
        for key in ["anon", "file", "kernel", "sock", "slab", "shmem"] {
            if let Some(value) = values.get(key) {
                gauge(&mut output, &CG_MEMORY, &[key], number(value)? as f64);
            }
        }
    }
    if let Some(text) = optional(path.join("memory.events"), config)? {
        let values = fields(&text)?;
        for key in ["low", "high", "max", "oom", "oom_kill", "oom_group_kill"] {
            if let Some(value) = values.get(key) {
                counter(&mut output, &CG_EVENTS, &[key], number(value)?);
            }
        }
    }
    Ok(output)
}

/// 业务作用：读取当前 cgroup 的线程进程数量和有限上限。
///
/// 参数说明：
/// - `path`：当前可见 cgroup。
/// - `config`：文件读取预算。
///
/// 返回：当前量与有限限制；不把 max 映射为零。
fn pids(path: &Path, config: &CollectorsConfig) -> Samples {
    let current = optional(path.join("pids.current"), config)?.ok_or(Failure::Unsupported)?;
    let mut output = Vec::new();
    gauge(
        &mut output,
        &CG_PIDS,
        &["current"],
        number(current.trim())? as f64,
    );
    if let Some(value) = optional(path.join("pids.max"), config)? {
        if value.trim() != "max" {
            gauge(
                &mut output,
                &CG_PIDS,
                &["limit"],
                number(value.trim())? as f64,
            );
        }
    }
    Ok(output)
}

/// 业务作用：只导出配置设备的 cgroup I/O，禁止内核设备列表增加指标基数。
///
/// 参数说明：
/// - `path`：当前可见 cgroup。
/// - `config`：设备别名白名单与文件预算。
///
/// 返回：实际存在设备字段；未见设备不补零，不读取宿主机设备统计。
fn io(path: &Path, config: &CollectorsConfig) -> Samples {
    if config.io_devices.is_empty() {
        return Err(Failure::Unsupported);
    }
    let text = optional(path.join("io.stat"), config)?.ok_or(Failure::Unsupported)?;
    let mut output = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for line in text.lines() {
        let mut values = line.split_whitespace();
        let device = values.next().ok_or(Failure::Unavailable)?;
        let Some((alias, _)) = config
            .io_devices
            .iter()
            .find(|(_, actual)| actual.as_str() == device)
        else {
            continue;
        };
        if !seen.insert(device) {
            return Err(Failure::Unavailable);
        }
        let mut parsed = BTreeMap::new();
        for value in values {
            let (key, value) = value.split_once('=').ok_or(Failure::Unavailable)?;
            if parsed.insert(key, value).is_some() {
                return Err(Failure::Unavailable);
            }
        }
        for (key, direction, descriptor) in [
            ("rbytes", "read", &CG_IO_BYTES),
            ("wbytes", "write", &CG_IO_BYTES),
            ("dbytes", "discard", &CG_IO_BYTES),
            ("rios", "read", &CG_IO_OPERATIONS),
            ("wios", "write", &CG_IO_OPERATIONS),
            ("dios", "discard", &CG_IO_OPERATIONS),
        ] {
            if let Some(value) = parsed.get(key) {
                counter(&mut output, descriptor, &[alias, direction], number(value)?);
            }
        }
    }
    Ok(output)
}

/// 业务作用：读取 CPU、内存及 I/O 的累计等待压力，不用短窗口平均值冒充 counter。
///
/// 参数说明：
/// - `path`：当前可见 cgroup。
/// - `config`：文件读取预算。
///
/// 返回：实际存在的 some/full total 微秒；内核未提供时明确不支持。
fn pressure(path: &Path, config: &CollectorsConfig) -> Samples {
    let mut output = Vec::new();
    for resource in ["cpu", "memory", "io"] {
        let Some(text) = optional(path.join(format!("{resource}.pressure")), config)? else {
            continue;
        };
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let kind = fields.next().ok_or(Failure::Unavailable)?;
            if !["some", "full"].contains(&kind) {
                continue;
            }
            let total = fields
                .find_map(|field| field.strip_prefix("total="))
                .ok_or(Failure::Unavailable)?;
            counter(&mut output, &CG_PRESSURE, &[resource, kind], number(total)?);
        }
    }
    if output.is_empty() {
        Err(Failure::Unsupported)
    } else {
        Ok(output)
    }
}

/// 业务作用：只采集当前网络命名空间中显式选择的接口，不把共享网络量归为单进程流量。
///
/// 参数说明：
/// - `config`：文件读取预算。
/// - `alias`：固定对外接口名称。
/// - `name`：实际接口名称。
///
/// 返回：接口的双向累计字节、包数、错误和丢弃；接口缺失视为不可用。
pub(super) fn network(config: &CollectorsConfig, alias: &str, name: &str) -> Samples {
    let text = read("/proc/self/net/dev", config)?;
    let mut output = Vec::new();
    let mut matched = false;
    for line in text.lines().skip(2) {
        let (device, fields) = line.split_once(':').ok_or(Failure::Unavailable)?;
        if device.trim() != name {
            continue;
        }
        if matched {
            return Err(Failure::Unavailable);
        }
        matched = true;
        let values: Vec<_> = fields
            .split_whitespace()
            .map(number)
            .collect::<Result<_, _>>()?;
        if values.len() != 16 {
            return Err(Failure::Unavailable);
        }
        for (direction, offset) in [("receive", 0), ("transmit", 8)] {
            for (column, descriptor) in [&NET_BYTES, &NET_PACKETS, &NET_ERRORS, &NET_DROPS]
                .iter()
                .enumerate()
            {
                counter(
                    &mut output,
                    descriptor,
                    &[alias, direction],
                    values[offset + column],
                );
            }
        }
    }
    if matched {
        Ok(output)
    } else {
        Err(Failure::Unavailable)
    }
}
