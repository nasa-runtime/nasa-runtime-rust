use std::collections::BTreeMap;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use libsqlite3_sys as ffi;

use crate::ownership::PhysicalLifetime;

static OPENING: Mutex<BTreeMap<String, Arc<PhysicalLifetime>>> = Mutex::new(BTreeMap::new());
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static INSTALLED: OnceLock<c_int> = OnceLock::new();

/// 建连登记覆盖 SQLx 初始化 PRAGMA 之前的原生句柄生命周期。
pub(crate) struct OpenGuard {
    name: String,
}

impl OpenGuard {
    /// 业务作用：为唯一 SQLx worker 登记原生打开时需要保留的文件权威。
    /// 参数说明：
    /// - `lifetime`：打开开始前取得的物理容量和文件锁责任。
    /// 返回：原生回调可识别的线程名称和登记责任；安装失败时不开始打开。
    pub(crate) fn new(lifetime: Arc<PhysicalLifetime>) -> anyhow::Result<Self> {
        let result =
            INSTALLED.get_or_init(|| unsafe { ffi::sqlite3_auto_extension(Some(anchor_open)) });
        anyhow::ensure!(
            *result == ffi::SQLITE_OK,
            "SQLite open lifetime guard is unavailable"
        );
        let name = format!(
            "nasa-sqlite-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        OPENING
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite open registry is unavailable"))?
            .insert(name.clone(), lifetime);
        Ok(Self {
            name,
        })
    }

    /// 业务作用：把本次建连身份传给 SQLx 的专用线程。
    /// 参数说明：无。
    /// 返回：只属于本次打开的 worker 名称。
    pub(crate) fn name(&self) -> String {
        self.name.clone()
    }
}

impl Drop for OpenGuard {
    /// 业务作用：建连已返回后撤销未被原生句柄接管的登记。
    /// 参数说明：无。
    /// 返回：已安装的原生析构责任不受此清理影响。
    fn drop(&mut self) {
        if let Ok(mut opening) = OPENING.lock() {
            opening.remove(&self.name);
        }
    }
}

/// 业务作用：在 SQLite 打开回调中锚定文件锁，覆盖随后失败的 SQLx 初始化。
/// 参数说明：
/// - `database`：SQLite 正在打开的原生连接。
/// - `_message`：SQLite 预留的错误消息输出。
/// - `_api`：SQLite 预留的扩展入口。
/// 返回：本框架以外的连接保持原状；登记连接只有安装成功才允许继续打开。
unsafe extern "C" fn anchor_open(
    database: *mut ffi::sqlite3,
    _message: *mut *mut c_char,
    _api: *const ffi::sqlite3_api_routines,
) -> c_int {
    let thread = std::thread::current();
    let Some(name) = thread.name() else {
        return ffi::SQLITE_OK;
    };
    let lifetime = match OPENING.lock() {
        Ok(mut opening) => opening.remove(name),
        Err(_) => return ffi::SQLITE_ERROR,
    };
    let Some(lifetime) = lifetime else {
        return ffi::SQLITE_OK;
    };
    // SQLite 即使拒绝函数注册也调用 xDestroy，保活责任只在原生路径解除。
    unsafe {
        ffi::sqlite3_create_function_v2(
            database,
            c"__nasa_physical_owner".as_ptr(),
            0,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            Arc::into_raw(lifetime) as *mut c_void,
            Some(physical_owner),
            None,
            None,
            Some(release_owner),
        )
    }
}

/// 业务作用：为原生析构回调提供不可用于业务操作的函数载体。
/// 参数说明：
/// - `context`：SQLite 函数调用上下文。
/// - `_count`：原生参数数量。
/// - `_values`：原生参数数组。
/// 返回：固定空值，不暴露文件或 owner 信息。
unsafe extern "C" fn physical_owner(
    context: *mut ffi::sqlite3_context,
    _count: c_int,
    _values: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        ffi::sqlite3_result_null(context);
    }
}

/// 业务作用：原生连接析构时归还打开阶段持有的物理责任。
/// 参数说明：
/// - `pointer`：注册函数时转交的唯一 Arc 原始引用。
/// 返回：最后一个责任退出后才能解除文件独占。
unsafe extern "C" fn release_owner(pointer: *mut c_void) {
    unsafe {
        drop(Arc::from_raw(pointer as *const PhysicalLifetime));
    }
}
