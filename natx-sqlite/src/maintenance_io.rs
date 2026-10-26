use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use libsqlite3_sys as ffi;

#[repr(C)]
struct Methods {
    methods: ffi::sqlite3_io_methods,
    original: ffi::sqlite3_io_methods,
    failure: Arc<AtomicI32>,
}

/// 只在独占原生 handle 的备份期间记录固定文件 I/O，不替换全局 VFS。
pub(crate) struct FileIo {
    files: Vec<(
        *mut ffi::sqlite3_file,
        *const ffi::sqlite3_io_methods,
        Box<Methods>,
    )>,
    failure: Arc<AtomicI32>,
}

impl FileIo {
    /// 业务作用：记录指定连接主文件和稳定 WAL 的真实 I/O 失败，避免 backup_finish 把源错误记在目标句柄后丢失归属。
    /// 参数说明：
    /// - `database`：由维护流程独占且保持打开的连接；本作用域不允许关闭连接。
    /// - `include_wal`：只读源连接可记录保持打开的 WAL；可写目标不接管可能关闭的 journal。
    /// 返回：退出前恢复全部方法表的守卫；无法取得主文件时拒绝开始复制。
    pub(crate) fn install(database: *mut ffi::sqlite3, include_wal: bool) -> anyhow::Result<Self> {
        let mut scope = Self {
            files: Vec::new(),
            failure: Arc::new(AtomicI32::new(0)),
        };
        for control in [
            ffi::SQLITE_FCNTL_FILE_POINTER,
            ffi::SQLITE_FCNTL_JOURNAL_POINTER,
        ] {
            if control == ffi::SQLITE_FCNTL_JOURNAL_POINTER && !include_wal {
                continue;
            }
            let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
            let result = unsafe {
                ffi::sqlite3_file_control(
                    database,
                    c"main".as_ptr(),
                    control,
                    &mut file as *mut _ as *mut c_void,
                )
            };
            if control == ffi::SQLITE_FCNTL_FILE_POINTER {
                anyhow::ensure!(
                    result == ffi::SQLITE_OK && !file.is_null(),
                    "SQLite backup file identity is unavailable"
                );
            }
            if result != ffi::SQLITE_OK
                || file.is_null()
                || scope.files.iter().any(|(existing, _, _)| *existing == file)
            {
                continue;
            }
            let original = unsafe { (*file).pMethods };
            if original.is_null() {
                continue;
            }
            let mut methods = Box::new(Methods {
                methods: unsafe { *original },
                original: unsafe { *original },
                failure: scope.failure.clone(),
            });
            methods.methods.xRead = Some(read);
            methods.methods.xWrite = Some(write);
            methods.methods.xSync = Some(sync);
            methods.methods.xTruncate = Some(truncate);
            methods.methods.xFileSize = Some(file_size);
            methods.methods.xLock = Some(lock);
            methods.methods.xUnlock = Some(unlock);
            methods.methods.xCheckReservedLock = Some(reserved);
            if methods.methods.iVersion >= 2 {
                if methods.original.xShmMap.is_some() {
                    methods.methods.xShmMap = Some(shm_map);
                }
                if methods.original.xShmLock.is_some() {
                    methods.methods.xShmLock = Some(shm_lock);
                }
            }
            if methods.methods.iVersion >= 3 {
                if methods.original.xFetch.is_some() {
                    methods.methods.xFetch = Some(fetch);
                }
                if methods.original.xUnfetch.is_some() {
                    methods.methods.xUnfetch = Some(unfetch);
                }
            }
            // 方法表地址由 Box 固定；源 worker 已停在 handle lock，目标由维护独占；作用域内主文件和源 WAL 保持打开。
            unsafe {
                (*file).pMethods = &methods.methods;
            }
            scope.files.push((file, original, methods));
        }
        Ok(scope)
    }

    /// 业务作用：取得当前复制期间直接由被跟踪文件返回的第一项失败证据。
    /// 参数说明：无。
    /// 返回：原生扩展码；成功、正常短读或锁竞争不记作存储失败。
    pub(crate) fn error_code(&self) -> Option<i32> {
        match self.failure.load(Ordering::Acquire) {
            0 => None,
            code => Some(code),
        }
    }
}

impl Drop for FileIo {
    /// 业务作用：在 handle 解锁或连接关闭之前还原原生方法表，防止回调引用已释放的归属记录。
    /// 参数说明：无。
    /// 返回：所有原生文件恢复其原有 VFS 行为。
    fn drop(&mut self) {
        for (file, original, _) in &self.files {
            unsafe {
                (**file).pMethods = *original;
            }
        }
    }
}

macro_rules! forward {
    ($name:ident, $field:ident, $($arg:ident: $ty:ty => $description:literal),*) => {
        /// 业务作用：透明调用原有文件方法并保留首个存储失败，锁竞争和短读不构成损坏证据。
        /// 参数说明：
        /// - `file`：由独占备份作用域固定的文件。
        $(#[doc = concat!("- `", stringify!($arg), "`：", $description)])*
        /// 返回：原始 VFS 返回码，不修改缓冲、文件内容或调用次数。
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file, $($arg: $ty),*) -> c_int {
            let methods = unsafe {
                &*((*file).pMethods as *const Methods)
            };
            let result = unsafe {
                (methods.original.$field.expect("SQLite file method"))(file, $($arg),*)
            };
            if result != ffi::SQLITE_OK
                && result != ffi::SQLITE_IOERR_SHORT_READ
                && result & 255 != ffi::SQLITE_BUSY
                && result & 255 != ffi::SQLITE_LOCKED
            {
                let _ = methods.failure.compare_exchange(
                    0,
                    result,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            result
        }
    };
}

forward!(read, xRead, buffer: *mut c_void => "读取目标缓冲。", bytes: c_int => "读取字节数。", offset: i64 => "文件偏移。");
forward!(write, xWrite, buffer: *const c_void => "写入来源缓冲。", bytes: c_int => "写入字节数。", offset: i64 => "文件偏移。");
forward!(sync, xSync, flags: c_int => "同步策略。");
forward!(truncate, xTruncate, size: i64 => "目标长度。");
forward!(file_size, xFileSize, size: *mut i64 => "文件长度输出。");
forward!(lock, xLock, level: c_int => "请求锁等级。");
forward!(unlock, xUnlock, level: c_int => "保留锁等级。");
forward!(reserved, xCheckReservedLock, held: *mut c_int => "保留锁状态输出。");
forward!(shm_map, xShmMap, page: c_int => "共享内存页。", size: c_int => "页大小。", extend: c_int => "是否扩展。", output: *mut *mut c_void => "映射地址输出。");
forward!(shm_lock, xShmLock, offset: c_int => "锁起点。", count: c_int => "锁数量。", flags: c_int => "共享锁策略。");
forward!(fetch, xFetch, offset: i64 => "文件偏移。", size: c_int => "映射长度。", output: *mut *mut c_void => "映射地址输出。");
forward!(unfetch, xUnfetch, offset: i64 => "文件偏移。", address: *mut c_void => "需要释放的映射地址。");
