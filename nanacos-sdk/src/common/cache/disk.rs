use std::{borrow::Cow, collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{OpenOptions, remove_file, rename},
    io::AsyncWriteExt,
    time::{Duration, sleep},
};
use tracing::{debug, error, info, instrument, warn};

use super::Store;

pub(crate) struct DiskStore {
    /// 当前来源的磁盘目录，保存前确认存在，不依赖后台创建的完成时序。
    disk_path: PathBuf,
    /// 完整的模块与 namespace 身份，用于重启时复验缓存隔离。
    scope: String,
}

/// 完整缓存身份随内容持久化，摘要文件名不能代替原键复验。
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    /// 固定磁盘布局标识，不自动加载旧的原文件名布局。
    format: String,
    /// 模块与 namespace 的完整身份。
    scope: String,
    /// 原始逻辑键，不执行路径解释或 Unicode 归一化。
    key: String,
    /// 完整缓存值，不代表实时配置或注册资格。
    value: serde_json::Value,
}

impl DiskStore {
    /// 业务作用：记录当前配置或发现缓存目录，不在构造时假定异步目录准备已经完成。
    ///
    /// 参数说明：
    /// - `root`：部署者独占管理的可信根目录，不允许不可信进程替换目录项。
    /// - `module`：当前缓存模块的逻辑名称。
    /// - `namespace`：原始 namespace，不作为路径解释。
    ///
    /// 返回：可执行独立原子保存的存储实例；目录在实际访问时准备。
    pub(crate) fn new(root: PathBuf, module: &str, namespace: &str) -> Self {
        let scope =
            serde_json::to_string(&[module, namespace]).expect("string tuple serialization");
        let disk_path = root.join(format!("cache-{:x}", Sha256::digest(scope.as_bytes())));
        info!(path = %disk_path.display(), "Creating DiskStore");
        Self {
            disk_path,
            scope,
        }
    }

    /// 业务作用：准备受控作用域目录并拒绝符号链接导致的路径转移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：规范化目录；非目录、链接和越界目标拒绝。
    async fn directory(&self) -> std::io::Result<PathBuf> {
        let root = self
            .disk_path
            .parent()
            .ok_or_else(|| std::io::Error::other("cache root absent"))?;
        tokio::fs::create_dir_all(root).await?;
        let root = tokio::fs::canonicalize(root).await?;
        let path = root.join(
            self.disk_path
                .file_name()
                .ok_or_else(|| std::io::Error::other("cache scope absent"))?,
        );
        match tokio::fs::create_dir(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        // 固定摘要目录仍须拒绝已有链接，不能把缓存写入或删除转移到根之外。
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        let canonical = tokio::fs::canonicalize(&path).await?;
        if !metadata.is_dir() || metadata.is_symlink() || canonical.parent() != Some(root.as_path())
        {
            return Err(std::io::Error::other("cache directory outside root"));
        }
        Ok(canonical)
    }

    /// 业务作用：只接受当前目录中的普通记录，拒绝链接与特殊文件。
    ///
    /// 参数说明：
    /// - `path`：受控目录的直接子路径。
    ///
    /// 返回：普通文件为 true，缺失为 false；非法类型或越界目标返回错误。
    async fn regular(path: &std::path::Path) -> std::io::Result<bool> {
        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) => {
                if !metadata.is_file()
                    || metadata.is_symlink()
                    || tokio::fs::canonicalize(path).await?.parent() != path.parent()
                {
                    return Err(std::io::Error::other(
                        "cache entry is not a confined regular file",
                    ));
                }
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// 业务作用：以完整逻辑键的摘要构造固定长度文件名，业务字符不会成为路径。
    ///
    /// 参数说明：
    /// - `key`：原始配置或发现缓存身份。
    ///
    /// 返回：仅含十六进制字符和固定后缀的文件名。
    fn filename(key: &str) -> String {
        format!("{:x}.json", Sha256::digest(key.as_bytes()))
    }

    /// 业务作用：复验内容身份，拒绝移动、碰撞、损坏或其它作用域的记录。
    ///
    /// 参数说明：
    /// - `path`：当前受控作用域中的候选文件。
    ///
    /// 返回：身份和摘要均一致的记录；非法内容不作为缓存证据。
    async fn read_record(&self, path: &std::path::Path) -> std::io::Result<Record> {
        if !Self::regular(path).await? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "cache entry absent",
            ));
        }
        let record: Record = serde_json::from_slice(&tokio::fs::read(path).await?)?;
        let expected = Self::filename(&record.key);
        if record.format != "nacos-cache-record"
            || record.scope != self.scope
            || path.file_name().and_then(|name| name.to_str()) != Some(expected.as_str())
        {
            return Err(std::io::Error::other("cache identity mismatch"));
        }
        Ok(record)
    }

    /// 业务作用：在本次独占临时文件中完整落盘，再原子替换缓存，避免并发保存互相截断或移动文件。
    ///
    /// 参数说明：
    /// - `tmp_path`：本次保存独有的临时路径，必须以排他创建取得所有权。
    /// - `write_path`：最终缓存键的路径。
    /// - `value`：本次已经序列化的完整缓存值。
    ///
    /// 返回：完整内容替换成功；失败只清理本次创建的临时文件，保留原缓存。
    async fn try_save(
        &self,
        tmp_path: &PathBuf,
        write_path: &PathBuf,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(tmp_path = %tmp_path.display(), "Opening temp file for write");

        // 目录就绪必须先于写入，不能依赖未被等待的初始化任务。
        let directory = self.directory().await?;
        if tmp_path.parent() != Some(directory.as_path())
            || write_path.parent() != Some(directory.as_path())
        {
            return Err(std::io::Error::other("cache directory changed").into());
        }
        Self::regular(write_path).await?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(tmp_path)
            .await?;

        let result = async {
            let mut file = file;
            file.write_all(value).await?;
            file.sync_all().await?;
            drop(file);
            // 原缓存只在完整新内容落盘后替换，读取者不会观察半条记录。
            if self.directory().await? != directory {
                return Err(std::io::Error::other("cache directory changed"));
            }
            Self::regular(write_path).await?;
            rename(tmp_path, write_path).await
        }
        .await;
        if result.is_err() {
            // 排他创建已经成功，此路径仅属于本次保存，不能删除其它并发写入的文件。
            let _ = remove_file(tmp_path).await;
        }
        result.map_err(Into::into)
    }

    /// 业务作用：删除已失效的磁盘缓存，缺失文件不再承担删除责任。
    ///
    /// 参数说明：
    /// - `path`：当前缓存项的完整路径。
    ///
    /// 返回：文件不存在或删除成功；其它文件系统失败交由调用方有界重试。
    async fn try_remove(
        &self,
        path: &PathBuf,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !Self::regular(path).await? {
            return Ok(());
        }
        if path.parent() != Some(self.directory().await?.as_path()) {
            return Err(std::io::Error::other("cache directory changed").into());
        }
        remove_file(path).await?;
        Ok(())
    }
}

#[async_trait]
impl<V> Store<V> for DiskStore
where
    V: de::DeserializeOwned + Send,
{
    /// 业务作用：为缓存访问日志提供固定的存储来源名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：磁盘来源标识，不包含业务键或凭据。
    fn name(&self) -> Cow<'_, str> {
        Cow::from("disk-store")
    }

    /// 业务作用：加载完整且可反序列化的磁盘缓存，忽略未完成的临时写入与损坏记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可用缓存集合；目录不可用时为空，不以磁盘内容证明当前服务端资格。
    async fn load(&self) -> HashMap<String, V>
    where
        V: de::DeserializeOwned,
    {
        let mut default_map = HashMap::default();

        let directory = match self.directory().await {
            Ok(directory) => directory,
            Err(error) => {
                warn!(%error, "Cache directory unavailable");
                return default_map;
            }
        };
        let disk_path_display = directory.display();

        let mut dir_iter = match tokio::fs::read_dir(&directory).await {
            Ok(iter) => iter,
            Err(e) => {
                error!(path = %disk_path_display, error = %e, "Failed to read cache directory");
                return default_map;
            }
        };

        let mut loaded_count = 0u64;
        let mut failed_count = 0u64;

        while let Ok(Some(entry)) = dir_iter.next_entry().await {
            let path = entry.path();

            let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            // 只识别固定摘要布局，临时文件和旧布局不会被误当成完整记录。
            if filename.len() != 69
                || !filename.ends_with(".json")
                || !filename.as_bytes()[..64].iter().all(u8::is_ascii_hexdigit)
            {
                continue;
            }
            match self.read_record(&path).await {
                Ok(record) => match serde_json::from_value::<V>(record.value) {
                    Ok(value) => {
                        default_map.insert(record.key, value);
                        loaded_count += 1;
                    }
                    Err(_) => failed_count += 1,
                },
                Err(_) => failed_count += 1,
            }
        }

        info!(
            path = %disk_path_display,
            loaded = loaded_count,
            failed = failed_count,
            "Cache loading completed"
        );

        default_map
    }

    /// 业务作用：有界重试当前完整缓存值，每次尝试使用独立临时文件，不与其它键或保存任务共享。
    ///
    /// 参数说明：
    /// - `key`：本次配置或发现缓存的固定键。
    /// - `value`：本次完整序列化值。
    ///
    /// 返回：保存成功即结束；重试耗尽记录失败，内存状态不由磁盘失败回滚。
    #[instrument(fields(key = key), skip_all)]
    async fn save(&self, key: &str, value: Vec<u8>) {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 100;

        let directory = match self.directory().await {
            Ok(directory) => directory,
            Err(error) => {
                error!(%error, "Cache directory unavailable");
                return;
            }
        };
        let write_path = directory.join(Self::filename(key));
        // 原内容必须仍属于当前键；摘要冲突或损坏不能静默覆盖其它身份。
        match self.read_record(&write_path).await {
            Ok(record) if record.key == key => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                error!("Cache identity mismatch");
                return;
            }
        }
        let encoded = serde_json::from_slice(&value).and_then(|value| {
            serde_json::to_vec(&Record {
                format: "nacos-cache-record".into(),
                scope: self.scope.clone(),
                key: key.into(),
                value,
            })
        });
        let value = match encoded {
            Ok(value) => value,
            Err(error) => {
                error!(%error, "Invalid cache value");
                return;
            }
        };

        let write_path_display = write_path.display();
        info!(path = %write_path_display, "Saving cache entry to disk");

        for attempt in 1..=MAX_RETRIES {
            // 随机名同时隔离同键、不同扩展名的键和多个存储实例；排他创建防止覆盖残留文件。
            let tmp_path = directory.join(format!("{:032x}.tmp", rand::random::<u128>()));
            match self.try_save(&tmp_path, &write_path, &value).await {
                Ok(()) => {
                    info!(path = %write_path_display, "Cache entry saved successfully");
                    return;
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        error!(
                            path = %write_path_display,
                            error = %e,
                            "Failed to save cache entry after all retries"
                        );
                        return;
                    }
                    warn!(
                        path = %write_path_display,
                        attempt = attempt,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "Save attempt failed, retrying..."
                    );
                    sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
                }
            }
        }
    }

    /// 业务作用：有界重试删除失效缓存，避免无限等待磁盘阻塞上层状态处理。
    ///
    /// 参数说明：
    /// - `key`：应删除的配置或发现缓存键。
    ///
    /// 返回：删除成功即结束；重试耗尽记录失败，调用方不得据此恢复失效资格。
    #[instrument(fields(key = key), skip_all)]
    async fn remove(&self, key: &str) {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 100;

        let directory = match self.directory().await {
            Ok(directory) => directory,
            Err(error) => {
                error!(%error, "Cache directory unavailable");
                return;
            }
        };
        let delete_path = directory.join(Self::filename(key));
        // 删除同样复验原键，不能让碰撞或伪造记录取得文件删除权限。
        match self.read_record(&delete_path).await {
            Ok(record) if record.key == key => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            _ => {
                error!("Cache identity mismatch");
                return;
            }
        }

        let delete_path_display = delete_path.display();
        info!(key = key, path = %delete_path_display, "Removing cache entry from disk");

        for attempt in 1..=MAX_RETRIES {
            match self.try_remove(&delete_path).await {
                Ok(()) => {
                    info!(key = key, path = %delete_path_display, "Cache entry removed successfully");
                    return;
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        error!(
                            key = key,
                            path = %delete_path_display,
                            error = %e,
                            "Failed to remove cache entry after all retries"
                        );
                        return;
                    }
                    warn!(
                        key = key,
                        attempt = attempt,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "Remove attempt failed, retrying..."
                    );
                    sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
                }
            }
        }
    }
}
