use crate::api::error::Error;
use crate::api::error::Result;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static LOCAL_IP: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| local_ipaddress::get().unwrap_or(String::from("127.0.0.1")));

#[allow(dead_code)]
pub(crate) static HOME_DIR: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::home_dir()
        .unwrap_or(std::env::temp_dir())
        .to_str()
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "/tmp".to_owned())
});

/// Generates unique client ID for the given module.
/// Format: `{module_name}-_-{server_addr}-_-{namespace}-_-{sequence}`
pub(crate) fn generate_client_id(
    module_name: &str,
    server_addr: &str,
    namespace: &str,
    seq: &AtomicU64,
) -> String {
    format!(
        "{module_name}-_-{server_addr}-_-{namespace}-_-{}",
        seq.fetch_add(1, Ordering::SeqCst)
    )
}

/// Returns the group name or default group if empty.
pub(crate) fn normalize_group_name(group_name: Option<String>) -> String {
    group_name
        .filter(|data| !data.is_empty())
        .unwrap_or_else(|| crate::api::constants::DEFAULT_GROUP.to_owned())
}

/// Checks whether two slices contain the same elements regardless of order.
/// Duplicates are significant: `[a, a]` != `[a]`.
pub(crate) fn unordered_eq<T>(a: &[T], b: &[T]) -> bool
where
    T: Ord,
{
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted: Vec<&T> = a.iter().collect();
    let mut b_sorted: Vec<&T> = b.iter().collect();
    a_sorted.sort();
    b_sorted.sort();
    a_sorted == b_sorted
}

/// Checks param_val not blank
pub(crate) fn check_not_blank<'a>(param_val: &'a str, param_name: &'a str) -> Result<&'a str> {
    if param_val.trim().is_empty() {
        Err(Error::InvalidParam(
            param_name.into(),
            "param must not blank!".into(),
        ))
    } else {
        Ok(param_val)
    }
}
