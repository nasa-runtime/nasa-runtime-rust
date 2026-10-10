mod provider;
pub(crate) use provider::*;

mod service;
pub(crate) use service::*;

pub(crate) trait ServerAddress: Sync + Send + 'static {
    fn host(&self) -> String;

    fn port(&self) -> u16;

    fn is_available(&self) -> bool;
}

use crate::api::constants::{DEFAULT_SERVER_PORT, ENV_NACOS_CLIENT_COMMON_SERVER_PORT};
use crate::api::error::Error;
use crate::api::error::Result;
use crate::properties::get_value_u16;

/// Parse a server address string (comma/semicolon/newline separated) into a list of `host:port`.
///
/// Each segment without an explicit port gets the default port appended.
pub(crate) fn parse_server_list(server_addr: &str) -> Result<Vec<String>> {
    let default_port = get_value_u16(ENV_NACOS_CLIENT_COMMON_SERVER_PORT, DEFAULT_SERVER_PORT);

    let result: Vec<String> = server_addr
        .trim()
        .split([',', ';', '\n'])
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(|addr| {
            let (host, port) = parse_host_port(addr);
            format!("{}:{}", host, port.unwrap_or(default_port))
        })
        .collect();

    if result.is_empty() {
        tracing::warn!("Server address is empty after parsing: {:?}", server_addr);
        return Err(Error::WrongServerAddress("Server address is empty".into()));
    }

    Ok(result)
}

/// Parse a list of `host:port` strings into `(host, port)` tuples.
///
/// Returns `Err` if the list is empty or all entries have illegal format.
pub(crate) fn parse_host_port_vec(server_list: &[String]) -> Result<Vec<(&str, u16)>> {
    if server_list.is_empty() {
        return Err(Error::WrongServerAddress(
            "server list must not empty".into(),
        ));
    }

    let result_list: Vec<(&str, u16)> = server_list
        .iter()
        .filter_map(|server| {
            let (host, port) = parse_host_port(server);
            Some((host, port?))
        })
        .collect();
    if result_list.is_empty() {
        tracing::warn!(
            "All server addresses have illegal format: {:?}",
            server_list
        );
        return Err(Error::WrongServerAddress(
            "all the server is illegal format!".into(),
        ));
    }
    Ok(result_list)
}

/// Parse a bare `host` or `host:port` string into `(host, Some(port))` or `(host, None)`.
///
/// e.g. `"endpoint.example.com"`     → `("endpoint.example.com", None)`
///      `"endpoint.example.com:9090"` → `("endpoint.example.com", Some(9090))`
///      `"[::1]:8848"`               → `("[::1]", Some(8848))`
///      `"[::1]"`                    → `("[::1]", None)`
pub(crate) fn parse_host_port(input: &str) -> (&str, Option<u16>) {
    if input.starts_with('[') {
        // IPv6 bracket notation: [host]:port or [host]
        if let Some(bracket_end) = input.find(']') {
            let host = &input[..=bracket_end];
            let rest = &input[bracket_end + 1..];
            let port = rest.strip_prefix(':').and_then(|p| p.parse::<u16>().ok());
            return (host, port);
        }
    }
    if let Some(colon_pos) = input.find(':') {
        let (host, port_str) = input.split_at(colon_pos);
        if host.is_empty() {
            // Bare IPv6 without brackets (e.g. "::1") — treat entire input as host
            return (input, None);
        }
        let port = port_str[1..].parse::<u16>().ok();
        (host, port)
    } else {
        (input, None)
    }
}
