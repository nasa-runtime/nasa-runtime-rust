mod provider;
pub(crate) use provider::*;

mod service;
pub(crate) use service::*;

pub(crate) trait ServerAddress: Sync + Send + 'static {
    /// 业务作用：提供传输层建立连接所需的主机标识，不在读取时执行地址解析。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：供连接适配器使用的主机字符串；地址是否合法或可达由后续连接过程判断。
    fn host(&self) -> String;

    /// 业务作用：提供服务地址的基础端口，供协议适配器决定实际连接端口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已选地址携带的端口；不隐含该端口已监听或通过健康检查。
    fn port(&self) -> u16;

    /// 业务作用：向连接适配器声明此地址是否允许发起连接尝试。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：为假时适配器拒绝使用地址；为真只代表可尝试，不能替代远端可达性检查。
    fn is_available(&self) -> bool;
}

use crate::api::constants::{DEFAULT_SERVER_PORT, ENV_NACOS_CLIENT_COMMON_SERVER_PORT};
use crate::api::error::Error;
use crate::api::error::Result;
use crate::properties::get_value_u16;

/// 业务作用：把静态配置或 endpoint 正文归一为带端口的服务地址列表。
///
/// 参数说明：
/// - `server_addr`：以逗号、分号或换行分隔的地址文本；空白片段被忽略。
///
/// 返回：至少一个地址时返回列表，缺少可解析端口的项使用配置默认端口；全空时返回地址错误，不校验 DNS 或网络可达性。
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
        // 无候选地址时不能建立轮询目录，拒绝启动而不是发布空列表。
        tracing::warn!("Server address is empty after parsing: {:?}", server_addr);
        return Err(Error::WrongServerAddress("Server address is empty".into()));
    }

    Ok(result)
}

/// 业务作用：从候选目录中筛出带可解析端口的地址，供本次轮询选择使用。
///
/// 参数说明：
/// - `server_list`：已归一的地址列表；缺少可解析 u16 端口的项不参与选择。
///
/// 返回：借用原文本的主机与端口；输入为空或全部端口不可解析时返回地址错误，不执行主机格式或网络校验。
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
        // 后续按候选数量取模，必须先拒绝没有可选端口的目录。
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

/// 业务作用：拆分主机与可选端口，保留带方括号的 IPv6 主机供 URL 构造使用。
///
/// 参数说明：
/// - `input`：单个地址片段；此入口不清除空白，非方括号形式按第一个冒号拆分。
///
/// 返回：原文本中的主机切片及可解析的 u16 端口；无端口或端口解析失败时为空，开头为冒号的输入整体视为主机。
pub(crate) fn parse_host_port(input: &str) -> (&str, Option<u16>) {
    if input.starts_with('[') {
        // 方括号限定主机边界，避免将 IPv6 内部冒号误认成端口分隔符。
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
            // 开头为冒号时不产生空主机，保留原输入由上层决定是否可连接。
            return (input, None);
        }
        let port = port_str[1..].parse::<u16>().ok();
        (host, port)
    } else {
        (input, None)
    }
}
