use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};

pub fn default_root() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".mews")
}

pub fn default_host_name() -> String {
    env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .or_else(system_host_name)
        .unwrap_or_else(|| "this-machine".into())
}

#[cfg(unix)]
fn system_host_name() -> Option<String> {
    let mut buffer = [0_u8; 256];
    if unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) } != 0 {
        return None;
    }
    let length = buffer.iter().position(|byte| *byte == 0)?;
    String::from_utf8(buffer[..length].to_vec()).ok()
}
#[cfg(not(unix))]
fn system_host_name() -> Option<String> {
    None
}

pub fn default_relay_url() -> String {
    default_relay_url_for(&default_host_name())
}
pub fn default_relay_url_for(host: &str) -> String {
    format!("ws://{}.local:8787", host.trim_end_matches(".local"))
}

pub fn derive_relay_listen(url: &str) -> Result<Option<SocketAddr>> {
    let url = reqwest::Url::parse(url).context("invalid relay URL")?;
    if url.scheme() == "wss" {
        return Ok(None);
    }
    if url.scheme() != "ws" {
        bail!("relay URL must use ws:// or wss://");
    }
    let port = url
        .port_or_known_default()
        .context("relay URL has no port")?;
    let ip = if matches!(url.host_str(), Some("localhost" | "127.0.0.1")) {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    };
    Ok(Some(SocketAddr::new(ip, port)))
}

pub fn concrete_relay_listen(listen: SocketAddr) -> Result<SocketAddr> {
    if listen.port() != 0 {
        return Ok(listen);
    }
    Ok(std::net::TcpListener::bind(listen)?.local_addr()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn derives_local_relay_listen_addresses() {
        assert_eq!(
            derive_relay_listen("ws://laptop.local:8787").unwrap(),
            Some("0.0.0.0:8787".parse().unwrap())
        );
        assert_eq!(
            derive_relay_listen("ws://127.0.0.1:9000").unwrap(),
            Some("127.0.0.1:9000".parse().unwrap())
        );
        assert_eq!(derive_relay_listen("wss://relay.example").unwrap(), None);
        assert_ne!(
            concrete_relay_listen("127.0.0.1:0".parse().unwrap())
                .unwrap()
                .port(),
            0
        );
    }
}
