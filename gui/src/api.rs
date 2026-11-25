//! storeboard's socket: HTTP over a unix socket (GET /v1/status and the
//! store verbs). Anyone may read; acting verbs are authorized per caller by
//! the daemon (the store's owner, or root).

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

pub fn socket() -> String {
    if let Ok(s) = std::env::var("STOREBOARD_SOCKET") {
        return s;
    }
    let user = format!("/run/user/{}/storeboard.sock", unsafe_uid());
    if std::path::Path::new("/run/storeboard/storeboard.sock").exists() || !std::path::Path::new(&user).exists() {
        "/run/storeboard/storeboard.sock".into()
    } else {
        user
    }
}

fn unsafe_uid() -> u32 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("Uid:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|u| u.parse().ok()))
        .unwrap_or(0)
}

fn request(method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
    let mut s = UnixStream::connect(socket()).map_err(|e| format!("cannot reach storeboard at {}: {e}", socket()))?;
    s.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: storeboard\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut raw = vec![];
    s.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("bad answer")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let mut body = raw[split + 4..].to_vec();
    if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        body = dechunk(&body);
    }
    let code: u16 = head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if code >= 400 {
        return Err(v.get("error").and_then(Value::as_str).map(str::to_string).unwrap_or(format!("storeboard answered {code}")));
    }
    Ok(v)
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    while let Some(i) = b.windows(2).position(|w| w == b"\r\n") {
        let n = usize::from_str_radix(String::from_utf8_lossy(&b[..i]).trim(), 16).unwrap_or(0);
        if n == 0 || b.len() < i + 2 + n {
            break;
        }
        out.extend_from_slice(&b[i + 2..i + 2 + n]);
        b = &b[(i + 4 + n).min(b.len())..];
    }
    out
}

pub fn get<T: for<'de> Deserialize<'de>>(path: &str) -> Result<T, String> {
    serde_json::from_value(request("GET", path, None)?).map_err(|e| e.to_string())
}

pub fn post(path: &str, body: Value) -> Result<Value, String> {
    request("POST", path, Some(&body))
}

pub fn esc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Peer {
    pub name: String,
    #[serde(default)]
    pub endpoints: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Group {
    pub name: String,
    #[serde(default)]
    pub peers: Vec<String>,
    #[serde(default)]
    pub stores: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Store {
    pub name: String,
    pub group: String,
    pub sync: String,
    pub root: String,
    #[serde(default)]
    pub symlinks: Vec<String>,
    #[serde(default, rename = "requestedBy")]
    pub requested_by: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Connection {
    pub peer: String,
    #[serde(default)]
    pub endpoint: String,
    pub state: String,
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub version: String,
    #[serde(default, rename = "clockSkewMs")]
    pub clock_skew_ms: i64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Perms {
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub mode: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Symlink {
    pub path: String,
    pub state: String,
    #[serde(default)]
    pub busy: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Sync {
    pub store: String,
    pub root: String,
    pub state: String,
    #[serde(default)]
    pub entries: i64,
    #[serde(default)]
    pub local: i64,
    #[serde(default)]
    pub pending: i64,
    #[serde(default)]
    pub scanning: bool,
    #[serde(default)]
    pub receiving: i64,
    #[serde(default)]
    pub conflicts: i64,
    #[serde(rename = "lastScan")]
    pub last_scan: Option<DateTime<Utc>>,
    #[serde(default)]
    pub permissions: Perms,
    #[serde(default)]
    pub symlinks: Vec<Symlink>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Status {
    pub version: String,
    #[serde(default, rename = "uptimeSeconds")]
    pub uptime: i64,
    #[serde(default)]
    pub socket: String,
    #[serde(default, rename = "dataDir")]
    pub data_dir: String,
    #[serde(default)]
    pub root: String,
    pub peer: String,
    #[serde(default)]
    pub peers: Vec<Peer>,
    #[serde(default)]
    pub groups: Vec<Group>,
    #[serde(default)]
    pub stores: Vec<Store>,
    #[serde(default)]
    pub connections: Vec<Connection>,
    #[serde(default)]
    pub sync: Vec<Sync>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Check {
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub hint: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Doctor {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub checks: Vec<Check>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct SpaceEntry {
    pub path: String,
    pub bytes: i64,
    #[serde(default)]
    pub files: i64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Space {
    #[serde(default, rename = "totalBytes")]
    pub total: i64,
    #[serde(default)]
    pub entries: Vec<SpaceEntry>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct CleanupStore {
    pub store: String,
    #[serde(default)]
    pub stream: bool,
    #[serde(default, rename = "trashFiles")]
    pub trash_files: i64,
    #[serde(default, rename = "trashBytes")]
    pub trash_bytes: i64,
    #[serde(default, rename = "trashKeep")]
    pub trash_keep: String,
    #[serde(default, rename = "historyKeep")]
    pub history_keep: String,
    #[serde(default, rename = "oldVersions")]
    pub old_versions: i64,
    #[serde(default, rename = "oldVersionBytes")]
    pub old_version_bytes: i64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Orphan {
    pub store: String,
    #[serde(default, rename = "dataBytes")]
    pub data_bytes: i64,
    #[serde(default)]
    pub folder: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Cleanup {
    #[serde(default)]
    pub stores: Vec<CleanupStore>,
    #[serde(default)]
    pub orphans: Vec<Orphan>,
    #[serde(default, rename = "unreferencedBytes")]
    pub unreferenced_bytes: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FileVersion {
    pub time: DateTime<Utc>,
    #[serde(default)]
    pub blob: String,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Versions {
    #[serde(default)]
    pub keep: String,
    #[serde(default)]
    pub versions: Vec<FileVersion>,
}

/// Everything the window shows.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Snapshot {
    pub status: Status,
    pub doctor: Doctor,
    pub space: Space,
    pub cleanup: Cleanup,
}

pub fn snapshot() -> Result<Snapshot, String> {
    Ok(Snapshot {
        status: get("/v1/status")?,
        doctor: get("/v1/doctor").unwrap_or_default(),
        space: get("/v1/space").unwrap_or_default(),
        cleanup: get("/v1/cleanup").unwrap_or_default(),
    })
}

pub fn bytes(n: i64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1000.0 && u < units.len() - 1 {
        v /= 1000.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", units[u])
    }
}

pub fn ago(t: &DateTime<Utc>) -> String {
    let s = (Utc::now() - *t).num_seconds().max(0);
    match s {
        0..=59 => "just now".into(),
        60..=3599 => format!("{} min ago", s / 60),
        3600..=86399 => format!("{} h ago", s / 3600),
        _ => format!("{} days ago", s / 86400),
    }
}

pub fn duration(s: i64) -> String {
    match s {
        0..=59 => format!("{s} s"),
        60..=3599 => format!("{} min", s / 60),
        3600..=86399 => format!("{} h {} min", s / 3600, (s % 3600) / 60),
        _ => format!("{} days", s / 86400),
    }
}

/// A Go duration ("8760h0m0s", "168h0m0s", "36h") as people say it.
pub fn go_duration(d: &str) -> String {
    let hours: f64 = d.split('h').next().and_then(|h| h.parse().ok()).unwrap_or(-1.0);
    if hours < 0.0 || !d.contains('h') {
        return d.to_string();
    }
    let days = hours / 24.0;
    match () {
        _ if days >= 365.0 && (days % 365.0).abs() < 1.0 => plural(days / 365.0, "year"),
        _ if days >= 7.0 && (days % 7.0).abs() < 0.01 => plural(days / 7.0, "week"),
        _ if hours >= 24.0 && (hours % 24.0).abs() < 0.01 => plural(days, "day"),
        _ => plural(hours, "hour"),
    }
}

fn plural(n: f64, unit: &str) -> String {
    let n = n.round() as i64;
    if n == 1 {
        format!("1 {unit}")
    } else {
        format!("{n} {unit}s")
    }
}
