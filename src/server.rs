use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Write as fmtWrite;
use std::net::TcpStream;

pub struct CacheValue {
    pub value: String,
    expiry_dt: DateTime<Utc>,
}

impl CacheValue {
    pub fn new(value: &str, expiry_ms: Option<i64>) -> Self {
        CacheValue {
            value: value.to_string(),
            expiry_dt: Utc::now() + Duration::milliseconds(expiry_ms.unwrap_or(i32::MAX.into())),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiry_dt < Utc::now()
    }
}

#[derive(Clone)]
pub struct ServerInfo {
    addr: String,
    master_addr: Option<String>,
    role: String,
    master_info: Option<MasterServerInfo>,
}

#[derive(Clone)]
struct MasterServerInfo {
    replid: String,
    repl_offset: i32,
}

impl ServerInfo {
    pub fn new_master(addr: &str) -> Self {
        let master_info = Some(MasterServerInfo::new(generate_server_id().as_str(), 0));
        ServerInfo {
            addr: addr.to_string(),
            master_addr: None,
            role: "master".to_string(),
            master_info,
        }
    }

    pub fn new_slave(addr: &str, master_addr: &str) -> Self {
        ServerInfo {
            addr: addr.to_string(),
            master_addr: Some(master_addr.to_string()),
            role: "slave".to_string(),
            master_info: None,
        }
    }

    pub fn replication_info(&self) -> Vec<String> {
        let mut values: Vec<String> = vec![format!("role:{}", self.role).to_string()];
        if self.master_info.is_some() {
            let master_info = self.master_info.as_ref().unwrap();
            values.push(format!("master_replid:{}", master_info.replid));
            values.push(format!("master_repl_offset:{}", master_info.repl_offset));
        }
        values
    }
}

fn generate_server_id() -> String {
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 20];
    rng.fill(&mut bytes);
    bytes.iter().fold(String::new(), |mut output, b| {
        let _ = write!(output, "{b:02X}");
        output
    })
}

impl MasterServerInfo {
    fn new(replid: &str, repl_offset: i32) -> Self {
        MasterServerInfo {
            replid: replid.to_string(),
            repl_offset,
        }
    }
}

pub struct Session {
    pub server_info: ServerInfo,
    pub stream: TcpStream,
    pub storage: HashMap<String, CacheValue>,
}

impl Session {
    pub fn new(server_info: ServerInfo, stream: TcpStream) -> Self {
        Session {
            server_info,
            stream,
            storage: HashMap::new(),
        }
    }
}
