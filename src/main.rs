use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Write as fmtWrite;
use std::io::Read;
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};
use threadpool::ThreadPool;
mod threadpool;

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    port: Option<i16>,
    #[arg(short, long)]
    replicaof: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    let port: i16 = cli.port.unwrap_or(6379);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    let pool = ThreadPool::new(4);
    let role = match cli.replicaof {
        Some(_) => "slave",
        None => "master",
    };
    let server_info = ServerInfo::new(role);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let session = Session::new(server_info.clone(), stream);
        pool.execute(|| handle_connection(session));
    }
}

fn handle_connection(mut session: Session) {
    handle_request(&mut session).unwrap();
}

fn handle_request(session: &mut Session) -> Result<(), String> {
    let mut read_buff = [0; 1024];
    let mut command_handler = CommandHandler;

    loop {
        let bytes_read = session.stream.read(&mut read_buff).unwrap();

        if bytes_read == 0 {
            break;
        }

        if let Ok(request) = String::from_utf8(read_buff[..bytes_read].to_vec()) {
            let command = parse_protocol(&request);

            match command {
                Ok(cmd) => command_handler.handle(session, cmd),
                Err(e) => println!("{}", e),
            }
        } else {
            return Err(String::from("Received non-UTF8 data."));
        }
    }

    Ok(())
}

fn parse_protocol(protocol_str: &str) -> Result<Command, &'static str> {
    println!("Incomming request string: {:#?}", protocol_str);
    let proto_elements: Vec<&str> = protocol_str.split("\r\n").collect();
    match proto_elements.get(2) {
        Some(val) => {
            let val = val.to_uppercase();
            let command_str = val.as_str();
            match command_str {
                "PING" => Ok(Command::Ping),
                "ECHO" => {
                    let message = proto_elements.get(4).expect("Could not get ECHO message.");
                    Ok(Command::Echo(EchoCommand::new(message)))
                }
                "INFO" => {
                    let section = proto_elements.get(4);
                    let section = match section {
                        Some(s) => Section::Custom(s.to_string()),
                        None => Section::All,
                    };
                    Ok(Command::Info(InfoCommand::new(section)))
                }
                "SET" => {
                    let key = proto_elements.get(4).expect("Could not get SET key.");
                    let value = proto_elements.get(6).expect("Could not get SET value.");
                    let mut px: Option<i64> = None;
                    let px_opt = proto_elements.get(8);

                    // Get PX if it's there.
                    if px_opt.is_some_and(|x| x.to_uppercase() == "PX") {
                        let px_val = proto_elements.get(10).unwrap();
                        let px_val = px_val.parse::<i64>().unwrap();
                        px = Some(px_val);
                    }
                    Ok(Command::Set(SetCommand::new(key, value, px)))
                }
                "GET" => {
                    let key = proto_elements.get(4).expect("Could not get GET key.");
                    Ok(Command::Get(GetCommand::new(key)))
                }
                _ => Err("Command not recognized."),
            }
        }
        None => Err("No command was found in protocol input."),
    }
}

struct EchoCommand {
    message: String,
}

struct InfoCommand {
    section: Section,
}

#[derive(Debug)]
enum Section {
    Custom(String),
    All,
}

impl InfoCommand {
    fn new(section: Section) -> Self {
        InfoCommand { section }
    }
}

struct SetCommand {
    key: String,
    value: String,
    px: Option<i64>,
}

struct GetCommand {
    key: String,
}

enum Command {
    Ping,
    Echo(EchoCommand),
    Info(InfoCommand),
    Set(SetCommand),
    Get(GetCommand),
}

impl EchoCommand {
    fn new(message: &str) -> Self {
        EchoCommand {
            message: message.to_string(),
        }
    }
}

impl SetCommand {
    fn new(key: &str, value: &str, px: Option<i64>) -> Self {
        SetCommand {
            key: key.to_string(),
            value: value.to_string(),
            px,
        }
    }
}

impl GetCommand {
    fn new(key: &str) -> Self {
        GetCommand {
            key: key.to_string(),
        }
    }
}

struct CacheValue {
    value: String,
    expiry_dt: DateTime<Utc>,
}

impl CacheValue {
    fn new(value: &str, expiry_ms: Option<i64>) -> Self {
        CacheValue {
            value: value.to_string(),
            expiry_dt: Utc::now() + Duration::milliseconds(expiry_ms.unwrap_or(i32::MAX.into())),
        }
    }

    fn is_expired(&self) -> bool {
        self.expiry_dt < Utc::now()
    }
}

#[derive(Clone)]
struct ServerInfo {
    role: String,
    master_info: Option<MasterServerInfo>,
}

#[derive(Clone)]
struct MasterServerInfo {
    replid: String,
    repl_offset: i32,
}

impl ServerInfo {
    fn new(role: &str) -> Self {
        let master_info = match role {
            "master" => Some(MasterServerInfo::new(generate_server_id().as_str(), 0)),
            "slave" => None,
            _ => None,
        };

        ServerInfo {
            role: role.to_string(),
            master_info,
        }
    }

    fn replication_info(&self) -> Vec<String> {
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
        println!("Server ID: {}", replid);
        MasterServerInfo {
            replid: replid.to_string(),
            repl_offset,
        }
    }
}

struct Session {
    server_info: ServerInfo,
    stream: TcpStream,
    storage: HashMap<String, CacheValue>,
}

impl Session {
    fn new(server_info: ServerInfo, stream: TcpStream) -> Self {
        Session {
            server_info,
            stream,
            storage: HashMap::new(),
        }
    }
}

struct CommandHandler;

impl CommandHandler {
    fn handle(&mut self, session: &mut Session, command: Command) {
        match command {
            Command::Ping => ping(session),
            Command::Echo(cmd) => echo(session, cmd),
            Command::Info(cmd) => info(session, cmd),
            Command::Set(cmd) => set(session, cmd),
            Command::Get(cmd) => get(session, cmd),
        }
    }
}

fn ping(session: &Session) {
    write_response(&session.stream, "+PONG\r\n");
}

fn echo(session: &Session, cmd: EchoCommand) {
    let res = format!("${}\r\n{}\r\n", cmd.message.len(), cmd.message);
    write_response(&session.stream, &res);
}

fn info(session: &Session, cmd: InfoCommand) {
    let mut infos: Vec<String> = Vec::new();
    match cmd.section {
        Section::Custom(section) => {
            if section == "replication" {
                infos.extend(session.server_info.replication_info());
            }
        }
        Section::All => {
            infos.extend(session.server_info.replication_info());
        }
    }

    let res = infos
        .iter()
        .map(|x| format!("${}\r\n{}", x.len(), x))
        .collect_vec()
        .join("\r\n");

    write_response(&session.stream, res.as_str());
}

fn set(session: &mut Session, cmd: SetCommand) {
    session
        .storage
        .insert(cmd.key, CacheValue::new(cmd.value.as_str(), cmd.px));
    write_response(&session.stream, "+OK\r\n");
}

fn get(session: &mut Session, cmd: GetCommand) {
    let cv = session.storage.get(&cmd.key);

    if cv.is_some_and(|x| !x.is_expired()) {
        let cv = cv.unwrap();
        write_response(
            &session.stream,
            format!("${}\r\n{}\r\n", cv.value.len(), cv.value).as_str(),
        );
    } else {
        write_response(&session.stream, "$-1\r\n");
    }
}

fn write_response(mut stream: &TcpStream, response_str: &str) {
    println!("RESPONSE: {:#?}", response_str);
    stream.write_all(response_str.as_bytes()).unwrap()
}
