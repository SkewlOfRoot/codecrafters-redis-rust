use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::io::Read;
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};
use threadpool::ThreadPool;

mod threadpool;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let session = Session::new(stream);
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

struct Session {
    stream: TcpStream,
    storage: HashMap<String, CacheValue>,
}

impl Session {
    fn new(stream: TcpStream) -> Self {
        Session {
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
