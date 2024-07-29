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
                Ok(cmd) => command_handler.handle(session, cmd).unwrap(),
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
            let val = val.to_lowercase();
            let command_str = val.as_str();
            match command_str {
                "ping" => Ok(Command::Ping),
                "echo" => {
                    let message = proto_elements.get(4).expect("Could not get ECHO message.");
                    Ok(Command::Echo(EchoCommand::new(message)))
                }
                "set" => {
                    let key = proto_elements.get(4).expect("Could not get SET key.");
                    let value = proto_elements.get(6).expect("Could not get SET value.");
                    Ok(Command::Set(SetCommand::new(key, value)))
                }
                "get" => {
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
    fn new(key: &str, value: &str) -> Self {
        SetCommand {
            key: key.to_string(),
            value: value.to_string(),
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

struct Session {
    stream: TcpStream,
    storage: HashMap<String, String>,
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
    fn handle(&mut self, session: &mut Session, command: Command) -> Result<(), &'static str> {
        match command {
            Command::Ping => write_response(&session.stream, "+PONG\r\n"),
            Command::Echo(cmd) => {
                echo(&session.stream, cmd)
                // let res = format!("${}\r\n{}\r\n", cmd.message.len(), cmd.message);
                // write_response(stream, &res);
            }
            Command::Set(cmd) => set(session, cmd),
            Command::Get(cmd) => get(session, cmd),
        }

        Ok(())
    }
}

fn echo(stream: &TcpStream, cmd: EchoCommand) {
    let res = format!("${}\r\n{}\r\n", cmd.message.len(), cmd.message);
    write_response(stream, &res);
}

fn set(session: &mut Session, cmd: SetCommand) {
    println!("SET key: {} val: {}", cmd.key, cmd.value);
    session.storage.insert(cmd.key, cmd.value);
    write_response(&session.stream, "+OK\r\n");
}

fn get(session: &mut Session, cmd: GetCommand) {
    println!("GET key: {}", cmd.key);
    if let Some(val) = session.storage.get(&cmd.key) {
        write_response(
            &session.stream,
            format!("${}\r\n{}\r\n", val.len(), val).as_str(),
        );
    } else {
        write_response(&session.stream, "$-1\r\n");
    }
}

fn write_response(mut stream: &TcpStream, response_str: &str) {
    println!("RESPONSE: {:#?}", response_str);
    stream.write_all(response_str.as_bytes()).unwrap()
}
