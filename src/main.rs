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
        pool.execute(|| handle_connection(stream));
    }
}

fn handle_connection(mut stream: TcpStream) {
    handle_request(&mut stream).unwrap();
}

fn handle_request(stream: &mut TcpStream) -> Result<(), String> {
    let mut read_buff = [0; 1024];

    loop {
        let bytes_read = stream.read(&mut read_buff).unwrap();

        if bytes_read == 0 {
            break;
        }

        if let Ok(request) = String::from_utf8(read_buff[..bytes_read].to_vec()) {
            let command = parse_protocol(&request);

            match command {
                Ok(c) => handle_response(stream, c),
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
    match proto_elements.get(1) {
        Some(val) => {
            let val = val.to_lowercase();
            let command_str = val.as_str();
            match command_str {
                "ping" => Ok(Command::Ping),
                "echo" => {
                    let message = proto_elements.get(2).expect("Could not get ECHO message.");
                    Ok(Command::Echo(EchoCommand::new(message)))
                }
                _ => Err("Command not recognized."),
            }
        }
        None => Err("No command was found in protocol input."),
    }
}

fn handle_response(stream: &mut TcpStream, command: Command) {
    match command {
        Command::Ping => write_response(stream, "+PONG\r\n"),
        Command::Echo(cmd) => {
            let res = format!("${}\r\n{}\r\n", cmd.message.len(), cmd.message);
            write_response(stream, &res);
        }
    }
}

fn write_response(mut stream: &TcpStream, response_str: &str) {
    println!("RESPONSE: {:#?}", response_str);
    stream.write_all(response_str.as_bytes()).unwrap()
}

struct EchoCommand {
    message: String,
}

enum Command {
    Ping,
    Echo(EchoCommand),
}

impl EchoCommand {
    fn new(message: &str) -> Self {
        EchoCommand {
            message: message.to_string(),
        }
    }
}
