mod command_handling;
mod commands;
mod helpers;
mod protocol_parser;
mod server;
mod threadpool;

use clap::Parser;
use command_handling::CommandHandler;
use server::{ServerInfo, Session};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;

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
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).unwrap();
    let pool = ThreadPool::new(4);

    let server_info = if let Some(master_addr) = cli.replicaof {
        let master_addr = master_addr.replace(' ', ":");
        master_handshake(&master_addr, port);
        ServerInfo::new_slave(&addr, &master_addr)
    } else {
        ServerInfo::new_master(&addr)
    };

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let session = Session::new(server_info.clone(), stream);
        pool.execute(|| handle_connection(session));
    }
}

fn master_handshake(master_addr: &str, slave_port: i16) {
    let mut stream = TcpStream::connect(master_addr).unwrap();

    // Send the PING command to master.
    send_ping_command(&mut stream);

    // Send the first REPLCONF command to master.
    let slave_port = slave_port.to_string();
    send_replconf_command(vec!["listening-port", &slave_port], &mut stream);

    // Send the second REPLCONF command to master.
    send_replconf_command(vec!["capa", "psync2"], &mut stream);
}

fn send_ping_command(stream: &mut TcpStream) {
    let ping = helpers::RespHelper::to_resp_array(vec!["PING"]);
    stream.write_all(ping.as_bytes()).unwrap();
    stream.flush().unwrap();

    let read_buff = &mut [0; 128];
    let bytes_read = stream.read(read_buff).unwrap();

    match String::from_utf8(read_buff[..bytes_read].to_vec()) {
        Ok(r) => {
            if r != "+PONG\r\n" {
                panic!("Unexpected PING response from master: {}", r);
            }
        }
        Err(_) => panic!("Received non-UTF8 data."),
    }
}

fn send_replconf_command(mut values: Vec<&str>, stream: &mut TcpStream) {
    values.insert(0, "REPLCONF");
    let replconf1 = helpers::RespHelper::to_resp_array(values);
    stream.write_all(replconf1.as_bytes()).unwrap();
    stream.flush().unwrap();

    let read_buff = &mut [0; 128];
    let bytes_read = stream.read(read_buff).unwrap();

    match String::from_utf8(read_buff[..bytes_read].to_vec()) {
        Ok(r) => {
            if r != "+OK\r\n" {
                panic!("Unexpected REPLCONF response from master: {}", r);
            }
        }
        Err(_) => panic!("Received non-UTF8 data."),
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
            let command = protocol_parser::parse_protocol(&request);

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
