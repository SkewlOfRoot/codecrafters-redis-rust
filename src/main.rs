mod command_handling;
mod commands;
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
    let server_info = match cli.replicaof {
        Some(master_addr) => {
            let master_addr = master_addr.replace(' ', ":");
            master_handshake(&master_addr);
            ServerInfo::new_slave(&addr, &master_addr)
        }
        None => ServerInfo::new_master(&addr),
    };

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let session = Session::new(server_info.clone(), stream);
        pool.execute(|| handle_connection(session));
    }
}

fn master_handshake(master_addr: &str) {
    let mut conn = TcpStream::connect(master_addr).unwrap();
    conn.write_all("*1\r\n$4\r\nping\r\n".as_bytes()).unwrap();
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
