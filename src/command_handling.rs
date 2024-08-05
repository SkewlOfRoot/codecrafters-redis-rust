use super::commands::*;
use super::server::{CacheValue, Session};
use std::{io::Write, net::TcpStream};

pub struct CommandHandler;

impl CommandHandler {
    pub fn handle(&mut self, session: &mut Session, command: Command) {
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

    let res = infos.join("\r\n");
    let res = format!("${}\r\n{}\r\n", res.len(), res);

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
