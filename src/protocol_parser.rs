use super::commands::{Command, EchoCommand, GetCommand, InfoCommand, Section, SetCommand};

pub fn parse_protocol(protocol_str: &str) -> Result<Command, &'static str> {
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
