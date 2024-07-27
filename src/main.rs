use std::io::Read;
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream);
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
            println!("Incomming request string: {:#?}", request);

            if request.contains("PING") {
                write_response(stream);
            }
        } else {
            return Err(String::from("Received non-UTF8 data."));
        }
    }

    Ok(())
}

fn write_response(mut stream: &TcpStream) {
    let response_text = "+PONG\r\n";
    println!("RESPONSE: {:#?}", response_text);
    stream.write_all(response_text.as_bytes()).unwrap()
}
