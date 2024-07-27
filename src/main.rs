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
    let requests = read_request(&mut stream).unwrap();
    println!("REQUEST:\r\n{:#?}", requests);

    for _ in requests {
        write_response(&stream);
    }
}

fn read_request(stream: &mut TcpStream) -> Result<Vec<String>, String> {
    let mut read_buff = [0; 1024];
    match stream.read(&mut read_buff) {
        Ok(bytes_read) => {
            if let Ok(request) = String::from_utf8(read_buff[..bytes_read].to_vec()) {
                let split = request.split('\n');
                let requests = split.into_iter().map(|x| x.to_string()).collect();
                Ok(requests)
            } else {
                Err(String::from("Received non-UTF8 data."))
            }
        }
        Err(e) => Err(format!("Failed to read bytes from stream: {}", e)),
    }
}

fn write_response(mut stream: &TcpStream) {
    stream.write_all("+PONG\r\n".as_bytes()).unwrap()
}
