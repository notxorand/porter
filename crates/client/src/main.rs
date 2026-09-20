use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

const SEND_ENDPOINT: &str = "127.0.0.1:4848";
const RECEIVE_ENDPOINT: &str = "0.0.0.0:4849";

#[derive(Debug, PartialEq, Eq)]
enum ClientMode {
    Send,
    Receive,
}

fn main() {
    let mode = if std::env::args().nth(1).as_deref() == Some("receive") {
        ClientMode::Receive
    } else {
        ClientMode::Send
    };
    println!("Client mode: {mode:?}");

    match mode {
        ClientMode::Send => send(),
        ClientMode::Receive => receive(),
    }
}

fn send() {
    let mut connections = Vec::<(usize, TcpStream)>::new();
    let mut next_connection_id = 0;
    let mut sequence = 0;

    loop {
        match TcpStream::connect(SEND_ENDPOINT) {
            Ok(stream) => {
                let connection_id = next_connection_id;
                next_connection_id += 1;
                connections.push((connection_id, stream));
                println!("opened connection {connection_id} to {SEND_ENDPOINT}");
            }
            Err(error) => {
                eprintln!("failed to connect to {SEND_ENDPOINT}: {error}");
            }
        }

        sequence += 1;
        connections.retain_mut(|(connection_id, stream)| {
            let data = format!("connection={connection_id} sequence={sequence}\n");

            if let Err(error) = stream.write_all(data.as_bytes()) {
                eprintln!("connection {connection_id} closed: {error}");
                return false;
            }

            if let Err(error) = stream.flush() {
                eprintln!("failed to flush connection {connection_id}: {error}");
                return false;
            }

            println!("sent sequence {sequence} on connection {connection_id}");
            true
        });

        thread::sleep(Duration::from_secs(3));
    }
}

fn receive() {
    let listener = TcpListener::bind(RECEIVE_ENDPOINT)
        .unwrap_or_else(|error| panic!("failed to bind {RECEIVE_ENDPOINT}: {error}"));
    println!("listening for received connections on {RECEIVE_ENDPOINT}");

    for incoming in listener.incoming() {
        let stream = match incoming {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("failed to accept received connection: {error}");
                continue;
            }
        };

        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                match line {
                    Ok(line) => println!("received: {line}"),
                    Err(error) => {
                        eprintln!("failed to read received connection: {error}");
                        break;
                    }
                }
            }
        });
    }
}
