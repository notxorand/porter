use std::{io::Write, net::TcpStream, thread, time::Duration};

const ENDPOINT: &str = "127.0.0.1:4848";

fn main() {
    let mut connections = Vec::<(usize, TcpStream)>::new();
    let mut next_connection_id = 0;
    let mut sequence = 0;

    loop {
        match TcpStream::connect(ENDPOINT) {
            Ok(stream) => {
                let connection_id = next_connection_id;
                next_connection_id += 1;
                connections.push((connection_id, stream));
                println!("opened connection {connection_id} to {ENDPOINT}");
            }
            Err(error) => {
                eprintln!("failed to connect to {ENDPOINT}: {error}");
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
