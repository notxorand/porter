use std::{io::Write, net::TcpStream, thread, time::Duration};

const ENDPOINT: &str = "127.0.0.1:4848";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Keep both connections owned by this function so they remain open.
    let mut first = TcpStream::connect(ENDPOINT)?;
    let mut second = TcpStream::connect(ENDPOINT)?;
    println!("connected two sockets to {ENDPOINT}");

    let mut sequence = 0;
    loop {
        sequence += 1;

        let first_data = format!("socket=first sequence={sequence}\n");
        let second_data = format!("socket=second sequence={sequence}\n");

        first.write_all(first_data.as_bytes())?;
        first.flush()?;
        second.write_all(second_data.as_bytes())?;
        second.flush()?;

        println!("sent sequence {sequence} on both sockets");
        thread::sleep(Duration::from_secs(1));
    }
}
