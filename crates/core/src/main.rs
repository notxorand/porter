use std::{
    net::TcpListener,
    os::{fd::AsRawFd, unix::net::UnixListener},
};

use sendfd::SendWithFd;

const TCP_ENDPOINT: &str = "0.0.0.0:4848";
const SOCKET_PATH: &str = "/tmp/porter_c.sock";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(TCP_ENDPOINT)?;
    println!("Listening on {TCP_ENDPOINT}");

    loop {
        let (stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("Failed to accept TCP connection: {error}");
                continue;
            }
        };

        if let Err(error) = send_sockets(&[stream.as_raw_fd()]) {
            eprintln!("Failed to send socket to process: {error}");
        }
    }
}

fn send_sockets(sockets: &[i32]) -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(SOCKET_PATH);
    let listener = UnixListener::bind(SOCKET_PATH)?;
    let (stream, _) = listener.accept()?;
    let data = [1u8];
    let len = stream.send_with_fd(&data, sockets)?;

    if len != data.len() {
        return Err("did not send the socket payload".into());
    }

    Ok(())
}
