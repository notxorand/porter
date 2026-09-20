use std::{future::pending, os::fd::AsRawFd};

use sendfd::SendWithFd;
use tokio::net::{TcpListener, UnixListener};

const TCP_ENDPOINT: &str = "0.0.0.0:4848";
const SOCKET_PATH: &str = "/tmp/porter_c.sock";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(TCP_ENDPOINT).await {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!("failed to bind TCP listener: {error}");
                return;
            }
        };
        println!("Listening on {TCP_ENDPOINT}");

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("failed to accept TCP connection: {error}");
                    continue;
                }
            };

            if let Err(error) = send_sockets(&[stream.as_raw_fd()]).await {
                eprintln!("failed to send socket to Blue: {error}");
            }
        }
    });
    pending::<()>().await;
    Ok(())
}

async fn send_sockets(sockets: &[i32]) -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(SOCKET_PATH);
    let listener = UnixListener::bind(SOCKET_PATH)?;
    let (stream, _) = listener.accept().await?;
    let data = [1u8];
    let len = loop {
        match stream.send_with_fd(&data, sockets) {
            Ok(len) => break len,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                stream.writable().await?;
            }
            Err(error) => return Err(error.into()),
        }
    };

    if len != data.len() {
        return Err("did not send the socket payload".into());
    }

    Ok(())
}
