use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};

const SEND_ENDPOINT: &str = "127.0.0.1:4848";
const RECEIVE_ENDPOINT: &str = "0.0.0.0:4849";
const CERT_PATH: &str = "certs/server.cert.pem";
const PRIVATE_KEY_PATH: &str = "certs/server.key.pem";

#[derive(Debug, PartialEq, Eq)]
enum ClientMode {
    Send,
    Receive,
}

fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

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
    let config =
        client_config().unwrap_or_else(|error| panic!("failed to load TLS client config: {error}"));
    let mut connections = Vec::<(usize, StreamOwned<ClientConnection, TcpStream>)>::new();
    let mut next_connection_id = 0;
    let mut sequence = 0;

    loop {
        match TcpStream::connect(SEND_ENDPOINT).and_then(|stream| {
            stream.set_write_timeout(Some(Duration::from_secs(1)))?;
            stream.set_read_timeout(Some(Duration::from_secs(1)))?;

            let server_name = ServerName::try_from("localhost")
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let connection = ClientConnection::new(Arc::clone(&config), server_name)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok(StreamOwned::new(connection, stream))
        }) {
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
    let config =
        server_config().unwrap_or_else(|error| panic!("failed to load TLS server config: {error}"));
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

        let config = Arc::clone(&config);
        thread::spawn(move || {
            let connection = match ServerConnection::new(config) {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("failed to create TLS server connection: {error}");
                    return;
                }
            };
            let stream = StreamOwned::new(connection, stream);

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

fn client_config() -> Result<Arc<ClientConfig>, Box<dyn std::error::Error>> {
    let cert_path = std::env::var("CERT_PATH").unwrap_or_else(|_| CERT_PATH.to_string());
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(cert_path)? {
        roots.add(cert?)?;
    }

    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn server_config() -> Result<Arc<ServerConfig>, Box<dyn std::error::Error>> {
    let cert_path = std::env::var("CERT_PATH").unwrap_or_else(|_| CERT_PATH.to_string());
    let private_key_path =
        std::env::var("PRIVATE_KEY_PATH").unwrap_or_else(|_| PRIVATE_KEY_PATH.to_string());
    let certs = CertificateDer::pem_file_iter(cert_path)?.collect::<Result<Vec<_>, _>>()?;
    let private_key = PrivateKeyDer::from_pem_file(private_key_path)?;

    Ok(Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)?,
    ))
}
