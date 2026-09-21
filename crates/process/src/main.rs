use std::{
    os::fd::{AsRawFd, FromRawFd},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ktls::{CorkStream, KtlsStream};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
use sendfd::{RecvWithFd, SendWithFd};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional},
    net::{TcpStream, UnixListener, UnixStream},
    sync::Mutex,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const SOCKET_PATH_CORE: &str = "/tmp/porter_c.sock";
const SOCKET_PATH_HANDOFF: &str = "/tmp/porter_h.sock";
const SOCKET_PATH_STATUS: &str = "/tmp/porter_s.sock";
const UPSTREAM_ENDPOINT: &str = "127.0.0.1:4849";

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sig_polite(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

/// The two connected sockets belonging to one proxy connection.
///
/// The FD handoff order is `[inbound, outbound, inbound, outbound, ...]`.
struct ProxySockets {
    inbound: TcpStream,
    outbound: TcpStream,
}

#[derive(Debug, PartialEq, Eq)]
enum Process {
    Blue,
    Green,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let process_type = std::env::args().nth(1).unwrap_or_default();
    let process = if process_type == "green" {
        Process::Green
    } else {
        Process::Blue
    };
    println!("Process Started: {process:?}");

    match process {
        Process::Blue => run_blue().await,
        Process::Green => run_green().await,
    }
}

fn certificate_paths() -> (String, String) {
    const CERT_PATH: &str = "certs/server.cert.pem";
    const PRIVATE_KEY_PATH: &str = "certs/server.key.pem";

    let cert_path = if let Ok(path) = std::env::var("CERT_PATH") {
        path.to_owned()
    } else {
        CERT_PATH.to_string()
    };
    let private_key_path = if let Ok(path) = std::env::var("PRIVATE_KEY_PATH") {
        path.to_owned()
    } else {
        PRIVATE_KEY_PATH.to_string()
    };

    (cert_path, private_key_path)
}

fn tls_server_config() -> Result<Arc<ServerConfig>, Box<dyn std::error::Error>> {
    let (cert_path, private_key_path) = certificate_paths();
    let certs = CertificateDer::pem_file_iter(cert_path)?.collect::<Result<Vec<_>, _>>()?;
    let private_key = PrivateKeyDer::from_pem_file(private_key_path)?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)?;
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

fn tls_client_config() -> Result<Arc<ClientConfig>, Box<dyn std::error::Error>> {
    let (cert_path, _) = certificate_paths();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(cert_path)? {
        roots.add(cert?)?;
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

async fn run_blue() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_sig_polite as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_sig_polite as *const () as libc::sighandler_t,
        );
    }

    let server_config = tls_server_config()?;
    let client_config = tls_client_config()?;
    let sockets = Arc::new(Mutex::new(Vec::<ProxySockets>::new()));
    let handoff_complete = Arc::new(AtomicBool::new(false));
    let sockets_for_core = Arc::clone(&sockets);
    let sockets_for_sender = Arc::clone(&sockets);
    let server_config_for_core = Arc::clone(&server_config);
    let client_config_for_core = Arc::clone(&client_config);
    let handoff_complete_for_sender = Arc::clone(&handoff_complete);
    let core_polling_stopped = Arc::new(AtomicBool::new(false));
    let core_polling_stopped_for_core = Arc::clone(&core_polling_stopped);
    let core_polling_stopped_for_sender = Arc::clone(&core_polling_stopped);
    let active_proxies = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active_proxies_for_core = Arc::clone(&active_proxies);
    let active_proxies_for_sender = Arc::clone(&active_proxies);

    tokio::spawn(async move {
        loop {
            let fd = tokio::select! {
                result = receive_core_socket() => match result {
                    Ok(fd) => fd,
                    Err(error) => {
                        if SHUTDOWN.load(Ordering::Relaxed) {
                            break;
                        }
                        eprintln!("failed to receive socket from core: {error}");
                        continue;
                    }
                },
                _ = tokio::time::sleep(Duration::from_millis(100)),
                    if SHUTDOWN.load(Ordering::Relaxed) => break,
            };

            if SHUTDOWN.load(Ordering::Relaxed) {
                let _ = unsafe { std::net::TcpStream::from_raw_fd(fd) };
                break;
            }

            println!("Blue received inbound fd from core: {fd}");

            let inbound = match tcp_stream_from_fd(fd) {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("failed to restore inbound socket: {error}");
                    continue;
                }
            };
            let outbound = match TcpStream::connect(UPSTREAM_ENDPOINT).await {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("failed to connect upstream {UPSTREAM_ENDPOINT}: {error}");
                    continue;
                }
            };

            let (_, inbound_tls_socket) = match duplicate_tcp_stream(inbound) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate inbound socket: {error}");
                    continue;
                }
            };
            let (_, outbound_tls_socket) = match duplicate_tcp_stream(outbound) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate outbound socket: {error}");
                    continue;
                }
            };

            let inbound_tls = match TlsAcceptor::from(Arc::clone(&server_config_for_core))
                .accept(CorkStream::new(inbound_tls_socket))
                .await
            {
                Ok(stream) => match ktls::config_ktls_server(stream).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        eprintln!("failed to configure inbound kTLS: {error}");
                        continue;
                    }
                },
                Err(error) => {
                    eprintln!("inbound TLS handshake failed: {error}");
                    continue;
                }
            };
            let server_name = match ServerName::try_from("localhost") {
                Ok(name) => name,
                Err(error) => {
                    eprintln!("invalid upstream server name: {error}");
                    continue;
                }
            };
            let outbound_tls = match TlsConnector::from(Arc::clone(&client_config_for_core))
                .connect(server_name, CorkStream::new(outbound_tls_socket))
                .await
            {
                Ok(stream) => match ktls::config_ktls_client(stream).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        eprintln!("failed to configure outbound kTLS: {error}");
                        continue;
                    }
                },
                Err(error) => {
                    eprintln!("outbound TLS handshake failed: {error}");
                    continue;
                }
            };

            let (inbound_drained, inbound_socket) = inbound_tls.into_raw();
            let (outbound_drained, outbound_socket) = outbound_tls.into_raw();

            let (inbound, inbound_proxy_socket) = match duplicate_tcp_stream(inbound_socket) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate inbound kTLS socket: {error}");
                    continue;
                }
            };
            let (outbound, outbound_proxy_socket) = match duplicate_tcp_stream(outbound_socket) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate outbound kTLS socket: {error}");
                    continue;
                }
            };

            let inbound_proxy = KtlsStream::new(inbound_proxy_socket, inbound_drained);
            let outbound_proxy = KtlsStream::new(outbound_proxy_socket, outbound_drained);

            sockets_for_core
                .lock()
                .await
                .push(ProxySockets { inbound, outbound });
            active_proxies_for_core.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(proxy_ktls(
                inbound_proxy,
                outbound_proxy,
                "Blue",
                Arc::clone(&active_proxies_for_core),
            ));
        }
        core_polling_stopped_for_core.store(true, Ordering::Relaxed);
    });

    tokio::spawn(async move {
        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                while !core_polling_stopped_for_sender.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                while active_proxies_for_sender.load(Ordering::Relaxed) != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                let streams = {
                    let mut sockets = sockets_for_sender.lock().await;
                    std::mem::take(&mut *sockets)
                };
                let fds = streams
                    .iter()
                    .flat_map(|proxy| [proxy.inbound.as_raw_fd(), proxy.outbound.as_raw_fd()])
                    .collect::<Vec<_>>();

                if let Err(error) = send_sockets(&fds).await {
                    eprintln!("failed to send sockets to Green: {error}");
                } else {
                    println!("sent {} proxy socket descriptors to Green", fds.len());
                    if let Err(error) = notify_green().await {
                        eprintln!("failed to notify Green: {error}");
                    }
                }

                handoff_complete_for_sender.store(true, Ordering::Relaxed);
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    while !handoff_complete.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("Blue exiting after handoff");
    Ok(())
}

async fn run_green() -> Result<(), Box<dyn std::error::Error>> {
    let server_config = tls_server_config()?;
    let client_config = tls_client_config()?;

    println!("Green waiting for handoff on {SOCKET_PATH_HANDOFF}");
    let fds = receive_sockets(SOCKET_PATH_HANDOFF).await?;
    println!(
        "Green received handoff FD payload: {} descriptors",
        fds.len()
    );
    if fds.len() % 2 != 0 {
        return Err("received an incomplete proxy socket pair".into());
    }

    receive_handoff_complete().await?;
    println!("Green received {} proxy socket descriptors", fds.len());

    for (index, pair) in fds.chunks_exact(2).enumerate() {
        let inbound = tcp_stream_from_fd(pair[0])?;
        let outbound = tcp_stream_from_fd(pair[1])?;
        println!("Green adopted proxy connection {index}");
        tokio::spawn(proxy(inbound, outbound, "Green"));
    }

    println!("Green now polling Core for new connections");
    loop {
        let fd = receive_core_socket().await?;
        println!("Green received new inbound fd from core: {fd}");

        let server_config = Arc::clone(&server_config);
        let client_config = Arc::clone(&client_config);
        tokio::spawn(async move {
            match establish_tls_proxy(fd, &server_config, &client_config).await {
                Ok((inbound, outbound)) => {
                    println!("Green established TLS proxy for core fd {fd}");
                    tokio::spawn(proxy_ktls(
                        inbound,
                        outbound,
                        "Green",
                        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
                    ));
                }
                Err(error) => eprintln!("Green failed to establish proxy for fd {fd}: {error}"),
            }
        });
    }
}

async fn establish_tls_proxy(
    fd: i32,
    server_config: &Arc<ServerConfig>,
    client_config: &Arc<ClientConfig>,
) -> Result<(KtlsStream<TcpStream>, KtlsStream<TcpStream>), Box<dyn std::error::Error>> {
    let inbound = tcp_stream_from_fd(fd)?;
    let outbound = TcpStream::connect(UPSTREAM_ENDPOINT).await?;

    let (_, inbound_tls_socket) = duplicate_tcp_stream(inbound)?;
    let (_, outbound_tls_socket) = duplicate_tcp_stream(outbound)?;

    let inbound_tls = tokio::time::timeout(
        Duration::from_secs(10),
        TlsAcceptor::from(Arc::clone(server_config)).accept(CorkStream::new(inbound_tls_socket)),
    )
    .await??;
    let inbound_tls = ktls::config_ktls_server(inbound_tls).await?;

    let server_name = ServerName::try_from("localhost")?;
    let outbound_tls = tokio::time::timeout(
        Duration::from_secs(10),
        TlsConnector::from(Arc::clone(client_config))
            .connect(server_name, CorkStream::new(outbound_tls_socket)),
    )
    .await??;
    let outbound_tls = ktls::config_ktls_client(outbound_tls).await?;

    let (inbound_drained, inbound_socket) = inbound_tls.into_raw();
    let (outbound_drained, outbound_socket) = outbound_tls.into_raw();
    let (_, inbound_proxy_socket) = duplicate_tcp_stream(inbound_socket)?;
    let (_, outbound_proxy_socket) = duplicate_tcp_stream(outbound_socket)?;

    Ok((
        KtlsStream::new(inbound_proxy_socket, inbound_drained),
        KtlsStream::new(outbound_proxy_socket, outbound_drained),
    ))
}

async fn proxy_ktls(
    mut inbound: KtlsStream<TcpStream>,
    mut outbound: KtlsStream<TcpStream>,
    process: &'static str,
    active_proxies: Arc<std::sync::atomic::AtomicUsize>,
) {
    tokio::select! {
        result = copy_bidirectional(&mut inbound, &mut outbound) => match result {
            Ok((inbound_bytes, outbound_bytes)) => println!(
                "{process} proxy closed: inbound={inbound_bytes} bytes, outbound={outbound_bytes} bytes"
            ),
            Err(error) => eprintln!("{process} proxy failed: {error}"),
        },
        _ = wait_for_shutdown() => {
            let (_, inbound) = inbound.into_raw();
            let (_, outbound) = outbound.into_raw();
            std::mem::forget(inbound);
            std::mem::forget(outbound);
        }
    }
    active_proxies.fetch_sub(1, Ordering::Relaxed);
}

async fn wait_for_shutdown() {
    while !SHUTDOWN.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn proxy<I, O>(mut inbound: I, mut outbound: O, process: &'static str)
where
    I: AsyncRead + AsyncWrite + Unpin,
    O: AsyncRead + AsyncWrite + Unpin,
{
    match copy_bidirectional(&mut inbound, &mut outbound).await {
        Ok((inbound_bytes, outbound_bytes)) => println!(
            "{process} proxy closed: inbound={inbound_bytes} bytes, outbound={outbound_bytes} bytes"
        ),
        Err(error) => eprintln!("{process} proxy failed: {error}"),
    }
}

fn tcp_stream_from_fd(fd: i32) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(stream)?)
}

fn duplicate_tcp_stream(
    stream: TcpStream,
) -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let std_stream = stream.into_std()?;
    let reader = std_stream.try_clone()?;
    Ok((
        TcpStream::from_std(std_stream)?,
        TcpStream::from_std(reader)?,
    ))
}

async fn send_sockets(sockets: &[i32]) -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(SOCKET_PATH_HANDOFF);
    let listener = UnixListener::bind(SOCKET_PATH_HANDOFF)?;
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

async fn notify_green() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(SOCKET_PATH_STATUS);
    let listener = UnixListener::bind(SOCKET_PATH_STATUS)?;
    let (mut stream, _) = listener.accept().await?;
    stream.write_all(b"blue-stopped-fetching-core").await?;
    Ok(())
}

async fn receive_core_socket() -> Result<i32, Box<dyn std::error::Error>> {
    println!("Process connecting to Core socket {SOCKET_PATH_CORE}");
    let fds = receive_sockets(SOCKET_PATH_CORE).await?;
    if fds.len() != 1 {
        return Err(format!("expected one core socket, received {}", fds.len()).into());
    }
    Ok(fds[0])
}

async fn receive_sockets(path: &str) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let stream = loop {
        match UnixStream::connect(path).await {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };

    let mut bytes = [0u8; 1024];
    let mut fds = vec![0i32; 64];
    let (_, fd_count) = loop {
        match stream.recv_with_fd(&mut bytes, &mut fds) {
            Ok(result) => break result,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                stream.readable().await?;
            }
            Err(error) => return Err(error.into()),
        }
    };

    fds.truncate(fd_count);
    Ok(fds)
}

async fn receive_handoff_complete() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = loop {
        match UnixStream::connect(SOCKET_PATH_STATUS).await {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };

    let mut message = [0u8; 26];
    stream.read_exact(&mut message).await?;
    if &message != b"blue-stopped-fetching-core" {
        return Err("received an invalid handoff status".into());
    }
    println!("Green confirmed that Blue stopped fetching core sockets");
    Ok(())
}
