use std::{
    os::fd::{AsRawFd, FromRawFd},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use sendfd::{RecvWithFd, SendWithFd};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::{TcpStream, UnixListener, UnixStream},
    sync::Mutex,
};

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

    let sockets = Arc::new(Mutex::new(Vec::<ProxySockets>::new()));
    let handoff_complete = Arc::new(AtomicBool::new(false));
    let sockets_for_core = Arc::clone(&sockets);
    let sockets_for_sender = Arc::clone(&sockets);
    let handoff_complete_for_sender = Arc::clone(&handoff_complete);

    tokio::spawn(async move {
        loop {
            let fd = match receive_core_socket().await {
                Ok(fd) => fd,
                Err(error) => {
                    eprintln!("failed to receive socket from core: {error}");
                    continue;
                }
            };
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

            let (inbound, inbound_reader) = match duplicate_tcp_stream(inbound) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate inbound socket: {error}");
                    continue;
                }
            };
            let (outbound, outbound_reader) = match duplicate_tcp_stream(outbound) {
                Ok(streams) => streams,
                Err(error) => {
                    eprintln!("failed to duplicate outbound socket: {error}");
                    continue;
                }
            };

            sockets_for_core
                .lock()
                .await
                .push(ProxySockets { inbound, outbound });
            tokio::spawn(proxy(inbound_reader, outbound_reader, "Blue"));
        }
    });

    tokio::spawn(async move {
        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
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
    loop {
        let fds = receive_sockets(SOCKET_PATH_HANDOFF).await?;
        if fds.len() % 2 != 0 {
            return Err("received an incomplete proxy socket pair".into());
        }

        receive_handoff_complete().await?;
        println!("Green received {} proxy socket descriptors", fds.len());

        for pair in fds.chunks_exact(2) {
            let inbound = tcp_stream_from_fd(pair[0])?;
            let outbound = tcp_stream_from_fd(pair[1])?;
            tokio::spawn(proxy(inbound, outbound, "Green"));
        }
    }
}

async fn proxy(mut inbound: TcpStream, mut outbound: TcpStream, process: &'static str) {
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
