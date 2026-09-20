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
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixListener, UnixStream},
    sync::Mutex,
};

const SOCKET_PATH_CORE: &str = "/tmp/porter_c.sock";
const SOCKET_PATH_HANDOFF: &str = "/tmp/porter_h.sock";
const SOCKET_PATH_STATUS: &str = "/tmp/porter_s.sock";

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sig_polite(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

/// `Blue` is a process that has open sockets.
///
/// `Green` is a process that takes over from `Blue`.
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

    let sockets = Arc::new(Mutex::new(Vec::<TcpStream>::new()));
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

            println!("Blue received fd from core: {fd}");

            let std_stream = match unsafe { std::net::TcpStream::from_raw_fd(fd) } {
                stream => stream,
            };
            if let Err(error) = std_stream.set_nonblocking(true) {
                eprintln!("failed to configure core socket: {error}");
                continue;
            }
            let reader = match std_stream.try_clone() {
                Ok(reader) => reader,
                Err(error) => {
                    eprintln!("failed to clone core socket: {error}");
                    continue;
                }
            };
            let stream = match TcpStream::from_std(std_stream) {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("failed to restore core socket: {error}");
                    continue;
                }
            };
            let reader = match TcpStream::from_std(reader) {
                Ok(reader) => reader,
                Err(error) => {
                    eprintln!("failed to restore core reader: {error}");
                    continue;
                }
            };

            sockets_for_core.lock().await.push(stream);

            tokio::spawn(async move {
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("Blue Request: {line}");
                }
            });
        }
    });

    tokio::spawn(async move {
        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                let streams = {
                    let mut sockets = sockets_for_sender.lock().await;
                    std::mem::take(&mut *sockets)
                };
                let fds = streams.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>();

                if let Err(error) = send_sockets(&fds).await {
                    eprintln!("failed to send sockets to Green: {error}");
                } else {
                    println!("sent {} sockets to Green", fds.len());
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
        receive_handoff_complete().await?;
        println!("Green received {} handoff sockets", fds.len());

        for fd in fds {
            let stream = tcp_stream_from_fd(fd)?;

            tokio::spawn(async move {
                let mut lines = BufReader::new(stream).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("Green Request: {line}");
                }
            });
        }
    }
}

fn tcp_stream_from_fd(fd: i32) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(stream)?)
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
    let mut fds = vec![0i32; 16];
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
