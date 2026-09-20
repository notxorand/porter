use std::{
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::net::{UnixListener, UnixStream},
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use sendfd::{RecvWithFd, SendWithFd};

const TCP_ENDPOINT: &str = "0.0.0.0:4848";
const SOCKET_PATH: &str = "/tmp/porter.sock";

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let process_type = std::env::args().nth(1).unwrap_or_default();
    let process = if process_type == "green" {
        Process::Green
    } else {
        Process::Blue
    };
    println!("Process Started: {process:?}");

    match process {
        Process::Blue => run_blue(),
        Process::Green => run_green(),
    }
}

fn run_blue() -> Result<(), Box<dyn std::error::Error>> {
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
    let handoff_complete_for_sender = Arc::clone(&handoff_complete);
    let sockets_for_listener = Arc::clone(&sockets);
    let sockets_for_sender = Arc::clone(&sockets);

    thread::spawn(move || {
        let listener = TcpListener::bind(TCP_ENDPOINT).unwrap();
        println!("Listening on {TCP_ENDPOINT}");

        for incoming in listener.incoming() {
            let stream = match incoming {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("failed to accept TCP connection: {error}");
                    continue;
                }
            };

            let reader = match stream.try_clone() {
                Ok(reader) => reader,
                Err(error) => {
                    eprintln!("failed to clone TCP stream: {error}");
                    continue;
                }
            };

            sockets_for_listener.lock().unwrap().push(stream);

            thread::spawn(move || {
                for line in BufReader::new(reader).lines() {
                    match line {
                        Ok(line) => println!("Blue Request: {line}"),
                        Err(error) => {
                            eprintln!("failed to read from Blue socket: {error}");
                            break;
                        }
                    }
                }
            });
        }
    });

    thread::spawn(move || {
        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                let streams = {
                    let mut sockets = sockets_for_sender.lock().unwrap();
                    std::mem::take(&mut *sockets)
                };
                let fds = streams.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>();

                if let Err(error) = send_sockets(&fds) {
                    eprintln!("failed to send sockets to Green: {error}");
                } else {
                    println!("sent {} sockets to Green", fds.len());
                }

                handoff_complete_for_sender.store(true, Ordering::Relaxed);
                break;
            }

            thread::sleep(Duration::from_millis(100));
        }
    });

    while !handoff_complete.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    println!("Blue exiting after handoff");
    Ok(())
}

fn run_green() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let fds = receive_sockets()?;

        for fd in fds {
            println!("Received fd: {fd}");
            let stream = unsafe { TcpStream::from_raw_fd(fd) };

            thread::spawn(move || {
                for line in BufReader::new(stream).lines() {
                    match line {
                        Ok(line) => println!("Green Request: {line}"),
                        Err(error) => {
                            eprintln!("failed to read from Green socket: {error}");
                            break;
                        }
                    }
                }
            });
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

fn receive_sockets() -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let stream = loop {
        match UnixStream::connect(SOCKET_PATH) {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error.into()),
        }
    };

    let mut bytes = [0u8; 1024];
    let mut fds = vec![0; 16];
    let (_, fd_count) = stream.recv_with_fd(&mut bytes, &mut fds)?;
    fds.truncate(fd_count);
    println!("Received {fd_count} socket descriptors");
    Ok(fds)
}
