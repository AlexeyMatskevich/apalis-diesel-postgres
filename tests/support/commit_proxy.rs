//! A loopback PostgreSQL wire witness that loses exactly one committed response.
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
#[derive(Default)]
struct State {
    armed: AtomicBool,
    committed: AtomicBool,
    listening: AtomicBool,
    stopped: AtomicBool,
    sockets: Mutex<Vec<TcpStream>>,
    clients: Mutex<Vec<JoinHandle<()>>>,
}
pub struct CommitProxy {
    url: String,
    address: SocketAddr,
    state: Arc<State>,
    listener: Option<JoinHandle<()>>,
}
impl CommitProxy {
    pub fn new(url: &str) -> Result<Self, String> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or("commit proxy requires a PostgreSQL URI")?;
        let (authority, path) = rest
            .split_once('/')
            .ok_or("commit proxy requires a database URI path")?;
        let (credentials, host) = authority
            .rsplit_once('@')
            .map_or((String::new(), authority), |(auth, host)| {
                (format!("{auth}@"), host)
            });
        let endpoint = if host.starts_with('[') {
            if host.contains("]:") {
                host.to_owned()
            } else {
                format!("{host}:5432")
            }
        } else if host.contains(':') {
            host.to_owned()
        } else {
            format!("{host}:5432")
        };
        let upstream = endpoint
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .find(|addr| addr.ip().is_loopback())
            .ok_or("commit proxy requires a loopback test PostgreSQL server")?;
        let socket = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let address = socket.local_addr().map_err(|e| e.to_string())?;
        let separator = if path.contains('?') { '&' } else { '?' };
        let url = format!(
            "{scheme}://{credentials}{address}/{path}{separator}sslmode=disable&gssencmode=disable"
        );
        let state = Arc::new(State::default());
        let shared = state.clone();
        let listener = thread::spawn(move || {
            for incoming in socket.incoming() {
                if shared.stopped.load(Ordering::Acquire) {
                    break;
                }
                let Ok(client) = incoming else { break };
                let Ok(server) = TcpStream::connect_timeout(&upstream, Duration::from_secs(5))
                else {
                    continue;
                };
                let _ = client.set_nodelay(true);
                let _ = server.set_nodelay(true);
                for stream in [&client, &server] {
                    if let Ok(owned) = stream.try_clone() {
                        shared.sockets.lock().unwrap().push(owned);
                    }
                }
                let state = shared.clone();
                let handle = thread::spawn(move || relay(client, server, state));
                shared.clients.lock().unwrap().push(handle);
            }
        });
        Ok(Self {
            url,
            address,
            state,
            listener: Some(listener),
        })
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn arm(&self) {
        self.state.committed.store(false, Ordering::Release);
        self.state.armed.store(true, Ordering::Release);
    }
    pub fn listening(&self) -> bool {
        self.state.listening.load(Ordering::Acquire)
    }
    pub fn committed(&self) -> bool {
        self.state.committed.load(Ordering::Acquire)
    }
}
fn frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut tag = [0];
    stream.read_exact(&mut tag)?;
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let count = u32::from_be_bytes(length);
    if !(4..=16 * 1024 * 1024).contains(&count) {
        return Err(std::io::Error::other("invalid diagnostic frame length"));
    }
    let mut data = vec![0; (count - 4) as usize];
    stream.read_exact(&mut data)?;
    Ok((tag[0], data))
}
fn send(stream: &mut TcpStream, tag: u8, data: &[u8]) -> std::io::Result<()> {
    stream.write_all(&[tag])?;
    stream.write_all(&((data.len() + 4) as u32).to_be_bytes())?;
    stream.write_all(data)
}
fn relay(mut client: TcpStream, mut server: TcpStream, state: Arc<State>) {
    let Ok(mut front_server) = server.try_clone() else {
        return;
    };
    let Ok(mut front_client) = client.try_clone() else {
        return;
    };
    let dropping = Arc::new(AtomicBool::new(false));
    let front_drop = dropping.clone();
    let front_state = state.clone();
    let front = thread::spawn(move || {
        let result = (|| -> std::io::Result<()> {
            let mut header = [0; 4];
            front_client.read_exact(&mut header)?;
            let count = u32::from_be_bytes(header);
            if !(8..=16 * 1024 * 1024).contains(&count) {
                return Err(std::io::Error::other("invalid diagnostic startup length"));
            }
            let mut startup = vec![0; (count - 4) as usize];
            front_client.read_exact(&mut startup)?;
            front_server.write_all(&header)?;
            front_server.write_all(&startup)?;
            loop {
                let (tag, data) = frame(&mut front_client)?;
                if tag == b'Q'
                    && data == b"COMMIT\0"
                    && front_state.armed.swap(false, Ordering::AcqRel)
                {
                    front_drop.store(true, Ordering::Release);
                }
                send(&mut front_server, tag, &data)?;
            }
        })();
        let _ = result;
        let _ = front_server.shutdown(Shutdown::Both);
        let _ = front_client.shutdown(Shutdown::Both);
    });
    let result = (|| -> std::io::Result<()> {
        let mut committed = false;
        loop {
            let (tag, data) = frame(&mut server)?;
            if tag == b'C' && data == b"LISTEN\0" {
                state.listening.store(true, Ordering::Release);
            }
            if dropping.load(Ordering::Acquire) {
                if tag == b'C' {
                    committed = data == b"COMMIT\0";
                    continue;
                }
                if tag == b'Z' && data == b"I" {
                    state.committed.store(committed, Ordering::Release);
                    return Ok(());
                }
            }
            send(&mut client, tag, &data)?;
        }
    })();
    let _ = result;
    let _ = client.shutdown(Shutdown::Both);
    let _ = server.shutdown(Shutdown::Both);
    let _ = front.join();
}
impl Drop for CommitProxy {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_secs(1));
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        for socket in self.state.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
        for client in self.state.clients.lock().unwrap().drain(..) {
            let _ = client.join();
        }
    }
}
