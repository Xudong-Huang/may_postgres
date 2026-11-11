use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use std::vec;

use may::net::TcpStream;

use crate::config::Host;
use crate::Error;

/// TCP options applied to every connection socket, before the startup and TLS handshakes (the TLS
/// stream wraps this same socket, so the options hold on that path too).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SocketOptions {
    /// `TCP_NODELAY`: send small writes at once instead of waiting for earlier data to be
    /// acknowledged (Nagle). Pipelined clients want it on.
    pub nodelay: bool,
    /// `SO_KEEPALIVE`.
    pub keepalives: bool,
    /// Idle time before the first keepalive probe (`TCP_KEEPIDLE`).
    pub keepalives_idle: Duration,
    /// Time between probes (`TCP_KEEPINTVL`); `None` keeps the OS default.
    pub keepalives_interval: Option<Duration>,
    /// Unanswered probes before the connection is dropped (`TCP_KEEPCNT`); `None` keeps the OS
    /// default.
    pub keepalives_retries: Option<u32>,
}

pub(crate) fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    options: &SocketOptions,
) -> Result<TcpStream, Error> {
    match host {
        Host::Tcp(host) => {
            let addrs = match host.parse::<IpAddr>() {
                Ok(ip) => {
                    // avoid dealing with blocking DNS entirely if possible
                    vec![SocketAddr::new(ip, port)].into_iter()
                }
                Err(_) => dns(host, port).map_err(Error::connect)?,
            };

            let mut error = None;
            for addr in addrs {
                let new_error = match connect_with_timeout(&addr, connect_timeout) {
                    Ok(socket) => {
                        apply_socket_options(&socket, options).map_err(Error::connect)?;
                        return Ok(socket);
                    }
                    Err(e) => e,
                };
                error = Some(new_error);
            }

            let error = error.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "resolved 0 addresses",
                ))
            });
            Err(error)
        }
    }
}

/// Set [`SocketOptions`] on a connected socket.
pub(crate) fn apply_socket_options(socket: &TcpStream, options: &SocketOptions) -> io::Result<()> {
    socket.set_nodelay(options.nodelay)?;
    #[cfg(unix)]
    {
        // SAFETY: the descriptor belongs to `socket`, which outlives this borrow.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket.as_raw_fd()) };
        let sock = socket2::SockRef::from(&fd);
        if options.keepalives {
            #[allow(unused_mut)]
            let mut keepalive = socket2::TcpKeepalive::new().with_time(options.keepalives_idle);
            #[cfg(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "netbsd"
            ))]
            {
                if let Some(interval) = options.keepalives_interval {
                    keepalive = keepalive.with_interval(interval);
                }
                if let Some(retries) = options.keepalives_retries {
                    keepalive = keepalive.with_retries(retries);
                }
            }
            sock.set_tcp_keepalive(&keepalive)?;
        } else {
            sock.set_keepalive(false)?;
        }
    }
    Ok(())
}

fn dns(host: &str, port: u16) -> io::Result<vec::IntoIter<SocketAddr>> {
    // TODO: use thread pool for the blocking
    (host, port).to_socket_addrs()
}

fn connect_with_timeout(addr: &SocketAddr, timeout: Option<Duration>) -> Result<TcpStream, Error> {
    match timeout {
        Some(_timeout) => unimplemented!(), //TcpStream::connect_timeout(addr, timeout).map_err(Error::connect),
        None => TcpStream::connect(addr).map_err(Error::connect),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;

    fn connected(options: &SocketOptions) -> (std::net::TcpListener, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let socket =
            connect_socket(&Host::Tcp("127.0.0.1".to_string()), port, None, options).unwrap();
        (listener, socket)
    }

    fn read_back(socket: &TcpStream) -> (bool, bool, Duration, Duration, u32) {
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket.as_raw_fd()) };
        let sock = socket2::SockRef::from(&fd);
        (
            sock.nodelay().unwrap(),
            sock.keepalive().unwrap(),
            sock.keepalive_time().unwrap(),
            sock.keepalive_interval().unwrap(),
            sock.keepalive_retries().unwrap(),
        )
    }

    #[test]
    fn options_are_set_on_the_socket() {
        let options = SocketOptions {
            nodelay: true,
            keepalives: true,
            keepalives_idle: Duration::from_secs(61),
            keepalives_interval: Some(Duration::from_secs(11)),
            keepalives_retries: Some(4),
        };
        let (_l, socket) = connected(&options);
        assert_eq!(
            read_back(&socket),
            (
                true,
                true,
                Duration::from_secs(61),
                Duration::from_secs(11),
                4
            )
        );
    }

    #[test]
    fn options_off_leave_nagle_and_no_keepalive() {
        let options = SocketOptions {
            nodelay: false,
            keepalives: false,
            keepalives_idle: Duration::from_secs(7200),
            keepalives_interval: None,
            keepalives_retries: None,
        };
        let (_l, socket) = connected(&options);
        let (nodelay, keepalive, ..) = read_back(&socket);
        assert!(!nodelay);
        assert!(!keepalive);
    }

    #[test]
    fn config_defaults_reach_the_socket() {
        let config = crate::Config::new();
        let (_l, socket) = connected(&config.socket_options());
        let (nodelay, keepalive, idle, ..) = read_back(&socket);
        assert_eq!(nodelay, config.get_tcp_nodelay());
        assert_eq!(keepalive, config.get_keepalives());
        if keepalive {
            assert_eq!(idle, config.get_keepalives_idle());
        }
    }
}
