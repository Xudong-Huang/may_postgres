use crate::config::Host;
use crate::keepalive::KeepaliveConfig;
use crate::{Error, Socket};
use may::net::TcpStream;
#[cfg(unix)]
use may::os::unix::net::UnixStream;
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::net::ToSocketAddrs;
use std::time::Duration;

pub(crate) fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    keepalive_config: Option<&KeepaliveConfig>,
) -> Result<Socket, Error> {
    match host {
        Host::Tcp(host) => {
            let h_p = format!("{}:{}", host, port);
            let addrs = h_p.to_socket_addrs().map_err(Error::io)?;
            let mut last_err = None;

            for addr in addrs {
                let connect_result = match connect_timeout {
                    Some(timeout) => TcpStream::connect_timeout(&addr, timeout),
                    None => TcpStream::connect(addr),
                };

                let stream = match connect_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        last_err = Some(Error::io(e));
                        continue;
                    }
                };

                stream.set_nodelay(true).map_err(Error::connect)?;
                if let Some(keepalive_config) = keepalive_config {
                    SockRef::from(&stream)
                        .set_tcp_keepalive(&TcpKeepalive::from(keepalive_config))
                        .map_err(Error::connect)?;
                }

                return Ok(Socket::new_tcp(stream));
            }

            Err(last_err.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve any addresses",
                ))
            }))
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            let path = path.join(format!(".s.PGSQL.{}", port));
            let socket = UnixStream::connect(path).map_err(Error::io)?;
            Ok(Socket::new_unix(socket))
        }
    }
}
