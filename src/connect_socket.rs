use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;
use std::vec;

use may::net::TcpStream;

use crate::config::Host;
use crate::Error;

pub(crate) fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    _keepalives: bool,
    _keepalives_idle: Duration,
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
                        // socket.set_nodelay(true).map_err(Error::connect)?;
                        // if keepalives {
                        //     socket
                        //         .set_keepalive(Some(keepalives_idle))
                        //         .map_err(Error::connect)?;
                        // }

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
