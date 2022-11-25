use may::io::AsIoData;

use crate::client::SocketConfig;
use crate::config::{Host, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::socket::SocketExt;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Error, SimpleQueryMessage, Socket};
use std::io;

pub fn connect<T>(mut tls: T, config: &Config) -> Result<Client, Error>
where
    T: MakeTlsConnect<Socket>,
    <T as MakeTlsConnect<Socket>>::Stream: SocketExt,
{
    if config.host.is_empty() {
        return Err(Error::config("host missing".into()));
    }

    if config.port.len() > 1 && config.port.len() != config.host.len() {
        return Err(Error::config("invalid number of ports".into()));
    }

    let mut error = None;
    for (i, host) in config.host.iter().enumerate() {
        let port = config
            .port
            .get(i)
            .or_else(|| config.port.first())
            .copied()
            .unwrap_or(5432);

        let hostname = match host {
            Host::Tcp(host) => host.as_str(),
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };

        let tls = tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        match connect_once(host, port, tls, config) {
            Ok(client) => return Ok(client),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

fn connect_once<T>(host: &Host, port: u16, tls: T, config: &Config) -> Result<Client, Error>
where
    T: TlsConnect<Socket>,
    <T as TlsConnect<Socket>>::Stream: AsIoData + SocketExt + Send,
{
    let socket = connect_socket(
        host,
        port,
        config.connect_timeout,
        if config.keepalives {
            Some(&config.keepalive_config)
        } else {
            None
        },
    )?;
    let mut client = connect_raw(socket, tls, config)?;

    if let TargetSessionAttrs::ReadWrite = config.target_session_attrs {
        let mut rows = client.simple_query_raw("SHOW transaction_read_only")?;

        loop {
            let next = rows.next();

            match next.transpose()? {
                Some(SimpleQueryMessage::Row(row)) => {
                    if row.try_get(0)? == Some("on") {
                        return Err(Error::connect(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "database does not allow writes",
                        )));
                    } else {
                        break;
                    }
                }
                Some(_) => {}
                None => return Err(Error::unexpected_message()),
            }
        }
    }

    client.set_socket_config(SocketConfig {
        host: host.clone(),
        port,
        connect_timeout: config.connect_timeout,
        keepalive: if config.keepalives {
            Some(config.keepalive_config.clone())
        } else {
            None
        },
    });

    Ok(client)
}
