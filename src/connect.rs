use crate::client::SocketConfig;
use crate::config::{Host, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::{Client, Config, Error, SimpleQueryMessage};
use std::io;

pub fn connect(config: &Config) -> Result<Client, Error> {
    if config.host.is_empty() {
        return Err(Error::config("host missing".into()));
    }

    if config.port.len() > 1 && config.port.len() != config.host.len() {
        return Err(Error::config("invalid number of ports".into()));
    }

    let mut error = None;
    for (i, host) in config.host.iter().enumerate() {
        let port = *config
            .port
            .get(i)
            .or_else(|| config.port.get(0))
            .unwrap_or(&5432);

        let hostname = match host {
            Host::Tcp(host) => &**host,
        };

        match connect_once(host, port, config) {
            Ok(client) => return Ok(client),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

fn connect_once(host: &Host, port: u16, config: &Config) -> Result<Client, Error> {
    let socket = connect_socket(
        host,
        port,
        config.connect_timeout,
        config.keepalives,
        config.keepalives_idle,
    )?;

    let mut client = connect_raw(socket, config)?;

    if let TargetSessionAttrs::ReadWrite = config.target_session_attrs {
        let mut rows = client.simple_query("SHOW transaction_read_only");

        loop {
            match rows.next().transpose()? {
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
        keepalives: config.keepalives,
        keepalives_idle: config.keepalives_idle,
    });

    Ok(client)
}
