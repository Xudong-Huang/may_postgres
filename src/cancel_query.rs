use crate::client::SocketConfig;
// use crate::config::Host;
use crate::{cancel_query_raw, connect_socket, Error};
use std::io;

pub(crate) fn cancel_query(
    config: Option<SocketConfig>,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error> {
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )))
        }
    };

    // let hostname = match &config.host {
    //     Host::Tcp(host) => &**host,
    // };

    let socket = connect_socket::connect_socket(
        &config.host,
        config.port,
        config.connect_timeout,
        config.keepalives,
        config.keepalives_idle,
    )?;

    cancel_query_raw::cancel_query_raw(socket, process_id, secret_key)
}
