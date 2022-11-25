use crate::config::SslMode;
use crate::tls::TlsConnect;
use crate::{connect_tls, Error};
use bytes::BytesMut;
use postgres_protocol::message::frontend;
use std::io::{Read, Write};

pub fn cancel_query_raw<S, T>(
    stream: S,
    mode: SslMode,
    tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: Read + Write,
    T: TlsConnect<S>,
{
    let mut stream = connect_tls::connect_tls(stream, mode, tls)?;

    let mut buf = BytesMut::new();
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).map_err(Error::io)?;
    stream.flush().map_err(Error::io)?;
    // TODO: stream.shutdown().map_err(Error::io)?;

    Ok(())
}
