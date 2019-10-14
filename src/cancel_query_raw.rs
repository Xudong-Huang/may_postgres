use crate::Error;
use bytes::BytesMut;
use may::net::TcpStream;
use postgres_protocol::message::frontend;
use std::io::Write;

pub fn cancel_query_raw(
    mut stream: TcpStream,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error> {
    let mut buf = BytesMut::new();
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).map_err(Error::io)?;
    stream.flush().map_err(Error::io)?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(Error::io)?;

    Ok(())
}
