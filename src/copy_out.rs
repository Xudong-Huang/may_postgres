use crate::client::{Client, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{query, Error, Statement};
use bytes::{Bytes, BytesMut};
use postgres_protocol::message::backend::Message;

pub fn copy_out(client: &Client, statement: Statement) -> Result<CopyOutStream, Error> {
    let buf = query::encode(client, &statement, &[])?;
    let responses = start(client, buf)?;
    Ok(CopyOutStream { responses })
}

fn start(client: &Client, buf: BytesMut) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next()? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next()? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

/// A stream of `COPY ... TO STDOUT` query data.
pub struct CopyOutStream {
    responses: Responses,
}

impl Iterator for CopyOutStream {
    type Item = Result<Bytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match o_try!(self.responses.next()) {
            Message::CopyData(body) => Some(Ok(body.into_bytes())),
            Message::CopyDone => None,
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
