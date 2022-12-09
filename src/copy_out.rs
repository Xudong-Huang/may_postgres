use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{query, slice_iter, Error, Statement};
use bytes::Bytes;
use log::debug;
use postgres_protocol::message::backend::Message;

pub fn copy_out(client: &InnerClient, statement: Statement) -> Result<CopyOutStream, Error> {
    debug!("executing copy out statement {}", statement.name());
    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    let responses = start(client, buf)?;
    Ok(CopyOutStream { responses })
}

fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
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
