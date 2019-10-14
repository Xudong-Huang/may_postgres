use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::ToSql;
use crate::{query, Error, Statement};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;

pub fn copy_out<'a, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<CopyStream, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = query::encode(client, &statement, params)?;
    let responses = start(client, buf)?;
    Ok(CopyStream { responses })
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

pub struct CopyStream {
    responses: Responses,
}

impl Iterator for CopyStream {
    type Item = Result<Bytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match o_try!(self.responses.next()) {
            Message::CopyData(body) => Some(Ok(body.into_bytes())),
            Message::CopyDone => None,
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
