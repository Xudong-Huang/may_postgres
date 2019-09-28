use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::try_iterator::TryIterator;
use crate::Error;
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use std::sync::Arc;

pub fn copy_out(
    client: Arc<InnerClient>,
    buf: Result<Vec<u8>, Error>,
) -> impl Iterator<Item = Result<Bytes, Error>> {
    let responses = i_try!(start(client, buf));
    TryIterator::Iter(CopyOut { responses })
}

fn start(client: Arc<InnerClient>, buf: Result<Vec<u8>, Error>) -> Result<Responses, Error> {
    let buf = buf?;
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

struct CopyOut {
    responses: Responses,
}

impl Iterator for CopyOut {
    type Item = Result<Bytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match o_try!(self.responses.next()) {
            Message::CopyData(body) => Some(Ok(body.into_bytes())),
            Message::CopyDone => None,
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
