use crate::client::{Client, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::Arc;

pub fn simple_query(client: &Client, query: &str) -> Result<SimpleQueryStream, Error> {
    let buf = encode(client, query)?;
    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(SimpleQueryStream {
        responses,
        columns: None,
    })
}

pub fn batch_execute(client: &Client, query: &str) -> Result<(), Error> {
    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next()? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

fn encode(client: &Client, query: &str) -> Result<BytesMut, Error> {
    client.with_buf(|mut buf| {
        frontend::query(query, &mut buf).map_err(Error::encode)?;
        Ok(buf)
    })
}

/// A stream of simple query results.
pub struct SimpleQueryStream {
    responses: Responses,
    columns: Option<Arc<[String]>>,
}

impl Iterator for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match o_try!(self.responses.next()) {
                Message::CommandComplete(body) => {
                    let rows = body.tag().map_err(Error::parse);
                    let rows = o_try!(rows)
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap_or(0);
                    return Some(Ok(SimpleQueryMessage::CommandComplete(rows)));
                }
                Message::EmptyQueryResponse => {
                    return Some(Ok(SimpleQueryMessage::CommandComplete(0)));
                }
                Message::RowDescription(body) => {
                    let columns = body
                        .fields()
                        .map(|f| Ok(f.name().to_string()))
                        .collect::<Vec<_>>()
                        .map_err(Error::parse);
                    let columns = o_try!(columns).into();
                    self.columns = Some(columns);
                }
                Message::DataRow(body) => {
                    let row = match &self.columns {
                        Some(columns) => o_try!(SimpleQueryRow::new(columns.clone(), body)),
                        None => return Some(Err(Error::unexpected_message())),
                    };
                    return Some(Ok(SimpleQueryMessage::Row(row)));
                }
                Message::ReadyForQuery(_) => return None,
                _ => return Some(Err(Error::unexpected_message())),
            }
        }
    }
}
