use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::try_iterator::TryIterator;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::Arc;

pub fn simple_query(
    client: Arc<InnerClient>,
    query: &str,
) -> impl Iterator<Item = Result<SimpleQueryMessage, Error>> {
    let buf = encode(query)?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    TryIterator::Iter(SimpleQuery {
        responses,
        columns: None,
    })
}

pub fn batch_execute(client: Arc<InnerClient>, query: &str) -> Result<(), Error> {
    let buf = encode(query)?;

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

fn encode(query: &str) -> Result<Vec<u8>, Error> {
    let mut buf = vec![];
    frontend::query(query, &mut buf).map_err(Error::encode)?;
    Ok(buf)
}

struct SimpleQuery {
    responses: Responses,
    columns: Option<Arc<[String]>>,
}

impl Iterator for SimpleQuery {
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
