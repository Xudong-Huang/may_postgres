use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::try_iterator::TryIterator;
use crate::types::{IsNull, ToSql};
use crate::{Error, Portal, Row, Statement};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::Arc;

pub fn query(
    client: Arc<InnerClient>,
    statement: Statement,
    buf: Result<Vec<u8>, Error>,
) -> impl Iterator<Item = Result<Row, Error>> {
    let responses = i_try!(start(client, buf));
    TryIterator::Iter(Query {
        statement,
        responses,
    })
}

pub fn query_portal(
    client: Arc<InnerClient>,
    portal: Portal,
    max_rows: i32,
) -> impl Iterator<Item = Result<Row, Error>> {
    let mut buf = vec![];
    i_try!(frontend::execute(portal.name(), max_rows, &mut buf).map_err(Error::encode));
    frontend::sync(&mut buf);

    let responses = i_try!(client.send(RequestMessages::Single(FrontendMessage::Raw(buf))));

    TryIterator::Iter(Query {
        statement: portal.statement().clone(),
        responses,
    })
}

pub fn execute(client: Arc<InnerClient>, buf: Result<Vec<u8>, Error>) -> Result<u64, Error> {
    let mut responses = start(client, buf)?;

    loop {
        match responses.next()? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                let rows = body
                    .tag()
                    .map_err(Error::parse)?
                    .rsplit(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
                return Ok(rows);
            }
            Message::EmptyQueryResponse => return Ok(0),
            _ => return Err(Error::unexpected_message()),
        }
    }
}

fn start(client: Arc<InnerClient>, buf: Result<Vec<u8>, Error>) -> Result<Responses, Error> {
    let buf = buf?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next()? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<'a, I>(statement: &Statement, params: I) -> Result<Vec<u8>, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let mut buf = encode_bind(statement, params, "")?;
    frontend::execute("", 0, &mut buf).map_err(Error::encode)?;
    frontend::sync(&mut buf);

    Ok(buf)
}

pub fn encode_bind<'a, I>(statement: &Statement, params: I, portal: &str) -> Result<Vec<u8>, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();

    assert!(
        statement.params().len() == params.len(),
        "expected {} parameters but got {}",
        statement.params().len(),
        params.len()
    );

    let mut buf = vec![];

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        Some(1),
        params.zip(statement.params()).enumerate(),
        |(idx, (param, ty)), buf| match param.to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        &mut buf,
    );
    match r {
        Ok(()) => Ok(buf),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

struct Query {
    statement: Statement,
    responses: Responses,
}

impl Iterator for Query {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match o_try!(self.responses.next()) {
            Message::DataRow(body) => Some(Ok(o_try!(Row::new(self.statement.clone(), body)))),
            Message::EmptyQueryResponse
            | Message::CommandComplete(_)
            | Message::PortalSuspended => None,
            Message::ErrorResponse(body) => Some(Err(Error::db(body))),
            _ => Some(Err(Error::unexpected_message())),
        }
    }
}
