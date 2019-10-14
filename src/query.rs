use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{IsNull, ToSql};
use crate::{Error, Portal, Row, Statement};
use bytes::{Bytes, BytesMut};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

pub fn query<'a, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    client.sender.read_lock();
    let buf = encode(client, &statement, params)?;
    let responses = start(client, buf)?;
    Ok(RowStream {
        statement,
        responses,
    })
}

pub fn query_portal(
    client: &InnerClient,
    portal: &Portal,
    max_rows: i32,
) -> Result<RowStream, Error> {
    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.take().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        statement: portal.statement().clone(),
        responses,
    })
}

pub fn execute<'a, I>(client: &InnerClient, statement: Statement, params: I) -> Result<u64, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = encode(client, &statement, params)?;
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

fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next()? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<'a, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.take().freeze())
    })
}

pub fn encode_bind<'a, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
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
        buf,
    );
    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

pub struct RowStream {
    statement: Statement,
    responses: Responses,
}

impl Iterator for RowStream {
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
