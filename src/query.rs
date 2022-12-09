use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{BorrowToSql, IsNull};
use crate::{Error, Portal, Row, Statement};
use bytes::{Bytes, BytesMut};
// use log::{debug, log_enabled, Level};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::fmt;

struct BorrowToSqlParamsDebug<'a, T>(&'a [T]);

impl<'a, T> fmt::Debug for BorrowToSqlParamsDebug<'a, T>
where
    T: BorrowToSql,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|x| x.borrow_to_sql()))
            .finish()
    }
}

pub fn query<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    // let buf = if log_enabled!(Level::Debug) {
    //     let params = params.into_iter().collect::<Vec<_>>();
    //     debug!(
    //         "executing statement {} with parameters: {:?}",
    //         statement.name(),
    //         BorrowToSqlParamsDebug(params.as_slice()),
    //     );
    //     encode(client, &statement, params)?
    // } else {
    //     encode(client, &statement, params)?
    // };
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
        let remaining = buf.capacity();
        if remaining < 1024 {
            buf.reserve(4096 * 32 - remaining);
        }
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        statement: portal.statement().clone(),
        responses,
    })
}

pub fn execute<P, I>(client: &InnerClient, statement: Statement, params: I) -> Result<u64, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    // let buf = if log_enabled!(Level::Debug) {
    //     let params = params.into_iter().collect::<Vec<_>>();
    //     debug!(
    //         "executing statement {} with parameters: {:?}",
    //         statement.name(),
    //         BorrowToSqlParamsDebug(params.as_slice()),
    //     );
    //     encode(client, &statement, params)?
    // } else {
    //     encode(client, &statement, params)?
    // };
    let buf = encode(client, &statement, params)?;
    let mut responses = start(client, buf)?;

    let mut rows = 0;
    loop {
        match responses.next()? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                rows = body
                    .tag()
                    .map_err(Error::parse)?
                    .rsplit(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
            }
            Message::EmptyQueryResponse => rows = 0,
            Message::ReadyForQuery(_) => return Ok(rows),
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

pub fn encode<P, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        let remaining = buf.capacity();
        if remaining < 1024 {
            buf.reserve(4096 * 32 - remaining);
        }

        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<P, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let param_types = statement.params();
    let params = params.into_iter();

    assert!(
        param_types.len() == params.len(),
        "expected {} parameters but got {}",
        param_types.len(),
        params.len()
    );

    let (param_formats, params): (Vec<_>, Vec<_>) = params
        .zip(param_types.iter())
        .map(|(p, ty)| (p.borrow_to_sql().encode_format(ty) as i16, p))
        .unzip();

    let params = params.into_iter();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        param_formats,
        params.zip(param_types).enumerate(),
        |(idx, (param, ty)), buf| match param.borrow_to_sql().to_sql_checked(ty, buf) {
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

/// A stream of table rows.
pub struct RowStream {
    statement: Statement,
    responses: Responses,
}

impl Iterator for RowStream {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match o_try!(self.responses.next()) {
                Message::DataRow(body) => {
                    return Some(Ok(o_try!(Row::new(self.statement.clone(), body))))
                }
                Message::EmptyQueryResponse
                | Message::CommandComplete(_)
                | Message::PortalSuspended => {}
                Message::ReadyForQuery(_) => return None,
                _ => return Some(Err(Error::unexpected_message())),
            };
        }
    }
}
