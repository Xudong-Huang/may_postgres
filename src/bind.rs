use crate::client::Client;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::ToSql;
use crate::{query, Error, Portal, Statement};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub fn bind(
    client: &Client,
    statement: Statement,
    params: &[&(dyn ToSql)],
) -> Result<Portal, Error> {
    let name = format!("p{}", NEXT_ID.fetch_add(1, Ordering::SeqCst));
    let buf = client.with_buf(|buf| {
        query::encode_bind(&statement, params, &name, buf)?;
        frontend::sync(buf);
        Ok(buf.split())
    })?;

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next()? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(Portal::new(client.inner(), name, statement))
}
