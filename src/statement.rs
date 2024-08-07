use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::Type;
use bytes::BytesMut;
use postgres_protocol::message::frontend;
use std::sync::{Arc, Weak};

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let mut buf = BytesMut::new();
            frontend::close(b'S', &self.name, &mut buf).unwrap();
            frontend::sync(&mut buf);
            let _ = client.raw_send(RequestMessages::Single(FrontendMessage::Raw(buf)));
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(
        inner: &Arc<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(inner),
            name,
            params,
            columns,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

/// Information about a column of a query.
#[derive(Debug)]
pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    pub(crate) fn new(name: String, type_: Type) -> Column {
        Column { name, type_ }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}
