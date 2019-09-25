use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{ToSql, Type};
use crate::{bind, query, Client, Error, Portal, Row, SimpleQueryMessage, Statement};
use bytes::{Bytes, IntoBuf};
use may::net::TcpStream;
use postgres_protocol::message::frontend;

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a Client,
    depth: u32,
    done: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let mut buf = vec![];
        let query = if self.depth == 0 {
            "ROLLBACK".to_string()
        } else {
            format!("ROLLBACK TO sp{}", self.depth)
        };
        frontend::query(&query, &mut buf).unwrap();
        let _ = self
            .client
            .inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(buf)));
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a Client) -> Transaction<'a> {
        Transaction {
            client,
            depth: 0,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if self.depth == 0 {
            "COMMIT".to_string()
        } else {
            format!("RELEASE sp{}", self.depth)
        };
        self.client.batch_execute(&query)
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if self.depth == 0 {
            "ROLLBACK".to_string()
        } else {
            format!("ROLLBACK TO sp{}", self.depth)
        };
        self.client.batch_execute(&query)
    }

    /// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query)
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(
        &mut self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.client.prepare_typed(query, parameter_types)
    }

    /// Like `Client::query`.
    pub fn query(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Iterator<Item = Result<Row, Error>> {
        self.client.query(statement, params)
    }

    /// Like `Client::query_iter`.
    pub fn query_iter<'b, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Iterator<Item = Result<Row, Error>> + 'static
    where
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        // https://github.com/rust-lang/rust/issues/63032
        let buf = query::encode(statement, params);
        query::query(self.client.inner(), statement.clone(), buf)
    }

    /// Like `Client::execute`.
    pub fn execute(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Result<u64, Error> {
        self.client.execute(statement, params)
    }

    /// Like `Client::execute_iter`.
    pub fn execute_iter<'b, I>(&mut self, statement: &Statement, params: I) -> Result<u64, Error>
    where
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        // https://github.com/rust-lang/rust/issues/63032
        let buf = query::encode(statement, params);
        query::execute(self.client.inner(), buf)
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created, and can only be used on the
    /// connection that created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Result<Portal, Error> {
        // https://github.com/rust-lang/rust/issues/63032
        let buf = bind::encode(statement, params.iter().cloned());
        bind::bind(self.client.inner(), statement.clone(), buf)
    }

    /// Like [`bind`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`bind`]: #method.bind
    pub fn bind_iter<'b, I>(&mut self, statement: &Statement, params: I) -> Result<Portal, Error>
    where
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = bind::encode(statement, params);
        bind::bind(self.client.inner(), statement.clone(), buf)
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all rows will be returned.
    pub fn query_portal(
        &mut self,
        portal: &Portal,
        max_rows: i32,
    ) -> impl Iterator<Item = Result<Row, Error>> {
        query::query_portal(self.client.inner(), portal.clone(), max_rows)
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T, E, S>(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
        stream: S,
    ) -> Result<u64, Error>
    where
        S: Iterator<Item = Result<T, E>>,
        T: IntoBuf,
        <T as IntoBuf>::Buf: 'static + Send,
        E: Into<Box<dyn std::error::Error + Sync + Send>>,
    {
        self.client.copy_in(statement, params, stream)
    }

    /// Like `Client::copy_out`.
    pub fn copy_out(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Iterator<Item = Result<Bytes, Error>> {
        self.client.copy_out(statement, params)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(
        &mut self,
        query: &str,
    ) -> impl Iterator<Item = Result<SimpleQueryMessage, Error>> {
        self.client.simple_query(query)
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query)
    }

    /// Like `Client::cancel_query`.
    pub fn cancel_query(&mut self) -> Result<(), Error> {
        self.client.cancel_query()
    }

    /// Like `Client::cancel_query_raw`.
    pub fn cancel_query_raw<S, T>(&mut self, stream: TcpStream) -> Result<(), Error> {
        self.client.cancel_query_raw(stream)
    }

    /// Like `Client::transaction`.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let depth = self.depth + 1;
        let query = format!("SAVEPOINT sp{}", depth);
        self.batch_execute(&query)?;

        Ok(Transaction {
            client: self.client,
            depth,
            done: false,
        })
    }
}
