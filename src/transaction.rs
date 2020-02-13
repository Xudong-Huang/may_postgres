use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::copy_out::CopyStream;
use crate::into_buf::IntoBuf;
use crate::query::RowStream;
use crate::types::{ToSql, Type};
use crate::{
    bind, query, slice_iter, Client, Error, Portal, Row, SimpleQueryMessage, Statement, ToStatement,
};
use may::net::TcpStream;
use postgres_protocol::message::frontend;

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    depth: u32,
    done: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let query = if self.depth == 0 {
            "ROLLBACK".to_string()
        } else {
            format!("ROLLBACK TO sp{}", self.depth)
        };
        let buf = self.client.inner().with_buf(|buf| {
            frontend::query(&query, buf).unwrap();
            buf.split().freeze()
        });
        let _ = self
            .client
            .inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(buf)));
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Transaction<'a> {
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
    pub fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query)
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.client.prepare_typed(query, parameter_types)
    }

    /// Like `Client::query`.
    pub fn query<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query(statement, params)
    }

    /// Like `Client::query_one`.
    pub fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query_one(statement, params)
    }

    /// Like `Client::query_opt`.
    pub fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query_opt(statement, params)
    }

    /// Like `Client::query_raw`.
    pub fn query_raw<'b, T, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.query_raw(statement, params)
    }

    /// Like `Client::execute`.
    pub fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.execute(statement, params)
    }

    /// Like `Client::execute_iter`.
    pub fn execute_raw<'b, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.execute_raw(statement, params)
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created, and can only be used on the
    /// connection that created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.bind_raw(statement, slice_iter(params))
    }

    /// A maximally flexible version of [`bind`].
    ///
    /// [`bind`]: #method.bind
    pub fn bind_raw<'b, T, I>(&self, statement: &T, params: I) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(&self.client)?;
        bind::bind(self.client.inner(), statement, params)
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all rows will be returned.
    pub fn query_portal(&self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.query_portal_raw(portal, max_rows)?.collect()
    }

    /// The maximally flexible version of [`query_portal`].
    ///
    /// [`query_portal`]: #method.query_portal
    pub fn query_portal_raw(&self, portal: &Portal, max_rows: i32) -> Result<RowStream, Error> {
        query::query_portal(self.client.inner(), portal, max_rows)
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T, E, S, M>(
        &self,
        statement: &M,
        params: &[&(dyn ToSql + Sync)],
        stream: S,
    ) -> Result<u64, Error>
    where
        M: ?Sized + ToStatement,
        S: Iterator<Item = Result<T, E>>,
        T: IntoBuf,
        <T as IntoBuf>::Buf: 'static + Send,
        E: Into<Box<dyn std::error::Error + Sync + Send>> + std::fmt::Debug,
    {
        self.client.copy_in(statement, params, stream)
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<CopyStream, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.copy_out(statement, params)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.client.simple_query(query)
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query)
    }

    /// Like `Client::cancel_query`.
    pub fn cancel_query(&self) -> Result<(), Error> {
        self.client.cancel_query()
    }

    /// Like `Client::cancel_query_raw`.
    pub fn cancel_query_raw<S, T>(&self, stream: TcpStream) -> Result<(), Error> {
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
