use crate::query::RowStream;
use crate::types::{BorrowToSql, ToSql, Type};
use crate::{Client, Error, Row, Statement, ToStatement, Transaction};

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like `Client::execute`.
    fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::execute_raw`.
    fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like `Client::query`.
    fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_one`.
    fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_opt`.
    fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_raw`.
    fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like `Client::prepare`.
    fn prepare(&self, query: &str) -> Result<Statement, Error>;

    /// Like `Client::prepare_typed`.
    fn prepare_typed(&self, query: &str, parameter_types: &[Type]) -> Result<Statement, Error>;

    /// Like `Client::transaction`.
    fn transaction(&mut self) -> Result<Transaction<'_>, Error>;

    /// Returns a reference to the underlying `Client`.
    fn client(&self) -> &Client;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.execute(query, params)
    }

    fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params)
    }

    fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query(query, params)
    }

    fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_one(statement, params)
    }

    fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_opt(statement, params)
    }

    fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params)
    }

    fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }

    fn prepare_typed(&self, query: &str, parameter_types: &[Type]) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types)
    }

    fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction()
    }

    fn client(&self) -> &Client {
        self
    }
}

impl private::Sealed for Transaction<'_> {}

impl GenericClient for Transaction<'_> {
    fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.execute(query, params)
    }

    fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params)
    }

    fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query(query, params)
    }

    fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_one(statement, params)
    }

    fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_opt(statement, params)
    }

    fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params)
    }

    fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }

    fn prepare_typed(&self, query: &str, parameter_types: &[Type]) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types)
    }

    #[allow(clippy::needless_lifetimes)]
    fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        self.transaction()
    }

    fn client(&self) -> &Client {
        self.client()
    }
}
