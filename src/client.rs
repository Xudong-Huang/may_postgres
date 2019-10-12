use crate::cancel_query;
use crate::codec::BackendMessages;
use crate::config::Host;
use crate::connection::{Connection, Request, RequestMessages};
use crate::types::{Oid, ToSql, Type};
use crate::{cancel_query_raw, copy_in, copy_out, query, Transaction};
use crate::{prepare, SimpleQueryMessage};
use crate::{simple_query, Row};
use crate::{Error, Statement};
use bytes::{Bytes, IntoBuf};
use fallible_iterator::FallibleIterator;
use may::net::TcpStream;
use may::sync::{mpsc, Mutex};
use postgres_protocol::message::backend::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Responses {
    pub fn next(&mut self) -> Result<Message, Error> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(message) => return Ok(message),
                None => {}
            }

            match self.receiver.recv() {
                Ok(messages) => self.cur = messages,
                Err(_) => return Err(Error::closed()),
            }
        }
    }
}

struct State {
    typeinfo: Option<Statement>,
    typeinfo_composite: Option<Statement>,
    typeinfo_enum: Option<Statement>,
    types: HashMap<Oid, Type>,
}

pub struct InnerClient {
    sender: Connection,
    state: Mutex<State>,
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::channel();
        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
    }

    pub fn typeinfo(&self) -> Option<Statement> {
        self.state.lock().unwrap().typeinfo.clone()
    }

    pub fn set_typeinfo(&self, statement: &Statement) {
        self.state.lock().unwrap().typeinfo = Some(statement.clone());
    }

    pub fn typeinfo_composite(&self) -> Option<Statement> {
        self.state.lock().unwrap().typeinfo_composite.clone()
    }

    pub fn set_typeinfo_composite(&self, statement: &Statement) {
        self.state.lock().unwrap().typeinfo_composite = Some(statement.clone());
    }

    pub fn typeinfo_enum(&self) -> Option<Statement> {
        self.state.lock().unwrap().typeinfo_enum.clone()
    }

    pub fn set_typeinfo_enum(&self, statement: &Statement) {
        self.state.lock().unwrap().typeinfo_enum = Some(statement.clone());
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.state.lock().unwrap().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: &Type) {
        self.state.lock().unwrap().types.insert(oid, type_.clone());
    }
}

#[derive(Clone)]
pub(crate) struct SocketConfig {
    pub host: Host,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
    pub keepalives: bool,
    pub keepalives_idle: Duration,
}

/// An asynchronous PostgreSQL client.
///
/// The client is one half of what is returned when a connection is established. Users interact with the database
/// through this client object.
pub struct Client {
    inner: Arc<InnerClient>,
    socket_config: Option<SocketConfig>,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(sender: Connection, process_id: i32, secret_key: i32) -> Client {
        Client {
            inner: Arc::new(InnerClient {
                sender,
                state: Mutex::new(State {
                    typeinfo: None,
                    typeinfo_composite: None,
                    typeinfo_enum: None,
                    types: HashMap::new(),
                }),
            }),
            socket_config: None,
            process_id,
            secret_key,
        }
    }

    pub(crate) fn inner(&self) -> Arc<InnerClient> {
        self.inner.clone()
    }

    pub(crate) fn set_socket_config(&mut self, socket_config: SocketConfig) {
        self.socket_config = Some(socket_config);
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    pub fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[])
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub fn prepare_typed(&self, query: &str, parameter_types: &[Type]) -> Result<Statement, Error> {
        prepare::prepare(self.inner(), query, parameter_types)
    }

    /// Executes a statement, returning a stream of the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query(
        &self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Iterator<Item = Result<Row, Error>> {
        self.inner.sender.read_lock();
        let buf = query::encode(statement, params.iter().cloned());
        query::query(self.inner(), statement.clone(), buf)
    }

    /// Like [`query`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`query`]: #method.query
    pub fn query_iter<'a, I>(
        &self,
        statement: &Statement,
        params: I,
    ) -> impl Iterator<Item = Result<Row, Error>>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = query::encode(statement, params);
        query::query(self.inner(), statement.clone(), buf)
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn execute(&self, statement: &Statement, params: &[&dyn ToSql]) -> Result<u64, Error> {
        let buf = query::encode(statement, params.iter().cloned());
        query::execute(self.inner(), buf)
    }

    /// Like [`execute`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`execute`]: #method.execute
    pub fn execute_iter<'a, I>(&self, statement: &Statement, params: I) -> Result<u64, Error>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = query::encode(statement, params);
        query::execute(self.inner(), buf)
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows created.
    ///
    /// The data in the provided stream is passed along to the server verbatim; it is the caller's responsibility to
    /// ensure it uses the proper format.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn copy_in<T, E, S>(
        &self,
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
        let buf = query::encode(statement, params.iter().cloned());
        copy_in::copy_in(self.inner(), buf, stream)
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn copy_out(
        &self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Iterator<Item = Result<Bytes, Error>> {
        let buf = query::encode(statement, params.iter().cloned());
        copy_out::copy_out(self.inner(), buf)
    }

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a stream over the
    /// rows, this method returns a stream over an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(
        &self,
        query: &str,
    ) -> impl Iterator<Item = Result<SimpleQueryMessage, Error>> {
        simple_query::simple_query(self.inner(), query)
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn batch_execute(&self, query: &str) -> Result<(), Error> {
        simple_query::batch_execute(self.inner(), query)
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub fn transaction(&self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction::new(self))
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    pub fn cancel_query(&self) -> Result<(), Error> {
        cancel_query::cancel_query(self.socket_config.clone(), self.process_id, self.secret_key)
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub fn cancel_query_raw(&self, stream: TcpStream) -> Result<(), Error> {
        cancel_query_raw::cancel_query_raw(stream, self.process_id, self.secret_key)
    }

    /// Determines if the connection to the server has already closed.
    ///
    /// In that case, all future queries will fail.
    pub fn _is_closed(&self) -> bool {
        // self.inner.sender.is_closed()
        unimplemented!()
    }
}
