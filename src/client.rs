use crate::cancel_query;
use crate::codec::BackendMessages;
use crate::config::Host;
use crate::connection::{Connection, RefOrValue, Request, RequestMessages};
use crate::copy_out::CopyOutStream;
use crate::query::RowStream;
use crate::simple_query::SimpleQueryStream;
use crate::types::{Oid, ToSql, Type};
use crate::{
    copy_in, copy_out, prepare, query, simple_query, slice_iter, CancelToken, CopyInSink, Error,
    Row, SimpleQueryMessage, Statement, ToStatement, Transaction, TransactionBuilder,
};
use bytes::{Buf, BytesMut};
use fallible_iterator::FallibleIterator;
use may::sync::spsc;
use postgres_protocol::message::backend::Message;
use spin::Mutex;

use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub struct Responses {
    tag: usize,
    cur: BackendMessages,
    rx: Rc<spsc::Receiver<BackendMessages>>,
}

impl Responses {
    #[inline]
    pub fn next(&mut self) -> Result<Message, Error> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some((_, Message::ErrorResponse(body))) => return Err(Error::db(body)),
                Some((_, message)) => return Ok(message),
                None => {}
            }

            match self.rx.recv() {
                Ok(messages) => {
                    if messages.tag != self.tag {
                        continue;
                    }
                    self.cur = messages
                }
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
    // buf: BytesMut,
}

pub struct InnerClient {
    pub(crate) sender: Connection,
    state: Mutex<State>,
}

struct CoChannel {
    tag: Cell<usize>,
    rx: Rc<spsc::Receiver<BackendMessages>>,
    tx: spsc::Sender<BackendMessages>,
}

impl CoChannel {
    fn sender(&self) -> RefOrValue<'static, spsc::Sender<BackendMessages>> {
        // Safety:
        // 1. there is only one sender
        // 2. we will wait until all response come back
        RefOrValue::Ref(unsafe { std::mem::transmute(&self.tx) })
    }

    fn tag(&self) -> usize {
        let tag = self.tag.get();
        self.tag.set(tag + 1);
        tag
    }

    fn receiver(&self) -> Rc<spsc::Receiver<BackendMessages>> {
        self.rx.clone()
    }
}

impl InnerClient {
    /// ignore the result
    pub fn raw_send(&self, messages: RequestMessages) -> Result<(), Error> {
        let (sender, _rx) = spsc::channel();
        let request = Request {
            tag: 0,
            messages,
            sender: RefOrValue::Value(sender),
        };
        self.sender.send(request);
        Ok(())
    }

    pub fn typeinfo(&self) -> Option<Statement> {
        self.state.lock().typeinfo.clone()
    }

    pub fn set_typeinfo(&self, statement: &Statement) {
        self.state.lock().typeinfo = Some(statement.clone());
    }

    pub fn typeinfo_composite(&self) -> Option<Statement> {
        self.state.lock().typeinfo_composite.clone()
    }

    pub fn set_typeinfo_composite(&self, statement: &Statement) {
        self.state.lock().typeinfo_composite = Some(statement.clone());
    }

    pub fn typeinfo_enum(&self) -> Option<Statement> {
        self.state.lock().typeinfo_enum.clone()
    }

    pub fn set_typeinfo_enum(&self, statement: &Statement) {
        self.state.lock().typeinfo_enum = Some(statement.clone());
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.state.lock().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: &Type) {
        self.state.lock().types.insert(oid, type_.clone());
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
    buf: UnsafeCell<BytesMut>,
    co_ch: CoChannel,
}

// Client is Send but not Sync
unsafe impl Send for Client {}

impl Clone for Client {
    fn clone(&self) -> Client {
        let co_ch = {
            let (tx, rx) = spsc::channel();
            let rx = Rc::new(rx);
            let tag = Cell::new(0);
            CoChannel { tag, rx, tx }
        };
        Client {
            inner: self.inner.clone(),
            socket_config: self.socket_config.clone(),
            process_id: self.process_id,
            secret_key: self.secret_key,
            buf: UnsafeCell::new(BytesMut::with_capacity(4096)),
            co_ch,
        }
    }
}

impl Client {
    pub(crate) fn new(sender: Connection, process_id: i32, secret_key: i32) -> Client {
        let co_ch = {
            let (tx, rx) = spsc::channel();
            let rx = Rc::new(rx);
            let tag = Cell::new(0);
            CoChannel { tag, rx, tx }
        };
        Client {
            inner: Arc::new(InnerClient {
                sender,
                state: Mutex::new(State {
                    typeinfo: None,
                    typeinfo_composite: None,
                    typeinfo_enum: None,
                    types: HashMap::new(),
                    // buf: BytesMut::new(),
                }),
            }),
            socket_config: None,
            process_id,
            secret_key,
            buf: UnsafeCell::new(BytesMut::with_capacity(4096)),
            co_ch,
        }
    }

    pub(crate) fn inner(&self) -> &Arc<InnerClient> {
        &self.inner
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
        prepare::prepare(self, query, parameter_types)
    }

    /// Executes a statement, returning a vector of the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_raw(statement, slice_iter(params))?.collect()
    }

    /// Executes a statement which returns a single row, returning it.
    ///
    /// Returns an error if the query does not return exactly one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query_one<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        let mut stream = self.query_raw(statement, slice_iter(params))?;

        let row = match stream.next().transpose()? {
            Some(row) => row,
            None => return Err(Error::row_count()),
        };

        if stream.next().transpose()?.is_some() {
            return Err(Error::row_count());
        }

        Ok(row)
    }

    /// Executes a statements which returns zero or one rows, returning it.
    ///
    /// Returns an error if the query returns more than one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let mut stream = self.query_raw(statement, slice_iter(params))?;

        let row = match stream.next().transpose()? {
            Some(row) => row,
            None => return Ok(None),
        };

        if stream.next().transpose()?.is_some() {
            return Err(Error::row_count());
        }

        Ok(Some(row))
    }

    /// The maximally flexible version of [`query`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// [`query`]: #method.query
    pub fn query_raw<'a, T, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self)?;
        query::query(self, statement, params)
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute_raw(statement, slice_iter(params))
    }

    /// The maximally flexible version of [`execute`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// [`execute`]: #method.execute
    pub fn execute_raw<'a, T, I>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self)?;
        query::execute(self, statement, params)
    }

    /// Executes a `COPY FROM STDIN` statement, returning a sink used to write the copy data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any. The copy *must*
    /// be explicitly completed via the `Sink::close` or `finish` methods. If it is not, the copy will be aborted.
    ///
    /// # Panics
    ///
    /// Panics if the statement contains parameters.
    pub fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, Error>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static + Send,
    {
        let statement = statement.__convert().into_statement(self)?;
        copy_in::copy_in(self, statement)
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any.
    ///
    /// # Panics
    ///
    /// Panics if the statement contains parameters.
    pub fn copy_out<T>(&self, statement: &T) -> Result<CopyOutStream, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = statement.__convert().into_statement(self)?;
        copy_out::copy_out(self, statement)
    }

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a list of the
    /// rows, this method returns a list of an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_raw(query)?.collect()
    }

    pub(crate) fn simple_query_raw(&self, query: &str) -> Result<SimpleQueryStream, Error> {
        simple_query::simple_query(self, query)
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
        simple_query::batch_execute(self, query)
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction::new(self))
    }

    /// Returns a builder for a transaction with custom settings.
    ///
    /// Unlike the `transaction` method, the builder can be used to control the transaction's isolation level and other
    /// attributes.
    pub fn build_transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder::new(self)
    }

    /// Constructs a cancellation token that can later be used to request
    /// cancellation of a query running on the connection associated with
    /// this client.
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken {
            socket_config: self.socket_config.clone(),
            process_id: self.process_id,
            secret_key: self.secret_key,
        }
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    pub fn cancel_query(&self) -> Result<(), Error> {
        cancel_query::cancel_query(self.socket_config.clone(), self.process_id, self.secret_key)
    }

    /// Determines if the connection to the server has already closed.
    ///
    /// In that case, all future queries will fail.
    pub fn _is_closed(&self) -> bool {
        // self.inner.sender.is_closed()
        unimplemented!()
    }

    #[inline]
    pub(crate) fn with_buf<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let buf = unsafe { &mut *self.buf.get() };
        f(buf)
    }

    #[inline]
    pub(crate) fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let tag = self.co_ch.tag();
        let sender = self.co_ch.sender();
        let request = Request {
            tag,
            messages,
            sender,
        };
        self.inner.sender.send(request);

        Ok(Responses {
            tag,
            cur: BackendMessages::empty(tag),
            rx: self.co_ch.receiver(),
        })
    }

    /// connection id
    #[inline]
    pub fn id(&self) -> usize {
        self.inner.sender.id()
    }
}
