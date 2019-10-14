//! An asynchronous, pipelined, PostgreSQL client.
//!
//! # Example
//!
//! ```no_run
//! use may_postgres::{Error, Row};
//!
//! fn main() -> Result<(), Error> {
//!     // Connect to the database.
//!     let client= may_postgres::connect("host=localhost user=postgres")?;
//!
//!     // Now we can prepare a simple statement that just returns its parameter.
//!     let stmt = client.prepare("SELECT $1::TEXT")?;
//!
//!     // And then execute it, returning a Stream of Rows which we collect into a Vec.
//!     let rows: Vec<Row> = client
//!         .query(&stmt, &[&"hello world"])?;
//!
//!     // Now we can check that we got back the same string we sent over.
//!     let value: &str = rows[0].get(0);
//!     assert_eq!(value, "hello world");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Behavior
//!
//! Calling a method like `Client::query` on its own does nothing. The associated request is not sent to the database
//! until the future returned by the method is first polled. Requests are executed in the order that they are first
//! polled, not in the order that their futures are created.
//!
//! # Pipelining
//!
//! The client supports *pipelined* requests. Pipelining can improve performance in use cases in which multiple,
//! independent queries need to be executed. In a traditional workflow, each query is sent to the server after the
//! previous query completes. In contrast, pipelining allows the client to send all of the queries to the server up
//! front, minimizing time spent by one side waiting for the other to finish sending data:
//!
//! ```not_rust
//!             Sequential                              Pipelined
//! | Client         | Server          |    | Client         | Server          |
//! |----------------|-----------------|    |----------------|-----------------|
//! | send query 1   |                 |    | send query 1   |                 |
//! |                | process query 1 |    | send query 2   | process query 1 |
//! | receive rows 1 |                 |    | send query 3   | process query 2 |
//! | send query 2   |                 |    | receive rows 1 | process query 3 |
//! |                | process query 2 |    | receive rows 2 |                 |
//! | receive rows 2 |                 |    | receive rows 3 |                 |
//! | send query 3   |                 |
//! |                | process query 3 |
//! | receive rows 3 |                 |
//! ```
//!
//! In both cases, the PostgreSQL server is executing the queries sequentially - pipelining just allows both sides of
//! the connection to work concurrently when possible.
//!
//! Pipelining happens automatically when futures are polled concurrently (for example, by using the futures `join`
//! combinator):
//!
#![doc(html_root_url = "https://docs.rs/may-postgres/0.1.0")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

macro_rules! o_try {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Some(Err(std::convert::From::from(err))),
        }
    };
}

pub use crate::client::Client;
pub use crate::config::Config;
use crate::error::DbError;
pub use crate::error::Error;
pub use crate::portal::Portal;
pub use crate::query::RowStream;
pub use crate::row::{Row, SimpleQueryRow};
pub use crate::to_statement::ToStatement;
pub use crate::transaction::Transaction;
pub use crate::types::ToSql;
pub use statement::{Column, Statement};

mod bind;
mod cancel_query;
mod cancel_query_raw;
mod client;
mod codec;
pub mod config;
mod connect;
mod connect_raw;
mod connect_socket;
mod connection;
mod copy_in;
mod copy_out;
pub mod error;
mod portal;
mod prepare;
mod query;
pub mod row;
mod simple_query;
mod statement;
mod to_statement;
mod transaction;
pub mod types;
mod vec_buf;

/// A convenience function which parses a connection string and connects to the database.
///
/// See the documentation for [`Config`] for details on the connection string format.
///
/// Requires the `runtime` Cargo feature (enabled by default).
///
/// [`Config`]: config/struct.Config.html
pub fn connect(config: &str) -> Result<Client, Error> {
    let config = config.parse::<Config>()?;
    config.connect()
}

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

impl Notification {
    /// The process ID of the notifying backend process.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// The name of the channel that the notify has been raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The "payload" string passed from the notifying process.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

/// Message returned by the `SimpleQuery` stream.
pub enum SimpleQueryMessage {
    /// A row of data.
    Row(SimpleQueryRow),
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    CommandComplete(u64),
    #[doc(hidden)]
    __NonExhaustive,
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}
