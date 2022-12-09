use crate::config::SslMode;
// use crate::tls::TlsConnect;
#[cfg(feature = "runtime")]
use crate::{cancel_query, client::SocketConfig};
use crate::{cancel_query_raw, Error};
use std::io::{Read, Write};

/// The capability to request cancellation of in-progress queries on a
/// connection.
#[derive(Clone)]
pub struct CancelToken {
    #[cfg(feature = "runtime")]
    pub(crate) socket_config: Option<SocketConfig>,
    pub(crate) ssl_mode: SslMode,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
}

impl CancelToken {
    /// Attempts to cancel the in-progress query on the connection associated
    /// with this `CancelToken`.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Cancellation is inherently racy. There is no guarantee that the
    /// cancellation request will reach the server before the query terminates
    /// normally, or that the connection associated with this token is still
    /// active.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub fn cancel_query(&self) -> Result<(), Error> {
        cancel_query::cancel_query(
            self.socket_config.clone(),
            self.ssl_mode,
            self.process_id,
            self.secret_key,
        )
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub fn cancel_query_raw<S>(&self, stream: S) -> Result<(), Error>
    where
        S: Read + Write,
    {
        cancel_query_raw::cancel_query_raw(stream, self.ssl_mode, self.process_id, self.secret_key)
    }
}
