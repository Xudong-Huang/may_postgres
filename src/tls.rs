//! TLS for the connection (rustls, ring provider), negotiated the way libpq does it.
//!
//! `sslmode`:
//! * `disable` - never TLS.
//! * `prefer` (default) - TLS when the server offers it, otherwise plaintext. No certificate checks.
//! * `require` - TLS or fail. No certificate checks, unless `sslrootcert` is set, which makes it `verify-ca` (libpq).
//! * `verify-ca` - TLS, server certificate chain checked against `sslrootcert`.
//! * `verify-full` - as `verify-ca`, and the certificate must name the host connected to.
//!
//! `sslcert` + `sslkey` (PEM files) present a client certificate when the server asks for one.
//!
//! The startup phase (TLS handshake, startup, authentication) runs on the blocking coroutine stream ([`PgStream`]
//! implements `Read`/`Write`); the connection's I/O coroutine drives rustls non-blocking against the raw socket
//! ([`tls_write`], [`tls_read`]).

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use may::net::TcpStream;
use postgres_protocol::message::frontend;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore,
    SignatureScheme,
};

use crate::config::{Config, SslMode};
use crate::Error;

/// The socket of one connection, plain or TLS.
pub(crate) struct PgStream {
    pub(crate) tcp: TcpStream,
    pub(crate) tls: Option<Box<ClientConnection>>,
}

impl PgStream {
    pub(crate) fn plain(tcp: TcpStream) -> PgStream {
        PgStream { tcp, tls: None }
    }
}

// Blocking (coroutine-suspending) I/O, used during startup and for the final Terminate.
impl Read for PgStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.tls {
            None => self.tcp.read(buf),
            Some(conn) => rustls::Stream::new(conn.as_mut(), &mut self.tcp).read(buf),
        }
    }
}

impl Write for PgStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.tls {
            None => self.tcp.write(buf),
            Some(conn) => rustls::Stream::new(conn.as_mut(), &mut self.tcp).write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.tls {
            None => self.tcp.flush(),
            Some(conn) => rustls::Stream::new(conn.as_mut(), &mut self.tcp).flush(),
        }
    }
}

/// Sends SSLRequest when the configuration asks for TLS and completes the handshake. `host` names the server for
/// SNI and `verify-full`.
pub(crate) fn negotiate(
    mut tcp: TcpStream,
    config: &Config,
    host: Option<&str>,
) -> Result<PgStream, Error> {
    let mode = config.get_ssl_mode();
    if mode == SslMode::Disable {
        return Ok(PgStream::plain(tcp));
    }
    let mut buf = BytesMut::new();
    frontend::ssl_request(&mut buf);
    tcp.write_all(&buf).map_err(Error::io)?;
    tcp.flush().map_err(Error::io)?;
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).map_err(Error::io)?;
    if answer[0] != b'S' {
        if mode == SslMode::Prefer {
            return Ok(PgStream::plain(tcp));
        }
        return Err(Error::tls("the server does not support TLS".into()));
    }

    let tls_config = client_config(config)?;
    let name = server_name(host, mode)?;
    let mut conn =
        ClientConnection::new(Arc::new(tls_config), name).map_err(|e| Error::tls(Box::new(e)))?;
    while conn.is_handshaking() {
        conn.complete_io(&mut tcp)
            .map_err(|e| Error::tls(Box::new(e)))?;
    }
    Ok(PgStream {
        tcp,
        tls: Some(Box::new(conn)),
    })
}

fn server_name(host: Option<&str>, mode: SslMode) -> Result<ServerName<'static>, Error> {
    match host {
        Some(h) => ServerName::try_from(h.to_owned())
            .map_err(|e| Error::tls(format!("invalid server name {h:?}: {e}").into())),
        None if mode == SslMode::VerifyFull => {
            Err(Error::tls("verify-full needs a host name".into()))
        }
        // Not checked in the other modes; SNI only.
        None => Ok(ServerName::try_from("localhost").expect("static name")),
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn client_config(config: &Config) -> Result<ClientConfig, Error> {
    let provider = provider();
    let mut mode = config.get_ssl_mode();
    let roots = match config.get_ssl_root_cert() {
        Some(path) => Some(root_store(path)?),
        None => None,
    };
    // libpq: require with a root certificate behaves as verify-ca.
    if mode == SslMode::Require && roots.is_some() {
        mode = SslMode::VerifyCa;
    }
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::tls(Box::new(e)))?;
    let verifier: Arc<dyn ServerCertVerifier> = match mode {
        SslMode::Prefer | SslMode::Require => Arc::new(NoServerVerification(provider.clone())),
        SslMode::VerifyCa | SslMode::VerifyFull => {
            let roots = roots.ok_or_else(|| {
                Error::config(format!("sslmode={} needs sslrootcert", mode.as_str()).into())
            })?;
            let webpki =
                WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .build()
                    .map_err(|e| Error::tls(Box::new(e)))?;
            if mode == SslMode::VerifyFull {
                webpki
            } else {
                Arc::new(ChainOnly(webpki))
            }
        }
        SslMode::Disable => unreachable!("no TLS configuration for sslmode=disable"),
    };
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let tls = match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(certs(cert)?, private_key(key)?)
            .map_err(|e| Error::tls(Box::new(e)))?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err(Error::config("sslcert and sslkey go together".into())),
    };
    Ok(tls)
}

fn pem(path: &Path) -> Result<BufReader<File>, Error> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|e| Error::config(format!("{}: {e}", path.display()).into()))
}

fn certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certs = rustls_pemfile::certs(&mut pem(path)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::config(format!("{}: {e}", path.display()).into()))?;
    if certs.is_empty() {
        return Err(Error::config(
            format!("{}: no certificate", path.display()).into(),
        ));
    }
    Ok(certs)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut pem(path)?)
        .map_err(|e| Error::config(format!("{}: {e}", path.display()).into()))?
        .ok_or_else(|| Error::config(format!("{}: no private key", path.display()).into()))
}

fn root_store(path: &Path) -> Result<RootCertStore, Error> {
    let mut roots = RootCertStore::empty();
    for cert in certs(path)? {
        roots
            .add(cert)
            .map_err(|e| Error::config(format!("{}: {e}", path.display()).into()))?;
    }
    Ok(roots)
}

/// `prefer` / `require`: encrypt, but accept any server certificate (handshake signatures are still checked).
#[derive(Debug)]
struct NoServerVerification(Arc<CryptoProvider>);

impl ServerCertVerifier for NoServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `verify-ca`: the chain must lead to the root certificate; the host name is not checked.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForName))
            | Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForNameContext {
                ..
            })) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "closed")
}

/// Non-blocking: moves `write_buf` into rustls and as much TLS as the socket takes onto it.
pub(crate) fn tls_write(
    conn: &mut ClientConnection,
    sock: &mut impl Write,
    write_buf: &mut BytesMut,
) -> io::Result<()> {
    loop {
        if write_buf.has_remaining() {
            let n = conn.writer().write(write_buf.chunk())?;
            write_buf.advance(n);
        }
        if !conn.wants_write() {
            if write_buf.has_remaining() {
                continue;
            }
            return Ok(());
        }
        match conn.write_tls(sock) {
            Ok(0) => return Err(closed()),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Non-blocking: reads TLS records from the socket until it would block and appends the plaintext to `read_buf`.
/// Returns true when the socket would block (the caller waits for I/O).
pub(crate) fn tls_read(
    conn: &mut ClientConnection,
    sock: &mut (impl Read + Write),
    read_buf: &mut BytesMut,
) -> io::Result<bool> {
    loop {
        let eof = match conn.read_tls(sock) {
            Ok(0) => true,
            Ok(_) => false,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(true),
            Err(e) => return Err(e),
        };
        let state = conn
            .process_new_packets()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut plain = state.plaintext_bytes_to_read();
        while plain > 0 {
            if read_buf.capacity() - read_buf.len() < plain {
                read_buf.reserve(plain);
            }
            let dst: &mut [u8] = unsafe { std::mem::transmute(read_buf.chunk_mut()) };
            let want = plain.min(dst.len());
            let n = conn.reader().read(&mut dst[..want])?;
            if n == 0 {
                break;
            }
            unsafe { read_buf.advance_mut(n) };
            plain -= n;
        }
        // Alerts, key updates: let rustls answer now rather than on the next request.
        while conn.wants_write() {
            match conn.write_tls(sock) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        if eof || state.peer_has_closed() {
            return Err(closed());
        }
    }
}
