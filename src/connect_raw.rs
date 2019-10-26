use crate::codec::{BackendMessage, BackendMessages, Framed, FrontendMessage, PostgresCodec};
use crate::config::Config;
use crate::connection::Connection;
use crate::{Client, Error};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use may::net::TcpStream;
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::ScramSha256;
use postgres_protocol::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol::message::frontend;
use std::collections::HashMap;
use std::io::{self, Write};

pub struct StartupStream {
    inner: Framed<TcpStream>,
    buf: BackendMessages,
}

impl StartupStream {
    fn send(&mut self, msg: FrontendMessage) -> io::Result<()> {
        let mut data = BytesMut::with_capacity(512);
        PostgresCodec::encode(msg, &mut data)?;
        self.inner.inner_mut().write_all(&data)
    }
}

impl Iterator for StartupStream {
    type Item = io::Result<Message>;

    fn next(&mut self) -> Option<io::Result<Message>> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Some(Ok(message)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }

            match self.inner.next() {
                Some(Ok(BackendMessage::Normal { messages, .. })) => self.buf = messages,
                Some(Ok(BackendMessage::Async(message))) => return Some(Ok(message)),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

pub fn connect_raw(stream: TcpStream, config: &Config) -> Result<Client, Error> {
    let mut stream = StartupStream {
        inner: Framed::new(stream),
        buf: BackendMessages::empty(),
    };

    startup(&mut stream, config)?;
    authenticate(&mut stream, config)?;
    let (process_id, secret_key, parameters) = read_info(&mut stream)?;

    let connection = Connection::new(stream.inner, parameters);
    let client = Client::new(connection, process_id, secret_key);

    Ok(client)
}

fn startup(stream: &mut StartupStream, config: &Config) -> Result<(), Error> {
    let mut params = vec![("client_encoding", "UTF8"), ("timezone", "GMT")];
    if let Some(user) = &config.user {
        params.push(("user", &**user));
    }
    if let Some(dbname) = &config.dbname {
        params.push(("database", &**dbname));
    }
    if let Some(options) = &config.options {
        params.push(("options", &**options));
    }
    if let Some(application_name) = &config.application_name {
        params.push(("application_name", &**application_name));
    }

    let mut buf = BytesMut::new();
    frontend::startup_message(params, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)
}

fn authenticate(stream: &mut StartupStream, config: &Config) -> Result<(), Error> {
    match stream.next().transpose().map_err(Error::io)? {
        Some(Message::AuthenticationOk) => return Ok(()),
        Some(Message::AuthenticationCleartextPassword) => {
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass)?;
        }
        Some(Message::AuthenticationMd5Password(body)) => {
            let user = config
                .user
                .as_ref()
                .ok_or_else(|| Error::config("user missing".into()))?;
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            let output = authentication::md5_hash(user.as_bytes(), pass, body.salt());
            authenticate_password(stream, output.as_bytes())?;
        }
        Some(Message::AuthenticationSasl(body)) => {
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_sasl(stream, body, pass)?;
        }
        Some(Message::AuthenticationKerberosV5)
        | Some(Message::AuthenticationScmCredential)
        | Some(Message::AuthenticationGss)
        | Some(Message::AuthenticationSspi) => {
            return Err(Error::authentication(
                "unsupported authentication method".into(),
            ))
        }
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    }

    match stream.next().transpose().map_err(Error::io)? {
        Some(Message::AuthenticationOk) => Ok(()),
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

fn authenticate_password(stream: &mut StartupStream, password: &[u8]) -> Result<(), Error> {
    let mut buf = BytesMut::new();
    frontend::password_message(password, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)
}

fn authenticate_sasl(
    stream: &mut StartupStream,
    body: AuthenticationSaslBody,
    password: &[u8],
) -> Result<(), Error> {
    let mut has_scram = false;
    let mut has_scram_plus = false;
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next().map_err(Error::parse)? {
        match mechanism {
            sasl::SCRAM_SHA_256 => has_scram = true,
            sasl::SCRAM_SHA_256_PLUS => has_scram_plus = true,
            _ => {}
        }
    }

    let (channel_binding, mechanism) = if has_scram_plus || has_scram {
        (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256)
    } else {
        return Err(Error::authentication("unsupported SASL mechanism".into()));
    };

    let mut scram = ScramSha256::new(password, channel_binding);

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response(mechanism, scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)?;

    let body = match stream.next().transpose().map_err(Error::io)? {
        Some(Message::AuthenticationSaslContinue(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .update(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    let mut buf = BytesMut::new();
    frontend::sasl_response(scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)?;

    let body = match stream.next().transpose().map_err(Error::io)? {
        Some(Message::AuthenticationSaslFinal(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .finish(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    Ok(())
}

fn read_info(stream: &mut StartupStream) -> Result<(i32, i32, HashMap<String, String>), Error> {
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut parameters = HashMap::new();

    loop {
        match stream.next().transpose().map_err(Error::io)? {
            Some(Message::BackendKeyData(body)) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            Some(Message::ParameterStatus(body)) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            Some(Message::NoticeResponse(_)) => {}
            Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key, parameters)),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}
