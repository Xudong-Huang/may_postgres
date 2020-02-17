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
    inner: Framed,
    buf: BackendMessages,
}

impl StartupStream {
    fn send(&mut self, msg: FrontendMessage) -> io::Result<()> {
        let mut encoder = PostgresCodec;
        let mut data = BytesMut::with_capacity(512);
        encoder.encode(msg, &mut data)?;
        self.inner.inner_mut().write_all(&data)
    }

    fn next_msg(&mut self) -> io::Result<Message> {
        loop {
            if let Some(message) = self.buf.next()? {
                return Ok(message);
            }

            match self.inner.next_msg()? {
                BackendMessage::Normal { messages, .. } => self.buf = messages,
                BackendMessage::Async(message) => return Ok(message),
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
    match stream.next_msg().map_err(Error::io)? {
        Message::AuthenticationOk => return Ok(()),
        Message::AuthenticationCleartextPassword => {
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass)?;
        }
        Message::AuthenticationMd5Password(body) => {
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
        Message::AuthenticationSasl(body) => {
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_sasl(stream, body, pass)?;
        }
        Message::AuthenticationKerberosV5
        | Message::AuthenticationScmCredential
        | Message::AuthenticationGss
        | Message::AuthenticationSspi => {
            return Err(Error::authentication(
                "unsupported authentication method".into(),
            ))
        }
        Message::ErrorResponse(body) => return Err(Error::db(body)),
        _ => return Err(Error::unexpected_message()),
    }

    match stream.next_msg().map_err(Error::io)? {
        Message::AuthenticationOk => Ok(()),
        Message::ErrorResponse(body) => Err(Error::db(body)),
        _ => Err(Error::unexpected_message()),
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

    let body = match stream.next_msg().map_err(Error::io)? {
        Message::AuthenticationSaslContinue(body) => body,
        Message::ErrorResponse(body) => return Err(Error::db(body)),
        _ => return Err(Error::unexpected_message()),
    };

    scram
        .update(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    let mut buf = BytesMut::new();
    frontend::sasl_response(scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)?;

    let body = match stream.next_msg().map_err(Error::io)? {
        Message::AuthenticationSaslFinal(body) => body,
        Message::ErrorResponse(body) => return Err(Error::db(body)),
        _ => return Err(Error::unexpected_message()),
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
        match stream.next_msg().map_err(Error::io)? {
            Message::BackendKeyData(body) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            Message::ParameterStatus(body) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            Message::NoticeResponse(_) => {}
            Message::ReadyForQuery(_) => return Ok((process_id, secret_key, parameters)),
            Message::ErrorResponse(body) => return Err(Error::db(body)),
            _ => return Err(Error::unexpected_message()),
        }
    }
}
