use crate::codec::Framed;
use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::config::{self, Config};
use crate::connect_tls::connect_tls;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::socket::SocketExt;
use crate::tls::{TlsConnect, TlsStream};
use crate::{Client, Connection, Error};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::ScramSha256;
use postgres_protocol::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};

pub struct StartupStream<S, T> {
    inner: Framed<MaybeTlsStream<S, T>>,
    buf: BackendMessages,
    delayed: VecDeque<BackendMessage>,
}

impl<S, T> StartupStream<S, T>
where
    S: Read + Write,
    T: Read + Write,
{
    pub fn send(&mut self, msg: FrontendMessage) -> io::Result<()> {
        self.inner.send(msg)
    }

    pub fn try_next(&mut self) -> io::Result<Option<Message>> {
        self.next().transpose()
    }
}

impl<S, T> Iterator for StartupStream<S, T>
where
    S: Read + Write,
    T: Read + Write,
{
    type Item = io::Result<Message>;

    fn next(&mut self) -> Option<io::Result<Message>> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Some(Ok(message)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }

            match self.inner.next_msg() {
                Ok(BackendMessage::Normal { messages, .. }) => self.buf = messages,
                Ok(BackendMessage::Async(message)) => return Some(Ok(message)),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

pub fn connect_raw<S, T>(stream: S, tls: T, config: &Config) -> Result<Client, Error>
where
    S: Read + Write + SocketExt,
    T: TlsConnect<S>,
    <T as TlsConnect<S>>::Stream: SocketExt,
{
    let stream = connect_tls(stream, config.ssl_mode, tls)?;

    let mut stream = StartupStream {
        inner: Framed::new(stream, PostgresCodec),
        buf: BackendMessages::empty(),
        delayed: VecDeque::new(),
    };

    startup(&mut stream, config)?;
    authenticate(&mut stream, config)?;
    let (process_id, secret_key, parameters) = read_info(&mut stream)?;

    let connection = Connection::new(stream.inner.into_stream(), parameters);
    let client = Client::new(connection, config.ssl_mode, process_id, secret_key);

    Ok(client)
}

fn startup<S, T>(stream: &mut StartupStream<S, T>, config: &Config) -> Result<(), Error>
where
    S: Read + Write,
    T: Read + Write,
{
    let mut params = vec![("client_encoding", "UTF8")];
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

fn authenticate<S, T>(stream: &mut StartupStream<S, T>, config: &Config) -> Result<(), Error>
where
    S: Read + Write,
    T: TlsStream,
{
    match stream.try_next().map_err(Error::io)? {
        Some(Message::AuthenticationOk) => {
            can_skip_channel_binding(config)?;
            return Ok(());
        }
        Some(Message::AuthenticationCleartextPassword) => {
            can_skip_channel_binding(config)?;

            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass)?;
        }
        Some(Message::AuthenticationMd5Password(body)) => {
            can_skip_channel_binding(config)?;

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
            authenticate_sasl(stream, body, config)?;
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

    match stream.try_next().map_err(Error::io)? {
        Some(Message::AuthenticationOk) => Ok(()),
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

fn can_skip_channel_binding(config: &Config) -> Result<(), Error> {
    match config.channel_binding {
        config::ChannelBinding::Disable | config::ChannelBinding::Prefer => Ok(()),
        config::ChannelBinding::Require => Err(Error::authentication(
            "server did not use channel binding".into(),
        )),
    }
}

fn authenticate_password<S, T>(
    stream: &mut StartupStream<S, T>,
    password: &[u8],
) -> Result<(), Error>
where
    S: Read + Write,
    T: Read + Write,
{
    let mut buf = BytesMut::new();
    frontend::password_message(password, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)
}

fn authenticate_sasl<S, T>(
    stream: &mut StartupStream<S, T>,
    body: AuthenticationSaslBody,
    config: &Config,
) -> Result<(), Error>
where
    S: Read + Write,
    T: TlsStream,
{
    let password = config
        .password
        .as_ref()
        .ok_or_else(|| Error::config("password missing".into()))?;

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

    let channel_binding = stream
        .inner
        .get_ref()
        .channel_binding()
        .tls_server_end_point
        .filter(|_| config.channel_binding != config::ChannelBinding::Disable)
        .map(sasl::ChannelBinding::tls_server_end_point);

    let (channel_binding, mechanism) = if has_scram_plus {
        match channel_binding {
            Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else if has_scram {
        match channel_binding {
            Some(_) => (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else {
        return Err(Error::authentication("unsupported SASL mechanism".into()));
    };

    if mechanism != sasl::SCRAM_SHA_256_PLUS {
        can_skip_channel_binding(config)?;
    }

    let mut scram = ScramSha256::new(password, channel_binding);

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response(mechanism, scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .map_err(Error::io)?;

    let body = match stream.try_next().map_err(Error::io)? {
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

    let body = match stream.try_next().map_err(Error::io)? {
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

fn read_info<S, T>(
    stream: &mut StartupStream<S, T>,
) -> Result<(i32, i32, HashMap<String, String>), Error>
where
    S: Read + Write,
    T: Read + Write,
{
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut parameters = HashMap::new();

    loop {
        match stream.try_next().map_err(Error::io)? {
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
            Some(msg @ Message::NoticeResponse(_)) => {
                stream.delayed.push_back(BackendMessage::Async(msg))
            }
            Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key, parameters)),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}
