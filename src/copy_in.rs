use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::Error;
use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use may::sync::mpsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::sync::Arc;

enum CopyInMessage {
    Message(FrontendMessage),
    Done,
}

pub struct CopyInReceiver {
    receiver: mpsc::Receiver<CopyInMessage>,
    done: bool,
}

impl CopyInReceiver {
    fn new(receiver: mpsc::Receiver<CopyInMessage>) -> CopyInReceiver {
        CopyInReceiver {
            receiver,
            done: false,
        }
    }
}

impl Iterator for CopyInReceiver {
    type Item = FrontendMessage;

    fn next(&mut self) -> Option<FrontendMessage> {
        if self.done {
            return None;
        }

        match self.receiver.recv() {
            Ok(CopyInMessage::Message(message)) => Some(message),
            Ok(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = vec![];
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Some(FrontendMessage::Raw(buf))
            }
            Err(_) => {
                self.done = true;
                let mut buf = vec![];
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Some(FrontendMessage::Raw(buf))
            }
        }
    }
}

pub fn copy_in<T, E, S>(
    client: Arc<InnerClient>,
    buf: Result<Vec<u8>, Error>,
    mut stream: S,
) -> Result<u64, Error>
where
    S: Iterator<Item = Result<T, E>>,
    T: IntoBuf,
    <T as IntoBuf>::Buf: 'static + Send,
    E: Into<Box<dyn std::error::Error + Sync + Send>>,
{
    let buf = buf?;

    let (sender, receiver) = mpsc::channel();
    let receiver = CopyInReceiver::new(receiver);
    let mut responses = client.send(RequestMessages::CopyIn(receiver))?;

    sender
        .send(CopyInMessage::Message(FrontendMessage::Raw(buf)))
        .map_err(|_| Error::closed())?;

    match responses.next()? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next()? {
        Message::CopyInResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    let mut bytes = BytesMut::new();

    while let Some(buf) = stream.next().transpose().map_err(Error::copy_in_stream)? {
        let buf = buf.into_buf();

        let data: Box<dyn Buf + Send> = if buf.remaining() > 4096 {
            if bytes.is_empty() {
                Box::new(buf)
            } else {
                Box::new(bytes.take().freeze().into_buf().chain(buf))
            }
        } else {
            bytes.reserve(buf.remaining());
            bytes.put(buf);
            if bytes.len() > 4096 {
                Box::new(bytes.take().freeze().into_buf())
            } else {
                continue;
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .map_err(|_| Error::closed())?;
    }

    if !bytes.is_empty() {
        let data: Box<dyn Buf + Send> = Box::new(bytes.freeze().into_buf());
        let data = CopyData::new(data).map_err(Error::encode)?;
        sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .map_err(|_| Error::closed())?;
    }

    sender
        .send(CopyInMessage::Done)
        .map_err(|_| Error::closed())?;

    match responses.next()? {
        Message::CommandComplete(body) => {
            let rows = body
                .tag()
                .map_err(Error::parse)?
                .rsplit(' ')
                .next()
                .unwrap()
                .parse()
                .unwrap_or(0);
            Ok(rows)
        }
        _ => Err(Error::unexpected_message()),
    }
}
