use crate::client::{Client, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{query, Error, Statement};
use bytes::{Buf, BufMut, BytesMut};
use may::sync::mpsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::marker::PhantomData;

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

    pub fn try_recv(&mut self) -> Result<Option<FrontendMessage>, ()> {
        use std::sync::mpsc::TryRecvError;
        if self.done {
            return Err(());
        }

        match self.receiver.try_recv() {
            Ok(CopyInMessage::Message(message)) => Ok(Some(message)),
            Ok(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Ok(Some(FrontendMessage::Raw(buf.freeze())))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(_) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Ok(Some(FrontendMessage::Raw(buf.freeze())))
            }
        }
    }

    pub fn recv(&mut self) -> Result<Option<FrontendMessage>, ()> {
        if self.done {
            return Err(());
        }

        match self.receiver.recv() {
            Ok(CopyInMessage::Message(message)) => Ok(Some(message)),
            Ok(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Ok(Some(FrontendMessage::Raw(buf.freeze())))
            }
            Err(_) => {
                self.done = true;
                let mut buf = BytesMut::new();
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Ok(Some(FrontendMessage::Raw(buf.freeze())))
            }
        }
    }
}

/// A sink for `COPY ... FROM STDIN` query data.
///
/// The copy *must* be explicitly completed via the `Sink::close` or `finish` methods. If it is
/// not, the copy will be aborted.
pub struct CopyInSink<T> {
    sender: mpsc::Sender<CopyInMessage>,
    responses: Responses,
    buf: BytesMut,
    _p: PhantomData<T>,
}

impl<T> CopyInSink<T>
where
    T: Buf + 'static + Send,
{
    /// send a buf
    pub fn send(&mut self, item: T) -> Result<(), Error> {
        let data: Box<dyn Buf + Send> = if item.remaining() > 4096 {
            if self.buf.is_empty() {
                Box::new(item)
            } else {
                Box::new(self.buf.split().freeze().chain(item))
            }
        } else {
            self.buf.put(item);
            if self.buf.len() > 4096 {
                Box::new(self.buf.split().freeze())
            } else {
                return Ok(());
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        self.sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .map_err(|_| Error::closed())
    }

    /// send iterator of bufs
    pub fn send_all<S>(&mut self, items: &mut S) -> Result<(), Error>
    where
        S: Iterator<Item = Result<T, Error>>,
    {
        for item in items {
            self.send(item?)?;
        }
        Ok(())
    }

    /// Completes the copy, returning the number of rows inserted.
    ///
    /// The `Sink::close` method is equivalent to `finish`, except that it does not return the
    /// number of rows.
    pub fn finish(&mut self) -> Result<u64, Error> {
        // flush the remaining data
        let data: Box<dyn Buf + Send> = Box::new(self.buf.split().freeze());
        if data.remaining() > 0 {
            let data = CopyData::new(data).map_err(Error::encode)?;
            self.sender
                .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
                .map_err(|_| Error::closed())?;
        }

        self.sender
            .send(CopyInMessage::Done)
            .map_err(|_| Error::closed())?;

        match self.responses.next()? {
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
}

pub fn copy_in<T>(client: &Client, statement: Statement) -> Result<CopyInSink<T>, Error>
where
    T: Buf + 'static + Send,
{
    // debug!("executing copy in statement {}", statement.name());

    let buf = query::encode(client, &statement, &[])?;

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

    Ok(CopyInSink {
        sender,
        responses,
        buf: BytesMut::new(),
        _p: PhantomData,
    })
}
