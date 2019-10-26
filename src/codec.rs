use bytes::{Buf, BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
// use may::sync::{RwLock, RwLockReadGuard};
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io::{self, Read};
// use std::sync::Arc;

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn empty() -> BackendMessages {
        BackendMessages(BytesMut::new())
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

pub struct PostgresCodec;

impl PostgresCodec {
    // impl Encoder
    pub fn encode(item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }

    // impl Decoder
    pub fn decode(src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                request_complete,
            }))
        }
    }
}

pub struct Framed<S> {
    r_stream: S,
    read_buf: BytesMut,
    // rw_lock: Arc<RwLock<()>>,
    // lock: Option<RwLockReadGuard<'static, ()>>,
}

impl<S: Read> Framed<S> {
    pub fn new(s: S) -> Self {
        // let rw_lock = Arc::new(RwLock::new(()));
        // let lock = unsafe { std::mem::transmute(rw_lock.read().unwrap()) };
        Framed {
            r_stream: s,
            read_buf: BytesMut::with_capacity(4096 * 8),
            // rw_lock,
            // lock: Some(lock),
        }
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.r_stream
    }

    // pub fn into_parts(self) -> (S, BytesMut) {
    //     (self.r_stream, self.read_buf)
    // }
}

// impl<S> Framed<S> {
//     pub fn get_rw_lock(&self) -> Arc<RwLock<()>> {
//         self.rw_lock.clone()
//     }
// }

impl<S: Read> Iterator for Framed<S> {
    type Item = io::Result<BackendMessage>;

    fn next(&mut self) -> Option<io::Result<BackendMessage>> {
        loop {
            let msg = PostgresCodec::decode(&mut self.read_buf).transpose();
            if msg.is_some() {
                return msg;
            }

            // try to read more data from stream
            // read the socket for reqs
            if self.read_buf.remaining_mut() < 1024 {
                self.read_buf.reserve(4096 * 8);
            }

            let n = {
                let read_buf = unsafe { self.read_buf.bytes_mut() };
                // drop(self.lock.take());
                let ret = self.r_stream.read(read_buf);
                // let lock = unsafe { std::mem::transmute(self.rw_lock.read().unwrap()) };
                // self.lock = Some(lock);
                match ret {
                    Ok(n) => n,
                    Err(e) => return Some(Err(e)),
                }
            };
            //connection was closed
            if n == 0 {
                #[cold]
                return None;
            }
            unsafe { self.read_buf.advance_mut(n) };
        }
    }
}
