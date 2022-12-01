use bytes::{Buf, BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use may::net::TcpStream;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io::{self, Read};

pub use frame_codec::Framed;

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
        if self.0.is_empty() {
            return Ok(None);
        }
        backend::Message::parse(&mut self.0)
    }
}

pub struct PostgresCodec;

// impl Encoder
impl PostgresCodec {
    pub fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

// impl Decoder
impl PostgresCodec {
    pub fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
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

// #[cfg(not(unix))]
mod frame_codec {
    use super::*;

    pub struct Framed {
        r_stream: TcpStream,
        read_buf: BytesMut,
        codec: PostgresCodec,
    }

    impl Framed {
        pub fn new(s: TcpStream) -> Self {
            Framed {
                r_stream: s,
                read_buf: BytesMut::with_capacity(4096 * 8),
                codec: PostgresCodec,
            }
        }

        pub fn inner_mut(&mut self) -> &mut TcpStream {
            &mut self.r_stream
        }

        pub fn into_stream(self) -> TcpStream {
            self.r_stream
        }

        pub fn next_msg(&mut self) -> io::Result<BackendMessage> {
            loop {
                if let Some(msg) = self.codec.decode(&mut self.read_buf)? {
                    return Ok(msg);
                }
                // try to read more data from stream
                // read the socket for reqs
                if self.read_buf.capacity() - self.read_buf.len() < 1024 {
                    self.read_buf.reserve(4096 * 8);
                }

                let n = {
                    let read_buf =
                        unsafe { &mut *(self.read_buf.bytes_mut() as *mut _ as *mut [u8]) };
                    self.r_stream.read(read_buf)?
                };
                //connection was closed
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
                }
                unsafe { self.read_buf.advance_mut(n) };
            }
        }
    }
}

#[cfg(unix_asd)]
mod frame_codec {
    use super::*;

    pub struct Framed {
        r_stream: TcpStream,
        read_buf: BytesMut,
        codec: PostgresCodec,
    }

    impl Framed {
        pub fn new(s: TcpStream) -> Self {
            Framed {
                r_stream: s,
                read_buf: BytesMut::with_capacity(4096 * 8),
                codec: PostgresCodec,
            }
        }

        pub fn inner_mut(&mut self) -> &mut TcpStream {
            &mut self.r_stream
        }

        pub fn next_msg(&mut self) -> io::Result<BackendMessage> {
            loop {
                if let Some(msg) = self.codec.decode(&mut self.read_buf)? {
                    return Ok(msg);
                }
                // try to read more data from stream
                // read the socket for reqs
                if self.read_buf.capacity() - self.read_buf.len() < 1024 {
                    self.read_buf.reserve(4096 * 8);
                }

                let read_buf = unsafe { &mut *(self.read_buf.bytes_mut() as *mut _ as *mut [u8]) };

                let n = self.r_stream.read(read_buf)?;
                //connection was closed
                if n == 0 {
                    #[cold]
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
                }
                unsafe { self.read_buf.advance_mut(n) };
            }
        }
    }
}
