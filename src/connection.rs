use bytes::{Buf, BufMut, BytesMut};
use crossbeam::queue::SegQueue;
use fallible_iterator::FallibleIterator;
use log::error;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::sync::mpsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::socket::SocketExt;
use crate::tls::TlsStream;
use crate::Error;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::sync::Arc;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: mpsc::Sender<BackendMessages>,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
pub struct Connection {
    io_handle: JoinHandle<()>,
    req_queue: Arc<SegQueue<Request>>,
    waker: WaitIoWaker,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rx = self.io_handle.coroutine();
        unsafe {
            rx.cancel();
        }
    }
}

// read the socket until no data
fn process_read(stream: &mut impl Read, read_buf: &mut BytesMut) -> io::Result<()> {
    let remaining = read_buf.capacity() - read_buf.len();
    if remaining < 512 {
        read_buf.reserve(4096 * 32 - remaining);
    }

    loop {
        let buf =
            unsafe { &mut *(read_buf.chunk_mut().as_uninit_slice_mut() as *mut _ as *mut [u8]) };
        match stream.read(buf) {
            Ok(n) => {
                if n > 0 {
                    unsafe { read_buf.advance_mut(n) };
                } else {
                    //connection was closed
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
                }
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                return Err(err);
            }
        }
    }
    Ok(())
}

fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &mut VecDeque<Response>,
    first_rsp: &mut Option<Response>,
    parameters: &mut HashMap<String, String>,
) -> Result<(), Error> {
    use crate::codec::PostgresCodec;

    // parse the messages
    while let Some(msg) = PostgresCodec.decode(read_buf).map_err(Error::io)? {
        match msg {
            BackendMessage::Async(Message::NoticeResponse(_body)) => {
                // let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                // return Ok(Some(AsyncMessage::Notice(error)));
            }
            BackendMessage::Async(Message::NotificationResponse(_body)) => {}
            BackendMessage::Async(Message::ParameterStatus(body)) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            BackendMessage::Async(_) => unreachable!(),
            BackendMessage::Normal {
                mut messages,
                request_complete,
            } => {
                let response = match first_rsp {
                    Some(response) => response,
                    None => match rsp_queue.pop_front() {
                        Some(response) => {
                            first_rsp.replace(response);
                            first_rsp.as_mut().unwrap()
                        }
                        None => match messages.next().map_err(Error::parse)? {
                            Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                            _ => return Err(Error::unexpected_message()),
                        },
                    },
                };

                response.sender.send(messages).ok();

                if request_complete {
                    first_rsp.take();
                }
            }
        }
    }
    Ok(())
}

fn nonblock_write(stream: &mut impl Write, write_buf: &mut BytesMut) -> io::Result<()> {
    let len = write_buf.len();
    let mut written = 0;
    while written < len {
        match stream.write(&write_buf[written..]) {
            Ok(n) => {
                if n > 0 {
                    written += n;
                } else {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
                }
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                return Err(err);
            }
        }
    }
    if written == len {
        unsafe { write_buf.set_len(0) }
    } else if written > 0 {
        write_buf.advance(written);
    }

    Ok(())
}

fn process_write(
    stream: &mut impl Write,
    req_queue: &SegQueue<Request>,
    rsp_queue: &mut VecDeque<Response>,
    write_buf: &mut BytesMut,
) -> io::Result<()> {
    let remaining = write_buf.capacity() - write_buf.len();
    if remaining < 512 {
        write_buf.reserve(4096 * 8 - remaining);
    }
    loop {
        match req_queue.pop() {
            Some(req) => {
                rsp_queue.push_back(Response { sender: req.sender });
                match req.messages {
                    RequestMessages::Single(msg) => match msg {
                        FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
                        FrontendMessage::CopyData(data) => {
                            let mut buf = BytesMut::new();
                            data.write(&mut buf);
                            write_buf.extend_from_slice(&buf.freeze())
                        }
                    },
                    RequestMessages::CopyIn(mut rcv) => {
                        let mut copy_in_msg = rcv.try_recv();
                        loop {
                            match copy_in_msg {
                                Ok(Some(msg)) => {
                                    match msg {
                                        FrontendMessage::Raw(buf) => {
                                            write_buf.extend_from_slice(&buf)
                                        }
                                        FrontendMessage::CopyData(data) => {
                                            let mut buf = BytesMut::new();
                                            data.write(&mut buf);
                                            write_buf.extend_from_slice(&buf.freeze())
                                        }
                                    }
                                    copy_in_msg = rcv.try_recv();
                                }
                                Ok(None) => {
                                    nonblock_write(stream, write_buf)?;

                                    // no data found we just write all the data and wait
                                    copy_in_msg = rcv.recv();
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
            }
            None => {
                // wait for enough time before flush the data
                may::coroutine::yield_now();
                if !req_queue.is_empty() {
                    continue;
                }

                nonblock_write(stream, write_buf)?;
                // still no new req found
                if req_queue.is_empty() {
                    break;
                }
            }
        }
    }

    Ok(())
}

impl Connection {
    pub(crate) fn new<S, T>(
        mut stream: MaybeTlsStream<S, T>,
        mut parameters: HashMap<String, String>,
    ) -> Self
    where
        S: Read + Write + SocketExt,
        T: TlsStream + SocketExt,
    {
        let waker = stream.waker();

        let req_queue = Arc::new(SegQueue::new());
        let req_queue_dup = req_queue.clone();
        let io_handle = go!(move || {
            let mut read_buf = BytesMut::with_capacity(4096 * 8);
            let mut rsp_queue = VecDeque::with_capacity(100);
            let mut first_rsp = None;
            let mut write_buf = BytesMut::with_capacity(4096 * 8);

            let mut is_error = false;

            stream.set_nonblocking(true).unwrap();
            loop {
                stream.reset_io();
                if !rsp_queue.is_empty() {
                    if let Err(e) = process_read(&mut stream, &mut read_buf) {
                        error!("receiver closed. err={}", e);
                        let mut request = BytesMut::new();
                        frontend::terminate(&mut request);
                        let (tx, _rx) = mpsc::channel();
                        let req = Request {
                            messages: RequestMessages::Single(FrontendMessage::Raw(
                                request.freeze(),
                            )),
                            sender: tx,
                        };
                        req_queue_dup.push(req);
                        is_error = true;
                    }

                    if let Err(e) = decode_messages(
                        &mut read_buf,
                        &mut rsp_queue,
                        &mut first_rsp,
                        &mut parameters,
                    ) {
                        error!("decode_messages err={}", e);
                        let mut request = BytesMut::new();
                        frontend::terminate(&mut request);
                        let (tx, _rx) = mpsc::channel();
                        let req = Request {
                            messages: RequestMessages::Single(FrontendMessage::Raw(
                                request.freeze(),
                            )),
                            sender: tx,
                        };
                        req_queue_dup.push(req);
                        is_error = true;
                    }
                }

                if let Err(e) =
                    process_write(&mut stream, &req_queue_dup, &mut rsp_queue, &mut write_buf)
                {
                    error!("writer closed. err={}", e);
                    break;
                }

                if is_error {
                    break;
                }
                stream.wait_io();
            }

            stream.shutdown(std::net::Shutdown::Both).ok();
        });

        Connection {
            io_handle,
            req_queue,
            waker,
        }
    }

    // /// Returns the value of a runtime parameter for this connection.
    // pub fn parameter(&self, name: &str) -> Option<&str> {
    //     self.parameters.get(name).map(|s| &**s)
    // }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        self.req_queue.push(req);
        self.waker.wakeup();
    }
}
