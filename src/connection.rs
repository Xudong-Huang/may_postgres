use bytes::{Buf, BufMut, BytesMut};
use crossbeam::queue::SegQueue;
use fallible_iterator::FallibleIterator;
use log::error;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::net::TcpStream;
use may::sync::mpsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
// use crate::maybe_tls_stream::MaybeTlsStream;
// use crate::socket::SocketExt;
// use crate::tls::TlsStream;
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
    tx: mpsc::Sender<BackendMessages>,
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
#[inline]
fn process_read(stream: &mut impl Read, read_buf: &mut BytesMut) -> io::Result<usize> {
    let remaining = read_buf.capacity();
    if remaining < 1024 {
        read_buf.reserve(4096 * 128 - remaining);
    }

    let mut read_cnt = 0;

    loop {
        let buf =
            unsafe { &mut *(read_buf.chunk_mut().as_uninit_slice_mut() as *mut _ as *mut [u8]) };
        match stream.read(buf) {
            Ok(n) => {
                if n > 0 {
                    read_cnt += n;
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
    Ok(read_cnt)
}

#[inline]
fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &mut VecDeque<Response>,
    parameters: &mut HashMap<String, String>,
) -> Result<(), Error> {
    use crate::codec::PostgresCodec;

    // parse the messages
    while let Some(msg) = PostgresCodec.decode(read_buf).map_err(Error::io)? {
        match msg {
            BackendMessage::Normal {
                mut messages,
                request_complete,
            } => {
                let response = match rsp_queue.front() {
                    Some(response) => response,
                    None => match messages.next().map_err(Error::parse)? {
                        Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                        _ => return Err(Error::unexpected_message()),
                    },
                };

                response.tx.send(messages).ok();

                if request_complete {
                    rsp_queue.pop_front();
                }
            }
            BackendMessage::Async(Message::NoticeResponse(_body)) => {}
            BackendMessage::Async(Message::NotificationResponse(_body)) => {}
            BackendMessage::Async(Message::ParameterStatus(body)) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            BackendMessage::Async(_) => unreachable!(),
        }
    }
    Ok(())
}

#[inline]
fn nonblock_write(stream: &mut impl Write, write_buf: &mut BytesMut) -> io::Result<usize> {
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
    write_buf.advance(written);

    Ok(written)
}

#[inline]
fn process_write(
    stream: &mut impl Write,
    req_queue: &SegQueue<Request>,
    rsp_queue: &mut VecDeque<Response>,
    write_buf: &mut BytesMut,
) -> io::Result<()> {
    let remaining = write_buf.capacity();
    if remaining < 1024 {
        write_buf.reserve(4096 * 32 - remaining);
    }
    loop {
        match req_queue.pop() {
            Some(req) => {
                rsp_queue.push_back(Response { tx: req.sender });
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
                if write_buf.is_empty() {
                    break;
                }
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

#[inline]
fn make_terminate_req() -> Request {
    let mut request = BytesMut::new();
    frontend::terminate(&mut request);
    let (tx, _rx) = mpsc::channel();
    Request {
        messages: RequestMessages::Single(FrontendMessage::Raw(request.freeze())),
        sender: tx,
    }
}

impl Connection {
    pub(crate) fn new(mut stream: TcpStream, mut parameters: HashMap<String, String>) -> Self {
        let waker = stream.waker();

        let req_queue = Arc::new(SegQueue::new());
        let req_queue_dup = req_queue.clone();
        let io_handle = go!(move || {
            let mut read_buf = BytesMut::with_capacity(4096 * 128);
            let mut rsp_queue = VecDeque::with_capacity(1000);
            let mut write_buf = BytesMut::with_capacity(4096 * 32);

            let mut has_error = false;

            while !has_error {
                stream.reset_io();
                let inner_stream = stream.inner_mut();

                if !rsp_queue.is_empty() {
                    let read_cnt = match process_read(inner_stream, &mut read_buf) {
                        Ok(n) => n,
                        Err(e) => {
                            error!("process_read err={}", e);
                            req_queue_dup.push(make_terminate_req());
                            has_error = true;
                            0
                        }
                    };

                    if read_cnt > 0 {
                        if let Err(e) =
                            decode_messages(&mut read_buf, &mut rsp_queue, &mut parameters)
                        {
                            error!("decode_messages err={}", e);
                            req_queue_dup.push(make_terminate_req());
                            has_error = true;
                        }
                    }
                }

                match process_write(inner_stream, &req_queue_dup, &mut rsp_queue, &mut write_buf) {
                    Ok(_) => stream.wait_io(),
                    Err(e) => {
                        error!("process_write err={}", e);
                        has_error = true;
                    }
                }
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
