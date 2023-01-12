use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::net::TcpStream;
use may::queue::mpsc_seg_queue::SegQueue;
use may::sync::spsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::Error;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::sync::Arc;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub tag: usize,
    pub messages: RequestMessages,
    pub sender: spsc::Sender<BackendMessages>,
}

pub struct Response {
    tag: usize,
    tx: spsc::Sender<BackendMessages>,
}

/// A connection to a PostgreSQL database.
pub(crate) struct Connection {
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
    if remaining < 512 {
        read_buf.reserve(4096 * 32 - remaining);
    }

    let mut read_cnt = 0;
    loop {
        let buf = unsafe { &mut *(read_buf.bytes_mut() as *mut _ as *mut [u8]) };
        assert!(!buf.is_empty());
        match stream.read(buf) {
            Ok(n) if n > 0 => {
                read_cnt += n;
                unsafe { read_buf.advance_mut(n) };
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(read_cnt),
            Ok(_) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Err(err) => return Err(err),
        }
    }
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
                        Some((_, Message::ErrorResponse(error))) => return Err(Error::db(error)),
                        _ => return Err(Error::unexpected_message()),
                    },
                };

                messages.tag = response.tag;
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
            Ok(n) if n > 0 => written += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
            Ok(_) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
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
    if remaining < 512 {
        write_buf.reserve(4096 * 16 - remaining);
    }
    loop {
        match req_queue.pop_bulk() {
            Some(reqs) => {
                for req in reqs {
                    rsp_queue.push_back(Response {
                        tag: req.tag,
                        tx: req.sender,
                    });
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
            }
            None => {
                if write_buf.is_empty() {
                    break;
                }
                nonblock_write(stream, write_buf)?;
            }
        }
    }

    Ok(())
}

#[inline]
fn terminate_connection(stream: &mut TcpStream) {
    let mut request = BytesMut::new();
    frontend::terminate(&mut request);
    stream.write_all(&request.freeze()).ok();
    stream.shutdown(std::net::Shutdown::Both).ok();
}

#[inline]
fn connection_loop(
    stream: &mut TcpStream,
    req_queue: Arc<SegQueue<Request>>,
    mut parameters: HashMap<String, String>,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(4096 * 32);
    let mut rsp_queue = VecDeque::with_capacity(1000);
    let mut write_buf = BytesMut::with_capacity(4096 * 16);

    loop {
        stream.reset_io();
        let inner_stream = stream.inner_mut();
        process_write(inner_stream, &req_queue, &mut rsp_queue, &mut write_buf)
            .map_err(Error::io)?;

        // if !rsp_queue.is_empty() &&
        if process_read(inner_stream, &mut read_buf).map_err(Error::io)? > 0 {
            decode_messages(&mut read_buf, &mut rsp_queue, &mut parameters)?;
        }

        stream.wait_io()
    }
}

impl Connection {
    pub(crate) fn new(mut stream: TcpStream, parameters: HashMap<String, String>) -> Connection {
        let waker = stream.waker();

        let req_queue = Arc::new(SegQueue::new());
        let req_queue_dup = req_queue.clone();
        let io_handle = go!(move || {
            if let Err(e) = connection_loop(&mut stream, req_queue_dup, parameters) {
                log::error!("connection error = {:?}", e);
                terminate_connection(&mut stream);
            }
        });

        Connection {
            io_handle,
            req_queue,
            waker,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        self.req_queue.push(req);
        self.waker.wakeup();
    }
}
