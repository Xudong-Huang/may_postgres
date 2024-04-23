use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::net::TcpStream;
use may::queue::mpsc::Queue;
use may::sync::spsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::Error;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const IO_BUF_SIZE: usize = 4096 * 16;

pub enum RefOrValue<'a, T> {
    Ref(&'a T),
    Value(T),
}

impl<'a, T> Deref for RefOrValue<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            RefOrValue::Ref(r) => r,
            RefOrValue::Value(ref v) => v,
        }
    }
}

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub tag: usize,
    pub messages: RequestMessages,
    pub sender: RefOrValue<'static, spsc::Sender<BackendMessages>>,
}

pub struct Response {
    tag: usize,
    tx: RefOrValue<'static, spsc::Sender<BackendMessages>>,
}

/// A connection to a PostgreSQL database.
pub(crate) struct Connection {
    io_handle: JoinHandle<()>,
    req_queue: Arc<Queue<Request>>,
    waker: WaitIoWaker,
    send_flag: Arc<AtomicBool>,
    id: usize,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rx = self.io_handle.coroutine();
        unsafe { rx.cancel() };
    }
}

// read the socket until no data
#[inline]
fn process_read(stream: &mut impl Read, read_buf: &mut BytesMut) -> io::Result<usize> {
    let remaining = read_buf.capacity() - read_buf.len();
    if remaining < 512 {
        read_buf.reserve(IO_BUF_SIZE - remaining);
    }

    let mut read_cnt = 0;
    loop {
        let buf: &mut [u8] = unsafe { std::mem::transmute(read_buf.chunk_mut()) };
        assert!(!buf.is_empty());
        match stream.read(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => {
                read_cnt += n;
                unsafe { read_buf.advance_mut(n) };
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(read_cnt),
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
        match stream.write(unsafe { write_buf.get_unchecked(written..) }) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }
    write_buf.advance(written);
    Ok(written)
}

#[inline]
fn process_write(
    stream: &mut impl Write,
    req_queue: &Queue<Request>,
    rsp_queue: &mut VecDeque<Response>,
    write_buf: &mut BytesMut,
) -> io::Result<usize> {
    let remaining = write_buf.capacity() - write_buf.len();
    if remaining < 512 {
        write_buf.reserve(IO_BUF_SIZE - remaining);
    }
    while let Some(req) = req_queue.pop() {
        rsp_queue.push_back(Response {
            tag: req.tag,
            tx: req.sender,
        });
        match req.messages {
            RequestMessages::Single(msg) => match msg {
                FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
                FrontendMessage::CopyData(data) => data.write(write_buf),
            },
            RequestMessages::CopyIn(mut rcv) => {
                let mut copy_in_msg = rcv.try_recv();
                loop {
                    match copy_in_msg {
                        Ok(Some(msg)) => {
                            match msg {
                                FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
                                FrontendMessage::CopyData(data) => data.write(write_buf),
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
    nonblock_write(stream, write_buf)
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
    req_queue: Arc<Queue<Request>>,
    send_flag: Arc<AtomicBool>,
    mut parameters: HashMap<String, String>,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let mut write_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let mut rsp_queue = VecDeque::with_capacity(1000);

    loop {
        stream.reset_io();
        let inner_stream = stream.inner_mut();
        let mut write_cnt = 0;
        if send_flag.load(Ordering::Acquire) {
            send_flag.store(false, Ordering::Relaxed);
            write_cnt = process_write(inner_stream, &req_queue, &mut rsp_queue, &mut write_buf)
                .map_err(Error::io)?;
        }

        let read_cnt = process_read(inner_stream, &mut read_buf).map_err(Error::io)?;
        if read_cnt > 0 {
            decode_messages(&mut read_buf, &mut rsp_queue, &mut parameters)?;

            if send_flag.load(Ordering::Relaxed) {
                continue;
            }
        }

        if read_cnt == 0 && (write_buf.is_empty() || write_cnt == 0) {
            stream.wait_io();
        }
    }
}

impl Connection {
    pub(crate) fn new(mut stream: TcpStream, parameters: HashMap<String, String>) -> Connection {
        use std::os::fd::AsRawFd;
        let id = stream.as_raw_fd() as usize;
        let waker = stream.waker();

        let req_queue = Arc::new(Queue::new());
        let req_queue_dup = req_queue.clone();
        let send_flag = Arc::new(AtomicBool::new(false));
        let send_flag_dup = send_flag.clone();
        let io_handle = go!(move || {
            if let Err(e) = connection_loop(&mut stream, req_queue_dup, send_flag_dup, parameters) {
                log::error!("connection error = {:?}", e);
                terminate_connection(&mut stream);
            }
        });

        Connection {
            io_handle,
            req_queue,
            waker,
            send_flag,
            id,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        self.req_queue.push(req);
        if self
            .send_flag
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.waker.wakeup();
        }
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }
}
