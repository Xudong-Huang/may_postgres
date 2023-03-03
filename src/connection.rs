use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::net::TcpStream;
use may::queue::spsc::Queue;
use may::sync::{spsc, Mutex};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::Error;

use std::collections::HashMap;
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

struct QueueWriterInner {
    stream: TcpStream,
    write_buf: BytesMut,
}

struct QueueWriter {
    inner: Mutex<QueueWriterInner>,
}

impl QueueWriter {
    fn new(stream: TcpStream) -> Self {
        Self {
            inner: Mutex::new(QueueWriterInner {
                stream,
                write_buf: BytesMut::with_capacity(IO_BUF_SIZE),
            }),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_stream(&self) -> &mut TcpStream {
        #[allow(clippy::cast_ref_to_mut)]
        let me = unsafe { &mut *(self as *const _ as *mut Self) };
        let inner = me.inner.get_mut().unwrap();
        &mut inner.stream
    }

    /// return Ok(true) if all data is send out
    pub fn write_data(&self, data: &[u8]) -> io::Result<bool> {
        #[cfg(feature = "default")]
        let mut inner = self.inner.lock().unwrap();
        #[cfg(not(feature = "default"))]
        let inner = {
            #[allow(clippy::cast_ref_to_mut)]
            let me = unsafe { &mut *(self as *const _ as *mut Self) };
            me.inner.get_mut().unwrap()
        };
        inner.write_data(data)
    }

    /// flush all the data
    pub fn write_flush(&self) -> io::Result<()> {
        #[cfg(feature = "default")]
        let mut inner = self.inner.lock().unwrap();
        #[cfg(not(feature = "default"))]
        let inner = {
            #[allow(clippy::cast_ref_to_mut)]
            let me = unsafe { &mut *(self as *const _ as *mut Self) };
            me.inner.get_mut().unwrap()
        };
        inner.write_flush()
    }

    #[inline]
    pub fn with_stream<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut TcpStream) -> R,
    {
        let mut inner = self.inner.lock().unwrap();
        f(&mut inner.stream)
    }
}

impl QueueWriterInner {
    #[inline]
    fn write_flush(&mut self) -> io::Result<()> {
        let n = nonblock_write(self.stream.inner_mut(), &self.write_buf)?;
        self.write_buf.advance(n);
        Ok(())
    }

    #[inline]
    fn write_data(&mut self, data: &[u8]) -> io::Result<bool> {
        if self.write_buf.is_empty() {
            let n = nonblock_write(self.stream.inner_mut(), data)?;
            if n == data.len() {
                return Ok(true);
            }
            self.write_buf.extend_from_slice(&data[n..]);
        } else {
            self.write_buf.extend_from_slice(data);
        }
        Ok(false)
    }
}

/// A connection to a PostgreSQL database.
pub(crate) struct Connection {
    io_handle: JoinHandle<()>,
    writer: Arc<QueueWriter>,
    rsp_queue: Arc<Queue<Response>>,
    waker: WaitIoWaker,
    send_flag: Arc<AtomicBool>,
    id: usize,
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
fn process_read(stream: &mut impl Read, req_buf: &mut BytesMut) -> io::Result<usize> {
    let remaining = req_buf.capacity();
    if remaining < 512 {
        req_buf.reserve(IO_BUF_SIZE - remaining);
    }

    let read_buf: &mut [u8] = unsafe { std::mem::transmute(&mut *req_buf.bytes_mut()) };
    let len = read_buf.len();
    let mut read_cnt = 0;
    while read_cnt < len {
        match stream.read(unsafe { read_buf.get_unchecked_mut(read_cnt..) }) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => read_cnt += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    unsafe { req_buf.advance_mut(read_cnt) };
    Ok(read_cnt)
}

#[inline]
fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &Queue<Response>,
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
                let response = match unsafe { rsp_queue.peek() } {
                    Some(response) => response,
                    None => match messages.next().map_err(Error::parse)? {
                        Some((_, Message::ErrorResponse(error))) => return Err(Error::db(error)),
                        _ => return Err(Error::unexpected_message()),
                    },
                };

                messages.tag = response.tag;
                response.tx.send(messages).ok();

                if request_complete {
                    rsp_queue.pop();
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
fn nonblock_write(stream: &mut impl Write, data: &[u8]) -> io::Result<usize> {
    let len = data.len();
    let mut written = 0;
    while written < len {
        match stream.write(&data[written..]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }
    Ok(written)
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
    writer: Arc<QueueWriter>,
    rsp_queue: Arc<Queue<Response>>,
    send_flag: Arc<AtomicBool>,
    mut parameters: HashMap<String, String>,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let stream = unsafe { writer.as_stream() };

    loop {
        stream.reset_io();
        if send_flag.load(Ordering::Acquire) {
            send_flag.store(false, Ordering::Relaxed);
            writer.write_flush().map_err(Error::io)?;
        }

        // if !rsp_queue.is_empty() &&
        if process_read(stream.inner_mut(), &mut read_buf).map_err(Error::io)? > 0 {
            decode_messages(&mut read_buf, &rsp_queue, &mut parameters)?;
        } else {
            stream.wait_io();
        }
    }
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, parameters: HashMap<String, String>) -> Connection {
        use std::os::fd::AsRawFd;
        let id = stream.as_raw_fd() as usize;
        let waker = stream.waker();

        let writer = Arc::new(QueueWriter::new(stream));
        let writer_dup = writer.clone();
        let rsp_queue = Arc::new(Queue::new());
        let rsp_queue_dup = rsp_queue.clone();
        let send_flag = Arc::new(AtomicBool::new(false));
        let send_flag_dup = send_flag.clone();
        let io_handle = go!(move || {
            let stream = writer_dup.clone();
            if let Err(e) = connection_loop(writer_dup, rsp_queue_dup, send_flag_dup, parameters) {
                log::error!("connection error = {:?}", e);
                stream.with_stream(terminate_connection);
            }
        });

        Connection {
            io_handle,
            writer,
            rsp_queue,
            waker,
            send_flag,
            id,
        }
    }

    #[inline]
    fn write_req(&self, req: Request) -> io::Result<()> {
        self.rsp_queue.push(Response {
            tag: req.tag,
            tx: req.sender,
        });
        let write_done = match req.messages {
            RequestMessages::Single(msg) => match msg {
                FrontendMessage::Raw(buf) => self.writer.write_data(&buf)?,
                FrontendMessage::CopyData(data) => {
                    // TODO: optimize this
                    let mut tmp = BytesMut::with_capacity(IO_BUF_SIZE);
                    data.write(&mut tmp);
                    self.writer.write_data(&tmp)?
                }
            },
            RequestMessages::CopyIn(mut rcv) => {
                let mut copy_in_msg = rcv.try_recv();
                loop {
                    match copy_in_msg {
                        Ok(Some(msg)) => {
                            match msg {
                                FrontendMessage::Raw(buf) => {
                                    self.writer.write_data(&buf)?;
                                }
                                FrontendMessage::CopyData(data) => {
                                    // TODO: optimize this
                                    let mut tmp = BytesMut::with_capacity(IO_BUF_SIZE);
                                    data.write(&mut tmp);
                                    self.writer.write_data(&tmp)?;
                                }
                            }
                            copy_in_msg = rcv.try_recv();
                        }
                        Ok(None) => {
                            self.writer.write_flush()?;

                            // no data found we just write all the data and wait
                            copy_in_msg = rcv.recv();
                        }
                        Err(_) => break false,
                    }
                }
            }
        };

        if !write_done {
            self.send_flag.store(true, Ordering::Release);
            self.waker.wakeup();
        }
        Ok(())
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        self.write_req(req).unwrap();
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }
}
