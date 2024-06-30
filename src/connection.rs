use bytes::{BufMut, BytesMut};
use cueue::{Reader, Writer};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::net::TcpStream;
use may::queue::spsc::Queue;
use may::sync::spsc;
use may::sync::Mutex;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::Error;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::ops::Deref;
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
    // client send part
    req_writer: Arc<Mutex<Writer<u8>>>,
    // client receive part
    rsp_queue: Arc<Queue<Response>>,
    waker: WaitIoWaker,
    id: usize,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rx = self.io_handle.coroutine();
        unsafe { rx.cancel() };
    }
}

#[inline]
pub(crate) fn reserve_buf(buf: &mut BytesMut) {
    let rem = buf.capacity() - buf.len();
    if rem < 1024 {
        buf.reserve(IO_BUF_SIZE - rem);
    }
}

#[inline]
#[cold]
fn err<T>(e: io::Error) -> io::Result<T> {
    Err(e)
}

#[inline]
fn nonblock_write(stream: &mut impl Write, write_buf: &[u8]) -> io::Result<usize> {
    let len = write_buf.len();
    let mut write_cnt = 0;
    while write_cnt < len {
        match stream.write(unsafe { write_buf.get_unchecked(write_cnt..) }) {
            Ok(0) => return err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => write_cnt += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return err(e),
        }
    }
    Ok(write_cnt)
}

#[inline]
fn nonblock_read(stream: &mut impl Read, read_buf: &mut BytesMut) -> io::Result<usize> {
    reserve_buf(read_buf);
    let buf: &mut [u8] = unsafe { std::mem::transmute(read_buf.chunk_mut()) };
    let len = buf.len();
    let mut read_cnt = 0;
    while read_cnt < len {
        match stream.read(unsafe { buf.get_unchecked_mut(read_cnt..) }) {
            Ok(0) => return err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => read_cnt += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return err(e),
        }
    }
    unsafe { read_buf.advance_mut(read_cnt) };
    Ok(read_cnt)
}

#[inline]
fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &Queue<Response>,
    msg_queue: &mut VecDeque<BackendMessages>,
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
                msg_queue.push_back(messages);

                if request_complete {
                    while let Some(messages) = msg_queue.pop_front() {
                        response.tx.send(messages).ok();
                    }
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
fn write_req(writer: &mut Writer<u8>, data: &[u8]) {
    let len = data.len();
    let mut write_cnt = 0;
    loop {
        let mut write_buf = writer.write_chunk();
        let cnt = write_buf
            .write(unsafe { data.get_unchecked(write_cnt..) })
            .unwrap();
        let buf_empty = write_buf.is_empty();
        writer.commit(cnt);
        write_cnt += cnt;
        if write_cnt < len {
            if unlikely(buf_empty) {
                // let the consumer to consume the data and try again
                may::coroutine::yield_now();
            }
        } else {
            break;
        }
    }
}

#[inline]
fn process_req(req: Request, rsp_queue: &Queue<Response>, write_buf: &Mutex<Writer<u8>>) {
    let mut writer = write_buf.lock().unwrap();
    rsp_queue.push(Response {
        tag: req.tag,
        tx: req.sender,
    });
    match req.messages {
        RequestMessages::Single(msg) => match msg {
            FrontendMessage::Raw(buf) => write_req(&mut writer, &buf),
            FrontendMessage::CopyData(_data) => todo!(), // data.write(writer),
        },
        RequestMessages::CopyIn(mut rcv) => {
            let mut copy_in_msg = rcv.try_recv();
            loop {
                match copy_in_msg {
                    Ok(Some(msg)) => {
                        match msg {
                            FrontendMessage::Raw(buf) => write_req(&mut writer, &buf),
                            FrontendMessage::CopyData(_data) => todo!(), // data.write(write_buf),
                        }
                        copy_in_msg = rcv.try_recv();
                    }
                    Ok(None) => {
                        // no data found we just write all the data and wait
                        copy_in_msg = rcv.recv();
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn terminate_connection(stream: &mut TcpStream) {
    let mut request = BytesMut::new();
    frontend::terminate(&mut request);
    stream.write_all(&request.freeze()).ok();
    stream.shutdown(std::net::Shutdown::Both).ok();
}

#[inline]
fn connection_loop(
    stream: &mut TcpStream,
    rsp_queue: Arc<Queue<Response>>,
    mut req_reader: Reader<u8>,
    mut parameters: HashMap<String, String>,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let mut msg_queue = VecDeque::with_capacity(1000);

    loop {
        stream.reset_io();
        let inner_stream = stream.inner_mut();

        // process_req(inner_stream, &req_queue, &mut rsp_queue, &mut write_buf).map_err(Error::io)?;
        let write_buf = req_reader.read_chunk();
        let write_buf_len = write_buf.len();
        let write_cnt = nonblock_write(inner_stream, write_buf).map_err(Error::io)?;
        req_reader.commit_read(write_cnt);

        let read_cnt = nonblock_read(inner_stream, &mut read_buf).map_err(Error::io)?;
        if read_cnt > 0 {
            decode_messages(&mut read_buf, &rsp_queue, &mut msg_queue, &mut parameters)?;
        }

        if read_cnt == 0 && (write_buf_len == 0 || write_cnt == 0) {
            stream.wait_io();
        }
    }
}

impl Connection {
    pub(crate) fn new(mut stream: TcpStream, parameters: HashMap<String, String>) -> Connection {
        use std::os::fd::AsRawFd;
        let id = stream.as_raw_fd() as usize;
        let waker = stream.waker();
        let (req_writer, req_reader) = cueue::cueue(IO_BUF_SIZE).expect("cueue create failed");
        let req_writer = Arc::new(Mutex::new(req_writer));

        let rsp_queue = Arc::new(Queue::new());
        let rsp_queue_dup = rsp_queue.clone();
        let io_handle = go!(move || {
            if let Err(e) = connection_loop(&mut stream, rsp_queue_dup, req_reader, parameters) {
                log::error!("connection error = {:?}", e);
                terminate_connection(&mut stream);
            }
        });

        Connection {
            io_handle,
            rsp_queue,
            req_writer,
            waker,
            id,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        process_req(req, &self.rsp_queue, &self.req_writer);
        self.waker.wakeup();
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }
}
