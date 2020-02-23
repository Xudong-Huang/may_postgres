use crate::codec::{BackendMessage, BackendMessages, Framed, FrontendMessage};
use crate::copy_in::CopyInReceiver;
// use crate::vec_buf::VecBufs;
use crate::Error;
use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use log::error;
use may::coroutine::Coroutine;
use may::go;
use may::io::WaitIo;
use may::sync::mpsc;
use may_queue::{mpmc_bounded, spsc};
use postgres_protocol::message::backend::Message;

use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
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
pub(crate) struct Connection {
    bg_co: Coroutine,
    req_queue: Arc<mpmc_bounded::Queue<Request>>,
    is_running: Arc<AtomicBool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
        unsafe { self.bg_co.cancel() };
    }
}

impl Connection {
    pub(crate) fn new(stream: Framed, mut parameters: HashMap<String, String>) -> Connection {
        let rsp_queue = spsc::Queue::<Response>::new();
        let req_queue = Arc::new(mpmc_bounded::Queue::<Request>::with_capacity(10000));
        let is_running = Arc::new(AtomicBool::new(true));

        let req_queue_c = req_queue.clone();
        let is_running_c = is_running.clone();

        let (mut stream, mut read_buf, mut codec) = stream.into_parts();

        let bg_handle = go!(move || {
            let mut main = || -> Result<(), Error> {
                const MAX_CACHE_SIZE: usize = 128;
                let mut message_cache = Vec::with_capacity(MAX_CACHE_SIZE);
                stream.set_nonblocking(true).unwrap();
                stream.read(&mut []).ok();
                let mut write_buf = BytesMut::with_capacity(1024 * 32);
                loop {
                    // finish the running task
                    if !is_running_c.load(Ordering::Relaxed) {
                        #[cold]
                        return Ok(());
                    }

                    if write_buf.capacity() - write_buf.len() < 1024 * 1024 {
                        write_buf.reserve(1024 * 1024 * 32);
                    }

                    // collect all the data
                    while let Some(req) = req_queue_c.pop() {
                        rsp_queue.push(Response { tx: req.sender });
                        match req.messages {
                            RequestMessages::Single(msg) => match msg {
                                FrontendMessage::Raw(buf) => {
                                    write_buf.extend_from_slice(&buf);
                                }
                                FrontendMessage::CopyData(data) => {
                                    data.write(&mut write_buf);
                                }
                            },
                            // just use block io here for simplicity,
                            // performance is poor, the cancel logic is wrong
                            RequestMessages::CopyIn(mut rcv) => loop {
                                match rcv.recv() {
                                    Ok(Some(msg)) => match msg {
                                        FrontendMessage::Raw(buf) => {
                                            write_buf.extend_from_slice(&buf);
                                        }
                                        FrontendMessage::CopyData(data) => {
                                            data.write(&mut write_buf);
                                        }
                                    },
                                    Ok(None) => unreachable!(),
                                    Err(_) => break, // finish
                                }
                            },
                        }
                    }

                    // deal with the read part
                    loop {
                        // try to read more data from stream
                        // read the socket for reqs
                        if read_buf.capacity() - read_buf.len() < 1024 * 1024 {
                            read_buf.reserve(4096 * 8 * 1024);
                        }

                        let n = {
                            let buf =
                                unsafe { &mut *(read_buf.bytes_mut() as *mut _ as *mut [u8]) };
                            match stream.raw_read(buf) {
                                Ok(v) => v,
                                Err(e) => {
                                    if e.kind() == io::ErrorKind::WouldBlock {
                                        break;
                                    }
                                    return Err(Error::io(e));
                                }
                            }
                        };
                        //connection was closed
                        if n == 0 {
                            #[cold]
                            return Err(Error::closed());
                        }
                        unsafe { read_buf.advance_mut(n) };
                    }

                    // decode all the messages
                    while let Some(msg) = codec.decode(&mut read_buf).map_err(Error::io)? {
                        match msg {
                            BackendMessage::Async(Message::NoticeResponse(_body)) => {}
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
                                let response = match unsafe { rsp_queue.peek() } {
                                    Some(response) => response,
                                    None => match messages.next().map_err(Error::parse)? {
                                        Some(Message::ErrorResponse(error)) => {
                                            return Err(Error::db(error))
                                        }
                                        _ => return Err(Error::unexpected_message()),
                                    },
                                };

                                message_cache.push(messages);
                                if message_cache.len() >= MAX_CACHE_SIZE {
                                    for msg in message_cache.drain(..) {
                                        response.tx.send(msg).ok();
                                    }
                                }
                                // response.tx.send(messages).ok();

                                if request_complete {
                                    for msg in message_cache.drain(..) {
                                        response.tx.send(msg).ok();
                                    }
                                    rsp_queue.pop();
                                }
                            }
                        }
                    }

                    // send all the data
                    if !write_buf.is_empty() {
                        let len = write_buf.len();
                        let mut written = 0;
                        while written < len {
                            match stream.raw_write(&write_buf[written..]) {
                                Ok(n) => {
                                    if n == 0 {
                                        return Ok(());
                                    } else {
                                        written += n;
                                    }
                                }
                                Err(err) => {
                                    if err.kind() == io::ErrorKind::WouldBlock {
                                        break;
                                    } else {
                                        return Err(Error::io(err));
                                    }
                                }
                            }
                        }
                        if written == len {
                            unsafe { write_buf.set_len(0) }
                        } else if written > 0 {
                            write_buf.advance(written);
                        }
                    }

                    stream.wait_io();
                }
            };

            if let Err(e) = main() {
                error!("back ground client closed. err={}", e);
            }
            stream.shutdown(std::net::Shutdown::Both).ok();
        });

        Connection {
            bg_co: bg_handle.coroutine().clone(),
            req_queue,
            is_running,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) -> io::Result<()> {
        let mut r = req;
        while let Err(req) = self.req_queue.push(r) {
            r = req;
            may::coroutine::yield_now();
        }
        // signal the back ground processing about the data
        unsafe { self.bg_co.cancel() };
        Ok(())
    }

    /// dummy impl
    pub fn read_lock(&self) -> AtomicBool {
        false.into()
    }
}
