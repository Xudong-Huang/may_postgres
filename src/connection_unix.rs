use crate::codec::{BackendMessage, BackendMessages, Framed, FrontendMessage};
use crate::copy_in::CopyInReceiver;
// use crate::vec_buf::VecBufs;
use crate::Error;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use log::error;
use may::coroutine::Coroutine;
use may::go;
use may::sync::mpsc;
use may_queue::{mpsc_list, spsc};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use std::collections::HashMap;
use std::io;
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
    req_queue: Arc<mpsc_list::Queue<Request>>,
    is_running: Arc<AtomicBool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
        unsafe { self.bg_co.cancel() };
    }
}

impl Connection {
    pub(crate) fn new(mut stream: Framed, mut parameters: HashMap<String, String>) -> Connection {
        let rsp_queue = Arc::new(spsc::Queue::<Response>::new());
        let req_queue = Arc::new(mpsc_list::Queue::new());
        let is_running = Arc::new(AtomicBool::new(true));

        let rsp_queue_c = rsp_queue.clone();
        let req_queue_c = req_queue.clone();
        let is_running_c = is_running.clone();

        let bg_handle = go!(move || {
            let mut main = || -> Result<(), Error> {
                const MAX_CACHE_SIZE: usize = 128;
                let mut message_cache = Vec::with_capacity(MAX_CACHE_SIZE);
                #[allow(clippy::while_let_on_iterator)]
                while let Some(rsp) = stream.next() {
                    match rsp.map_err(Error::io)? {
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

                            if request_complete {
                                for msg in message_cache.drain(..) {
                                    response.tx.send(msg).ok();
                                }
                                rsp_queue.pop();
                            }
                        }
                    }
                }
                Ok(())
            };

            if let Err(e) = main() {
                error!("back ground client closed. err={}", e);
                let mut request = BytesMut::new();
                frontend::terminate(&mut request);
                let (tx, _rx) = mpsc::channel();
                let req = Request {
                    messages: RequestMessages::Single(FrontendMessage::Raw(request.freeze())),
                    sender: tx,
                };
                req_tx.send(req).ok();
            }
            stream.inner_mut().shutdown(std::net::Shutdown::Both).ok();
        });

        // let tx_handle = go!(move || {
        //     let mut writer = VecBufs::new(writer);
        //     let mut main = || -> Result<(), io::Error> {
        //         use std::sync::mpsc::TryRecvError;
        //         let mut request = req_rx.try_recv();
        //         loop {
        //             match request {
        //                 Ok(req) => {
        //                     rsp_queue.push(Response { tx: req.sender });
        //                     match req.messages {
        //                         RequestMessages::Single(msg) => match msg {
        //                             FrontendMessage::Raw(buf) => writer.write_bytes(buf)?,
        //                             FrontendMessage::CopyData(data) => {
        //                                 let mut buf = BytesMut::new();
        //                                 data.write(&mut buf);
        //                                 writer.write_bytes(buf.freeze())?;
        //                             }
        //                         },
        //                         RequestMessages::CopyIn(mut rcv) => {
        //                             let mut copy_in_msg = rcv.try_recv();
        //                             loop {
        //                                 match copy_in_msg {
        //                                     Ok(Some(msg)) => {
        //                                         match msg {
        //                                             FrontendMessage::Raw(buf) => {
        //                                                 writer.write_bytes(buf)?
        //                                             }
        //                                             FrontendMessage::CopyData(data) => {
        //                                                 let mut buf = BytesMut::new();
        //                                                 data.write(&mut buf);
        //                                                 writer.write_bytes(buf.freeze())?;
        //                                             }
        //                                         }
        //                                         copy_in_msg = rcv.try_recv();
        //                                     }
        //                                     Ok(None) => {
        //                                         writer.flush()?;

        //                                         // no data found we just write all the data and wait
        //                                         copy_in_msg = rcv.recv();
        //                                     }
        //                                     Err(_) => break,
        //                                 }
        //                             }
        //                         }
        //                     }
        //                     request = req_rx.try_recv();
        //                 }
        //                 Err(TryRecvError::Empty) => {
        //                     may::coroutine::yield_now();
        //                     request = req_rx.try_recv();
        //                     match &request {
        //                         Err(TryRecvError::Empty) => {}
        //                         _ => continue,
        //                     }

        //                     writer.flush()?;

        //                     request = req_rx.recv().map_err(|_| TryRecvError::Empty);
        //                     may::coroutine::yield_now();
        //                 }
        //                 Err(_) => {
        //                     return Err(io::Error::new(
        //                         io::ErrorKind::BrokenPipe,
        //                         "request queue closed",
        //                     ));
        //                 }
        //             }
        //         }
        //     };

        //     if let Err(e) = main() {
        //         error!("writer closed. err={}", e);
        //     }
        //     writer.inner_mut().shutdown(std::net::Shutdown::Both).ok();
        // });

        Connection {
            bg_co: bg_handle.coroutine().clone(),
            req_queue,
            is_running,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) -> io::Result<()> {
        self.req_queue.push(req);
        // signal the back ground processing about the data
        unsafe { self.bg_co.cancel() };
        Ok(())
    }

    /// dummy impl
    pub fn read_lock(&self) -> AtomicBool {
        false.into()
    }
}
