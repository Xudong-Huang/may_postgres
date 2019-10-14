use crate::codec::{BackendMessage, BackendMessages, Framed, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::vec_buf::VecBufs;
use crate::Error;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use log::error;
use may::coroutine::JoinHandle;
use may::net::TcpStream;
use may::sync::{mpsc, RwLock, RwLockReadGuard};
use may::{coroutine_local, go};
use may_queue::spsc;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

coroutine_local! {
    static READER_LOCK: RefCell<Option<RwLockReadGuard<'static, ()>>> = RefCell::new(None)
}

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
    rx_handle: JoinHandle<()>,
    tx_handle: JoinHandle<()>,
    req_tx: mpsc::Sender<Request>,
    rw_lock: Arc<RwLock<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rx = self.rx_handle.coroutine();
        let tx = self.tx_handle.coroutine();
        unsafe {
            rx.cancel();
            tx.cancel();
        }
    }
}

impl Connection {
    pub(crate) fn new(
        mut stream: Framed<TcpStream>,
        mut parameters: HashMap<String, String>,
    ) -> Connection {
        let writer = stream
            .inner_mut()
            .try_clone()
            .expect("failed to clone stream for wirter");
        let rsp_queue = Arc::new(spsc::Queue::new());
        let (req_tx, req_rx) = mpsc::channel();
        let rx_handle = {
            let rsp_queue: Arc<spsc::Queue<Response>> = rsp_queue.clone();
            let req_tx = req_tx.clone();
            go!(move || {
                let mut main = || -> Result<(), Error> {
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

                                response.tx.send(messages).ok();

                                if request_complete {
                                    rsp_queue.pop();
                                }
                            }
                        }
                    }
                    Ok(())
                };

                if let Err(e) = main() {
                    error!("receiver closed. err={}", e);
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
            })
        };

        let rw_lock = Arc::new(RwLock::new(()));
        let rw_lock_1 = rw_lock.clone();

        let tx_handle = go!(move || {
            let mut writer = VecBufs::new(writer);
            let mut main = || -> Result<(), io::Error> {
                use std::sync::mpsc::TryRecvError;
                let mut request = req_rx.try_recv();
                loop {
                    match request {
                        Ok(req) => {
                            rsp_queue.push(Response { tx: req.sender });
                            match req.messages {
                                RequestMessages::Single(msg) => match msg {
                                    FrontendMessage::Raw(buf) => writer.write_bytes(buf)?,
                                    FrontendMessage::CopyData(data) => {
                                        let mut buf = BytesMut::new();
                                        data.write(&mut buf);
                                        writer.write_bytes(buf.freeze())?;
                                    }
                                },
                                RequestMessages::CopyIn(mut rcv) => {
                                    let mut copy_in_msg = rcv.try_recv();
                                    loop {
                                        match copy_in_msg {
                                            Ok(Some(msg)) => {
                                                match msg {
                                                    FrontendMessage::Raw(buf) => {
                                                        writer.write_bytes(buf)?
                                                    }
                                                    FrontendMessage::CopyData(data) => {
                                                        let mut buf = BytesMut::new();
                                                        data.write(&mut buf);
                                                        writer.write_bytes(buf.freeze())?;
                                                    }
                                                }
                                                copy_in_msg = rcv.try_recv();
                                            }
                                            Ok(None) => {
                                                writer.flush()?;

                                                // no data found we just write all the data and wait
                                                copy_in_msg = rcv.recv();
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                }
                            }
                            request = req_rx.try_recv();
                        }
                        Err(TryRecvError::Empty) => {
                            let _g = rw_lock_1.write().unwrap();
                            request = req_rx.try_recv();
                            match &request {
                                Err(TryRecvError::Empty) => {}
                                _ => continue,
                            }
                            drop(_g);

                            writer.flush()?;
                            request = req_rx.recv().map_err(|_| TryRecvError::Empty);
                        }
                        Err(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "request queue closed",
                            ));
                        }
                    }
                }
            };

            if let Err(e) = main() {
                error!("writer closed. err={}", e);
            }
            writer.inner_mut().shutdown(std::net::Shutdown::Both).ok();
        });

        Connection {
            rx_handle,
            tx_handle,
            req_tx,
            rw_lock,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) -> io::Result<()> {
        let ret = self
            .req_tx
            .send(req)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "send req failed"));
        READER_LOCK.with(|l| l.borrow_mut().take());
        ret
    }

    pub fn read_lock(&self) {
        let lock = unsafe { std::mem::transmute(self.rw_lock.read().unwrap()) };
        READER_LOCK.with(|l| l.borrow_mut().replace(lock));
    }
}
