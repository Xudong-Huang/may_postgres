use crate::codec::{BackendMessage, BackendMessages, Framed, FrontendMessage, PostgresCodec};
use crate::copy_in::CopyInReceiver;
use crate::Error;
use bytes::BytesMut;
use log::error;
use may::coroutine::JoinHandle;
use may::go;
use may::net::TcpStream;
use may::sync::{mpsc, Mutex};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{self, Write};
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
    rx_handle: JoinHandle<()>,
    tx_handle: JoinHandle<()>,
    req_tx: mpsc::Sender<Request>,
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
        let mut writer = stream
            .inner_mut()
            .try_clone()
            .expect("failed to clone stream for wirter");
        let rsp_queue = Arc::new(Mutex::new(VecDeque::with_capacity(512)));
        let (req_tx, req_rx) = mpsc::channel();
        let rx_handle = {
            let rsp_queue: Arc<Mutex<VecDeque<Response>>> = rsp_queue.clone();
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
                                messages,
                                request_complete,
                            } => {
                                let mut rsp_queue = rsp_queue.lock().unwrap();
                                let response = &rsp_queue[0];
                                response.tx.send(messages).ok();

                                if request_complete {
                                    rsp_queue.pop_front();
                                }
                            }
                        }
                    }
                    Ok(())
                };

                if let Err(e) = main() {
                    error!("receiver closed. err={}", e);
                    let mut request = vec![];
                    frontend::terminate(&mut request);
                    let (tx, _rx) = mpsc::channel();
                    let req = Request {
                        messages: RequestMessages::Single(FrontendMessage::Raw(request)),
                        sender: tx,
                    };
                    req_tx.send(req).ok();
                }
                stream.inner_mut().shutdown(std::net::Shutdown::Both).ok();
            })
        };

        let tx_handle = go!(move || {
            let mut main = || -> Result<(), io::Error> {
                let mut buf = BytesMut::with_capacity(1024);
                use std::sync::mpsc::TryRecvError;
                let mut request = req_rx.try_recv();
                loop {
                    match request {
                        Ok(req) => {
                            let mut rsp_queue = rsp_queue.lock().unwrap();
                            rsp_queue.push_back(Response { tx: req.sender });
                            drop(rsp_queue);
                            match req.messages {
                                RequestMessages::Single(msg) => {
                                    PostgresCodec.encode(msg, &mut buf)?;
                                }
                                RequestMessages::CopyIn(mut rcv) => {
                                    let mut copy_in_msg = rcv.try_recv();
                                    loop {
                                        match copy_in_msg {
                                            Ok(Some(msg)) => {
                                                PostgresCodec.encode(msg, &mut buf)?;
                                                copy_in_msg = rcv.try_recv();
                                            }
                                            Ok(None) => {
                                                let n = writer.write(&buf)?;
                                                buf.advance(n);

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
                            writer.write_all(&buf)?;
                            buf.clear();
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
            writer.shutdown(std::net::Shutdown::Both).ok();
        });

        Connection {
            rx_handle,
            tx_handle,
            req_tx,
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) -> io::Result<()> {
        self.req_tx
            .send(req)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "send req failed"))
    }
}
