use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{SplitIo, SplitReader, SplitWriter};
use may::net::TcpStream;
use may::queue::spsc::Queue as SpscQueue;
use may::sync::spsc;
use postgres_protocol::message::backend::Message;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::queued_writer::QueuedWriter;
use crate::Error;

use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::Arc;

const IO_BUF_SIZE: usize = 4096 * 16;

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
    pub tag: usize,
    pub tx: spsc::Sender<BackendMessages>,
}

/// A connection to a PostgreSQL database.
pub(crate) struct Connection {
    read_handle: JoinHandle<()>,
    writer: QueuedWriter<SplitWriter<TcpStream>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rx = self.read_handle.coroutine();
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
        read_buf.reserve(IO_BUF_SIZE - remaining);
    }

    let buf = unsafe { &mut *(read_buf.bytes_mut() as *mut _ as *mut [u8]) };
    let n = stream.read(buf)?;
    unsafe { read_buf.advance_mut(n) };
    Ok(n)
}

#[inline]
fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &SpscQueue<Response>,
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

// #[inline]
// fn nonblock_write(stream: &mut impl Write, write_buf: &mut BytesMut) -> io::Result<usize> {
//     let len = write_buf.len();
//     let mut written = 0;
//     while written < len {
//         match stream.write(&write_buf[written..]) {
//             Ok(n) if n > 0 => written += n,
//             Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
//             Err(err) => return Err(err),
//             Ok(_) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
//         }
//     }
//     write_buf.advance(written);
//     Ok(written)
// }

// #[inline]
// fn process_write(
//     stream: &mut impl Write,
//     req_queue: &Queue<Request>,
//     rsp_queue: &SpscQueue<Response>,
//     write_buf: &mut BytesMut,
// ) -> io::Result<()> {
//     let remaining = write_buf.capacity();
//     if remaining < 512 {
//         write_buf.reserve(IO_BUF_SIZE - remaining);
//     }
//     while let Some(req) = req_queue.pop() {
//         rsp_queue.push(Response {
//             tag: req.tag,
//             tx: req.sender,
//         });
//         match req.messages {
//             RequestMessages::Single(msg) => match msg {
//                 FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
//                 FrontendMessage::CopyData(data) => data.write(write_buf),
//             },
//             RequestMessages::CopyIn(mut rcv) => {
//                 let mut copy_in_msg = rcv.try_recv();
//                 loop {
//                     match copy_in_msg {
//                         Ok(Some(msg)) => {
//                             match msg {
//                                 FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
//                                 FrontendMessage::CopyData(data) => data.write(write_buf),
//                             }
//                             copy_in_msg = rcv.try_recv();
//                         }
//                         Ok(None) => {
//                             nonblock_write(stream, write_buf)?;

//                             // no data found we just write all the data and wait
//                             copy_in_msg = rcv.recv();
//                         }
//                         Err(_) => break,
//                     }
//                 }
//             }
//         }
//     }
//     nonblock_write(stream, write_buf)?;
//     Ok(())
// }

// #[inline]
// fn terminate_connection(stream: &mut SplitWriter<TcpStream>) {
//     let mut request = BytesMut::new();
//     frontend::terminate(&mut request);
//     stream.write_all(&request.freeze()).ok();
//     stream.inner().shutdown(std::net::Shutdown::Both).ok();
// }

// #[inline]
// fn write_loop(
//     writer: &mut SplitWriter<TcpStream>,
//     req_queue: Arc<Queue<Request>>,
//     rsp_queue: Arc<SpscQueue<Response>>,
// ) -> Result<(), Error> {
//     let mut write_buf = BytesMut::with_capacity(IO_BUF_SIZE);
//     loop {
//         let inner_stream = writer.inner_mut().inner_mut();
//         process_write(inner_stream, &req_queue, &rsp_queue, &mut write_buf).map_err(Error::io)?;
//     }
// }

#[inline]
fn read_loop(
    mut reader: SplitReader<TcpStream>,
    rsp_queue: Arc<SpscQueue<Response>>,
    mut parameters: HashMap<String, String>,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(IO_BUF_SIZE);

    while process_read(&mut reader, &mut read_buf).map_err(Error::io)? > 0 {
        decode_messages(&mut read_buf, &rsp_queue, &mut parameters)?;
    }

    Ok(())
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, parameters: HashMap<String, String>) -> Connection {
        let (reader, writer) = stream.split().unwrap();

        let rsp_queue = Arc::new(SpscQueue::new());
        let rsp_queue_dup = rsp_queue.clone();

        let read_handle = go!(move || {
            if let Err(e) = read_loop(reader, rsp_queue_dup, parameters) {
                log::error!("connection read loop error = {:?}", e);
            }
        });

        Connection {
            read_handle,
            writer: QueuedWriter::new(writer, rsp_queue),
        }
    }

    /// send a request to the connection
    pub fn send(&self, req: Request) {
        self.writer.write(req).unwrap();
    }

    // /// send a request to the connection without flush
    // pub fn raw_send(&self, req: Request) {
    //     self.req_queue.push(req);
    // }

    // pub fn flush(&self) {
    //     unimplemented!()
    // }
}
