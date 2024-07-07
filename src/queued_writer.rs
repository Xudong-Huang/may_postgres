use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use may::queue::mpsc::Queue;
use may::queue::mpsc::Queue as SpscQueue;
use may::sync::Mutex;

use crate::codec::FrontendMessage;
use crate::connection::{Request, RequestMessages, Response};

struct BufWriter<W: Write> {
    writer: W,
    buf: BytesMut,
    rsp_queue: Arc<SpscQueue<Response>>,
}

impl<W: Write> BufWriter<W> {
    fn new(writer: W, rsp_queue: Arc<SpscQueue<Response>>) -> Self {
        BufWriter {
            writer,
            rsp_queue,
            buf: BytesMut::with_capacity(1024 * 32),
        }
    }

    #[inline]
    fn put_req(&mut self, req: Request) -> io::Result<()> {
        self.rsp_queue.push(Response {
            tag: req.tag,
            tx: req.sender,
        });
        match req.messages {
            RequestMessages::Single(msg) => match msg {
                FrontendMessage::Raw(buf) => self.buf.extend_from_slice(&buf),
                FrontendMessage::CopyData(data) => data.write(&mut self.buf),
            },
            RequestMessages::CopyIn(mut rcv) => {
                let mut copy_in_msg = rcv.try_recv();
                loop {
                    match copy_in_msg {
                        Ok(Some(msg)) => {
                            match msg {
                                FrontendMessage::Raw(buf) => self.buf.extend_from_slice(&buf),
                                FrontendMessage::CopyData(data) => data.write(&mut self.buf),
                            }
                            copy_in_msg = rcv.try_recv();
                        }
                        Ok(None) => {
                            self.write_all()?;

                            // no data found we just write all the data and wait
                            copy_in_msg = rcv.recv();
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn write_all(&mut self) -> std::io::Result<()> {
        let ret = self.writer.write_all(&self.buf);
        self.buf.clear();
        let capacity = self.buf.capacity();
        self.buf.reserve(capacity);
        ret
    }
}

pub struct QueuedWriter<W: Write> {
    req_count: AtomicUsize,
    req_queue: Queue<Request>,
    writer: Mutex<BufWriter<W>>,
}

impl<W: Write> QueuedWriter<W> {
    pub fn new(writer: W, rsp_queue: Arc<SpscQueue<Response>>) -> Self {
        QueuedWriter {
            req_count: AtomicUsize::new(0),
            req_queue: Queue::new(),
            writer: Mutex::new(BufWriter::new(writer, rsp_queue)),
        }
    }

    /// it's safe and efficient to call this API concurrently
    pub fn write(&self, req: Request) -> io::Result<()> {
        self.req_queue.push(req);
        // only allow the first writer perform the write operation
        // other concurrent writers would just push the data
        if self.req_count.fetch_add(1, Ordering::AcqRel) == 0 {
            // in any cases this should not block since we have only one writer
            let mut writer = self.writer.lock().unwrap();

            loop {
                let mut cnt = 0;
                while let Some(req) = self.req_queue.pop() {
                    writer.put_req(req)?;
                    cnt += 1;
                }

                // detect if there are more packet need to deal with
                if self.req_count.fetch_sub(cnt, Ordering::AcqRel) == cnt {
                    break;
                }
            }

            writer.write_all()?;
        }
        Ok(())
    }
}
