use crate::tls::{tls_read, tls_write, PgStream};
use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use may::coroutine::JoinHandle;
use may::go;
use may::io::{WaitIo, WaitIoWaker};
use may::queue::mpsc::Queue;
use may::sync::{spsc, Semphore};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage};
use crate::copy_in::CopyInReceiver;
use crate::{Error, Notification};

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const IO_BUF_SIZE: usize = 4096 * 2;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

/// What the connection coroutine sends back to a waiting `Responses::next`.
///
/// `Err` is sent exactly once per pending request when the I/O loop exits, so
/// a caller blocked on `rx.recv()` wakes with a connection-closed error
/// instead of waiting forever on a channel whose only producer is gone.
pub type ResponseResult = Result<BackendMessages, Error>;

pub struct Request {
    tag: usize,
    messages: RequestMessages,
    // OWNED (shared) sender, never a transmuted borrow: requests can sit in
    // the queues after the originating Client is dropped, so a borrow here
    // would dangle into freed Client memory the moment drop stopped being
    // an instant kill.
    sender: Arc<spsc::Sender<ResponseResult>>,
}

impl Request {
    #[inline]
    pub fn new(
        tag: usize,
        messages: RequestMessages,
        sender: Arc<spsc::Sender<ResponseResult>>,
    ) -> Request {
        Request {
            tag,
            messages,
            sender,
        }
    }
}

pub struct Response {
    tag: usize,
    tx: Arc<spsc::Sender<ResponseResult>>,
}

/// Asynchronous notifications delivered by the server: a queue the connection
/// coroutine fills and the client drains, plus a count of pushes a consumer can
/// park on instead of polling the queue.
pub(crate) struct Notifications {
    queue: Queue<Notification>,
    ready: Semphore,
}

impl Notifications {
    fn new() -> Self {
        Notifications {
            queue: Queue::new(),
            ready: Semphore::new(0),
        }
    }

    fn push(&self, notification: Notification) {
        self.queue.push(notification);
        self.ready.post();
    }

    /// Wake a consumer without a notification (the connection closed).
    fn wake(&self) {
        self.ready.post();
    }

    pub(crate) fn queue(&self) -> &Queue<Notification> {
        &self.queue
    }

    pub(crate) fn ready(&self) -> &Semphore {
        &self.ready
    }
}

/// A connection to a PostgreSQL database.
pub(crate) struct Connection {
    /// Held only to keep the io coroutine owned; dropping it DETACHES. The
    /// coroutine is never cancelled (cancel skips destructors, leaking the
    /// socket) and never joined (drop can run inside a coroutine that is
    /// itself being cancelled, where parking to join aborts the process).
    _io_handle: JoinHandle<()>,
    req_queue: Arc<Queue<Request>>,
    /// Asynchronous notifications delivered by the server, filled by the
    /// connection coroutine and drained by the client.
    notifications: Arc<Notifications>,
    waker: Arc<WaitIoWaker>,
    /// Cooperative shutdown: `drop` sets this and wakes the coroutine; the
    /// connection loop polls it and exits on its own stack, closing its own
    /// socket. Reaching in from outside (cancelling, or shutting the fd down
    /// from `drop`) corrupts the coroutine mid-flight.
    shutdown: Arc<AtomicBool>,
    /// Set (only) by the I/O coroutine when its loop has exited, on error, EOF
    /// or cooperative shutdown. Once set, the connection is permanently dead:
    /// nothing will ever drain `req_queue` or complete a response again, so
    /// `send` fails new requests immediately instead of queueing them forever.
    dead: Arc<AtomicBool>,
    /// Serializes draining `req_queue` on the death path. The queue is MPSC —
    /// while the loop runs it is the only consumer; after `dead` is set both
    /// the exiting coroutine and any sender that lost the race may need to
    /// drain, and this lock keeps "single consumer" true.
    drain_lock: Arc<spin::Mutex<()>>,
    id: usize,
}

/// Pop every queued request and fail its response channel with
/// [`Error::closed`]. Callers must hold `drain_lock` and only call this after
/// `dead` is set (the I/O loop must no longer be consuming).
fn fail_queued_requests(req_queue: &Queue<Request>) {
    while let Some(req) = req_queue.pop() {
        req.sender.send(Err(Error::closed())).ok();
    }
}

/// Fail every response the server still owed us. Runs once, on the I/O
/// coroutine's own exit path.
fn fail_pending_responses(rsp_queue: &mut VecDeque<Response>) {
    for rsp in rsp_queue.drain(..) {
        rsp.tx.send(Err(Error::closed())).ok();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Signal the loop and wake it out of wait_io(); it exits on its own
        // stack, sends the protocol Terminate and closes its own socket. This
        // must NOT block: a Client can be dropped inside a coroutine that is
        // itself being cancelled, and parking there (to join) aborts. The
        // waker holds an Arc to the reactor slot, so waking after the loop
        // has already exited is a harmless no-op.
        self.shutdown.store(true, Ordering::Release);
        self.waker.wakeup();
    }
}

#[inline]
pub(crate) fn reserve_buf(buf: &mut BytesMut) {
    let rem = buf.capacity() - buf.len();
    if rem < 512 {
        buf.reserve(IO_BUF_SIZE - rem);
    }
}

#[inline]
#[cold]
fn err<T>(e: io::Error) -> io::Result<T> {
    Err(e)
}

#[inline]
fn nonblock_write(stream: &mut impl Write, write_buf: &mut BytesMut) -> io::Result<usize> {
    let buf = write_buf.chunk();
    let len = buf.len();
    let mut write_cnt = 0;
    while write_cnt < len {
        match stream.write(unsafe { buf.get_unchecked(write_cnt..) }) {
            Ok(0) => return err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
            Ok(n) => write_cnt += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return err(e),
        }
    }
    write_buf.advance(write_cnt);
    Ok(write_cnt)
}

#[inline]
fn nonblock_read(stream: &mut impl Read, read_buf: &mut BytesMut) -> io::Result<bool> {
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
    Ok(read_cnt < len)
}

#[inline]
fn decode_messages(
    read_buf: &mut BytesMut,
    rsp_queue: &mut VecDeque<Response>,
    parameters: &mut HashMap<String, String>,
    notifications: &Notifications,
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
                response.tx.send(Ok(messages)).ok();

                if request_complete {
                    rsp_queue.pop_front();
                }
            }
            BackendMessage::Async(Message::NoticeResponse(_body)) => {}
            BackendMessage::Async(Message::NotificationResponse(body)) => {
                notifications.push(Notification {
                    process_id: body.process_id(),
                    channel: body.channel().map_err(Error::parse)?.to_string(),
                    payload: body.message().map_err(Error::parse)?.to_string(),
                });
            }
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

/// Move whatever copy-in data is currently available into the write buffer.
///
/// Returns without waiting when the channel is empty, leaving the receiver in
/// `pending` so the caller retries on the next pass. Clearing `pending` signals
/// the copy is over — either finished (`Done`, which the receiver turns into
/// `CopyDone`/`Sync`) or abandoned, when the sender was dropped and the receiver
/// produces `CopyFail`/`Sync`.
#[inline]
fn drain_copy_in(pending: &mut Option<CopyInReceiver>, write_buf: &mut BytesMut) {
    let Some(rcv) = pending.as_mut() else {
        return;
    };

    loop {
        match rcv.try_recv() {
            Ok(Some(msg)) => {
                reserve_buf(write_buf);
                match msg {
                    FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
                    FrontendMessage::CopyData(data) => data.write(write_buf),
                }
            }
            // Nothing queued yet; the client is still writing. Yield so the loop
            // can service reads, and pick this up again next pass.
            Ok(None) => return,
            // The copy has ended, in either direction.
            Err(()) => {
                *pending = None;
                return;
            }
        }
    }
}

#[inline]
fn process_req(
    req_queue: &Queue<Request>,
    rsp_queue: &mut VecDeque<Response>,
    write_buf: &mut BytesMut,
    pending_copy_in: &mut Option<CopyInReceiver>,
) -> io::Result<()> {
    // While a copy is in flight the connection is in COPY mode: the server will
    // only accept CopyData/CopyDone/CopyFail until it ends. Writing any other
    // queued request now would inject it into the copy stream and the server
    // would reject the whole thing with "unexpected message type during COPY".
    //
    // The previous blocking `rcv.recv()` gave this ordering for free, by never
    // leaving the copy arm. Draining without blocking means the guard has to be
    // explicit — requests simply wait their turn.
    if pending_copy_in.is_some() {
        return Ok(());
    }

    while let Some(req) = req_queue.pop() {
        reserve_buf(write_buf);
        rsp_queue.push_back(Response {
            tag: req.tag,
            tx: req.sender,
        });
        match req.messages {
            RequestMessages::Single(msg) => match msg {
                FrontendMessage::Raw(buf) => write_buf.extend_from_slice(&buf),
                FrontendMessage::CopyData(data) => data.write(write_buf),
            },
            RequestMessages::CopyIn(rcv) => {
                // Take ownership and drain without blocking. Blocking here — as
                // this used to, via `rcv.recv()` — parks the only coroutine that
                // reads the socket, while the client waits for a response that
                // only this coroutine can decode. That is a deadlock between the
                // two halves of one connection, and it strikes whenever the copy
                // channel drains before the client has queued more data, which
                // on an otherwise idle connection is immediately.
                *pending_copy_in = Some(rcv);
                drain_copy_in(pending_copy_in, write_buf);
            }
        }
    }
    Ok(())
}

#[cold]
fn terminate_connection(stream: &mut PgStream) {
    let mut request = BytesMut::new();
    frontend::terminate(&mut request);
    stream.write_all(&request.freeze()).ok();
    if let Some(conn) = stream.tls.as_deref_mut() {
        conn.send_close_notify();
    }
    stream.flush().ok();
    stream.tcp.shutdown(std::net::Shutdown::Both).ok();
}

#[inline]
fn connection_loop(
    stream: &mut PgStream,
    req_queue: &Queue<Request>,
    rsp_queue: &mut VecDeque<Response>,
    mut params: HashMap<String, String>,
    notifications: Arc<Notifications>,
    shutdown: &AtomicBool,
) -> Result<(), Error> {
    let mut read_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let mut write_buf = BytesMut::with_capacity(IO_BUF_SIZE);
    let mut pending_copy_in: Option<CopyInReceiver> = None;

    let mut io_events = 1; // allow read
    loop {
        // Cooperative shutdown, set by Connection::drop. Exiting HERE -
        // rather than being cancelled from outside - is what guarantees the
        // stream is dropped on this stack with destructors run.
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let inner_stream = stream.tcp.inner_mut();

        process_req(req_queue, rsp_queue, &mut write_buf, &mut pending_copy_in)
            .map_err(Error::io)?;
        // An in-flight copy may have had more data queued since the last pass.
        drain_copy_in(&mut pending_copy_in, &mut write_buf);
        match stream.tls.as_deref_mut() {
            None => nonblock_write(inner_stream, &mut write_buf).map(|_| ()),
            Some(conn) => tls_write(conn, inner_stream, &mut write_buf),
        }
        .map_err(Error::io)?;

        let mut read_blocked = true;
        if io_events & 1 != 0 {
            read_blocked = match stream.tls.as_deref_mut() {
                None => nonblock_read(inner_stream, &mut read_buf),
                Some(conn) => tls_read(conn, inner_stream, &mut read_buf),
            }
            .map_err(Error::io)?;
            decode_messages(&mut read_buf, rsp_queue, &mut params, &notifications)?;
        }

        io_events = if read_blocked {
            stream.tcp.wait_io()
        } else {
            1
        }
    }
}

impl Connection {
    pub(crate) fn new(mut stream: PgStream, parameters: HashMap<String, String>) -> Connection {
        use std::os::fd::AsRawFd;
        let id = stream.tcp.as_raw_fd() as usize;
        let waker = Arc::new(stream.tcp.waker());

        let req_queue = Arc::new(Queue::new());
        let req_queue_dup = req_queue.clone();
        let notifications = Arc::new(Notifications::new());
        let notifications_dup = notifications.clone();
        let notifications_exit = notifications.clone();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_dup = shutdown.clone();
        let dead = Arc::new(AtomicBool::new(false));
        let dead_dup = dead.clone();
        let drain_lock = Arc::new(spin::Mutex::new(()));
        let drain_lock_dup = drain_lock.clone();
        // EXPLICIT stack for the I/O coroutine. Bare go! inherits the
        // embedder ambient coroutine stack config, and inside BRRTRouter
        // that proved fatal: a connection spawned at RUNTIME (lifeguard
        // max-lifetime rotation, 2026-08-30 market wedge) overflowed its
        // stack and may killed it WITHOUT unwinding - so the exit path
        // below (mark dead, fail pending) never ran and every request
        // into that slot hung forever instead of failing fast. Same class
        // as the may-redis overflow recorded in PriceWhisperer hot_tier.rs.
        // 1 MiB: 256 KiB still overflowed once in ~8 rotation cycles under the
        // compressed-lifetime repro, and stack here is cheap at pool scale.
        let io_stack: usize = std::env::var("MAY_POSTGRES_IO_STACK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0x100000);
        let io_handle = go!(
            may::coroutine::Builder::new()
                .name("pg-io".to_owned())
                .stack_size(io_stack),
            move || {
                let mut rsp_queue: VecDeque<Response> = VecDeque::with_capacity(512);
                if let Err(e) = connection_loop(
                    &mut stream,
                    &req_queue_dup,
                    &mut rsp_queue,
                    parameters,
                    notifications_dup,
                    &shutdown_dup,
                ) {
                    log::error!("connection error = {:?}", e);
                }
                // On EVERY exit (error, EOF, cooperative shutdown): the loop will
                // never write another request or decode another response, so fail
                // everything that is still waiting. Without this, a caller blocked
                // in `Responses::next` waits forever — its channel's only producer
                // is this coroutine — and that wedge is what starved lifeguard's
                // pool workers (2026-08-29 tickers outage).
                //
                // Order matters: mark dead FIRST, so a sender that enqueues after
                // our drain sees `dead` on its post-push check and drains its own
                // request; then drain under the lock. SeqCst pairs with the SeqCst
                // load in `send` (Dekker pattern: push / load-dead on one side,
                // store-dead / drain on the other) so no push can slip between the
                // final drain and a stale `dead == false` read.
                dead_dup.store(true, Ordering::SeqCst);
                fail_pending_responses(&mut rsp_queue);
                {
                    let _g = drain_lock_dup.lock();
                    fail_queued_requests(&req_queue_dup);
                }
                // A consumer parked waiting for a notification would otherwise
                // wait out its timeout on a connection that will never deliver.
                notifications_exit.wake();
                // Send the protocol Terminate and close the socket, on this
                // coroutine's own stack. This is what releases the server backend
                // the moment the client goes away, instead of at process exit.
                terminate_connection(&mut stream);
            }
        );

        Connection {
            // Builder spawn is fallible (stack allocation); a connection we
            // cannot give a safe stack is a connection we must not hand out.
            _io_handle: io_handle.expect("spawn pg-io coroutine"),
            req_queue,
            notifications,
            waker,
            shutdown,
            dead,
            drain_lock,
            id,
        }
    }

    /// send a request to the connection
    ///
    /// If the I/O loop has already exited, the request's response channel is
    /// completed with [`Error::closed`] immediately (fail-fast) instead of
    /// leaving the request on a queue no coroutine will ever drain.
    #[inline]
    pub fn send(&self, req: Request) {
        self.req_queue.push(req);
        self.waker.wakeup();
        // Post-push check: either the loop was alive after our push (it will
        // consume the request normally, or fail it on its own exit drain), or
        // it was already dead — then nothing consumes the queue anymore and
        // WE drain it. Both racers may drain; the lock keeps it single-file.
        // SeqCst pairs with the SeqCst store on the exit path (see above).
        if self.dead.load(Ordering::SeqCst) {
            let _g = self.drain_lock.lock();
            fail_queued_requests(&self.req_queue);
        }
    }

    /// `true` once the connection I/O loop has exited; the connection can
    /// never carry another request and every call on it will fail fast.
    #[inline]
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Asynchronous notifications received from the server.
    ///
    /// Populated by `LISTEN`; see [`crate::Client::notifications`].
    #[inline]
    pub fn notifications(&self) -> &Notifications {
        &self.notifications
    }

    /// Waker for the connection coroutine.
    ///
    /// `send` wakes the loop when a request is queued. A copy-in streams its
    /// data through a separate channel, so it must do the same, or the loop
    /// stays parked in `wait_io` with data waiting to be written.
    #[inline]
    pub fn waker(&self) -> Arc<WaitIoWaker> {
        self.waker.clone()
    }
}
