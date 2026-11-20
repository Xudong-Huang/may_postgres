//! A scripted fake server, for the protocol paths a real PostgreSQL never takes: authentication
//! methods the client does not support, messages out of order, errors at every stage of the
//! startup, a server that hangs up.
//!
//! Each test gives the server a script: messages to send, and frontend messages to wait for
//! before sending more.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use may_postgres::{Client, Config, Error, SimpleQueryMessage};

enum Step {
    /// Write these bytes.
    Send(Vec<u8>),
    /// Read frontend messages up to and including one of this type.
    Expect(u8),
    /// Read the SCRAM client-first message and answer with a server-first that continues it.
    ScramServerFirst,
    /// Hang up.
    Close,
}

use Step::*;

struct Fake {
    port: u16,
    server: Option<JoinHandle<()>>,
    done: Arc<AtomicBool>,
}

impl Fake {
    fn start(script: Vec<Step>) -> Fake {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let done = Arc::new(AtomicBool::new(false));
        let server_done = done.clone();
        let server = std::thread::spawn(move || {
            let done = server_done;
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            if !read_startup(&mut socket) {
                return;
            }
            for step in script {
                let ok = match step {
                    Send(bytes) => socket.write_all(&bytes).is_ok(),
                    Expect(kind) => expect(&mut socket, kind),
                    ScramServerFirst => scram_server_first(&mut socket),
                    Close => return,
                };
                if !ok {
                    return;
                }
            }
            // Hold the connection until the client goes away or the test is done with it.
            socket
                .set_read_timeout(Some(Duration::from_millis(20)))
                .unwrap();
            let mut sink = [0u8; 1024];
            while !done.load(Ordering::Acquire) {
                match socket.read(&mut sink) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => break,
                }
            }
        });
        Fake {
            port,
            server: Some(server),
            done,
        }
    }

    fn connect_raw(&self, conninfo: &str) -> Result<Client, Error> {
        let socket = may::net::TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        conninfo.parse::<Config>().unwrap().connect_raw(socket)
    }

    fn finish(mut self) {
        self.done.store(true, Ordering::Release);
        self.server.take().unwrap().join().unwrap();
    }
}

/// Reads the startup message, answering SSLRequests with 'N'. False when the client hung up.
fn read_startup(socket: &mut TcpStream) -> bool {
    loop {
        let mut len = [0u8; 4];
        if socket.read_exact(&mut len).is_err() {
            return false;
        }
        let mut body = vec![0u8; i32::from_be_bytes(len) as usize - 4];
        if socket.read_exact(&mut body).is_err() {
            return false;
        }
        if body[..4] == 80877103i32.to_be_bytes() {
            socket.write_all(b"N").unwrap();
            continue;
        }
        return true;
    }
}

fn read_message(socket: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    socket.read_exact(&mut header).ok()?;
    let len = i32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len - 4];
    socket.read_exact(&mut body).ok()?;
    Some((header[0], body))
}

/// Reads up to and including a message of type `kind`. Dropped statements (Close, then Sync)
/// are answered on the way, as a server would, and their Sync does not count.
fn expect(socket: &mut TcpStream, kind: u8) -> bool {
    let mut closing = false;
    while let Some((k, _)) = read_message(socket) {
        match k {
            b'C' => closing = true,
            b'S' if closing => {
                closing = false;
                if socket
                    .write_all(&cat(&[message(b'3', &[]), ready()]))
                    .is_err()
                {
                    return false;
                }
            }
            k if k == kind => return true,
            _ => {}
        }
    }
    false
}

fn scram_server_first(socket: &mut TcpStream) -> bool {
    let Some((b'p', body)) = read_message(socket) else {
        return false;
    };
    // mechanism\0, i32 length, then "n,,n=,r=<nonce>"
    let text = String::from_utf8_lossy(&body);
    let nonce = text.split("r=").nth(1).unwrap().to_string();
    let server_first = format!("r={nonce}fake,s=c2FsdHNhbHQ=,i=4096");
    socket.write_all(&auth(11, server_first.as_bytes())).is_ok()
}

fn message(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    out.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth(code: i32, rest: &[u8]) -> Vec<u8> {
    let mut body = code.to_be_bytes().to_vec();
    body.extend_from_slice(rest);
    message(b'R', &body)
}

fn auth_ok() -> Vec<u8> {
    auth(0, &[])
}

fn key_data() -> Vec<u8> {
    let mut body = 4242i32.to_be_bytes().to_vec();
    body.extend_from_slice(&7i32.to_be_bytes());
    message(b'K', &body)
}

fn ready() -> Vec<u8> {
    message(b'Z', b"I")
}

fn error(code: &str) -> Vec<u8> {
    message(b'E', format!("SERROR\0C{code}\0Mfake error\0\0").as_bytes())
}

fn notice() -> Vec<u8> {
    message(b'N', b"SNOTICE\0C00000\0Mfake notice\0\0")
}

fn parameter(name: &str, value: &str) -> Vec<u8> {
    message(b'S', format!("{name}\0{value}\0").as_bytes())
}

fn parse_complete() -> Vec<u8> {
    message(b'1', &[])
}

fn bind_complete() -> Vec<u8> {
    message(b'2', &[])
}

fn no_data() -> Vec<u8> {
    message(b'n', &[])
}

fn no_parameters() -> Vec<u8> {
    message(b't', &0i16.to_be_bytes())
}

fn text_column(name: &str) -> Vec<u8> {
    let mut body = 1i16.to_be_bytes().to_vec();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i32.to_be_bytes()); // table
    body.extend_from_slice(&0i16.to_be_bytes()); // column
    body.extend_from_slice(&25i32.to_be_bytes()); // text
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    message(b'T', &body)
}

fn data_row(value: &str) -> Vec<u8> {
    let mut body = 1i16.to_be_bytes().to_vec();
    body.extend_from_slice(&(value.len() as i32).to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    message(b'D', &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    message(b'C', format!("{tag}\0").as_bytes())
}

fn copy_response(kind: u8) -> Vec<u8> {
    let mut body = vec![0u8];
    body.extend_from_slice(&0i16.to_be_bytes());
    message(kind, &body)
}

fn cat(parts: &[Vec<u8>]) -> Vec<u8> {
    parts.concat()
}

/// A completed startup: authenticated, keys, ready.
fn handshake() -> Step {
    Send(cat(&[
        auth_ok(),
        key_data(),
        parameter("server_version", "17.0"),
        ready(),
    ]))
}

/// The answer to a prepare of a statement with no parameters and no result columns.
fn prepared() -> Vec<Step> {
    vec![
        Expect(b'S'),
        Send(cat(&[
            parse_complete(),
            no_parameters(),
            no_data(),
            ready(),
        ])),
    ]
}

fn startup_error(script: Vec<Step>, conninfo: &str) -> String {
    let fake = Fake::start(script);
    let error = fake
        .connect_raw(conninfo)
        .map(drop)
        .expect_err("the connection should fail");
    fake.finish();
    error.to_string()
}

#[test]
fn a_plain_handshake() {
    let fake = Fake::start(vec![handshake()]);
    let client = fake.connect_raw("user=u").unwrap();
    drop(client);
    fake.finish();
}

#[test]
fn unsupported_authentication_methods() {
    for code in [2, 6, 7, 9] {
        let e = startup_error(vec![Send(auth(code, &[]))], "user=u password=p");
        assert!(
            e.contains("unsupported authentication method"),
            "{code}: {e}"
        );
    }
}

#[test]
fn passwords_are_required_when_asked_for() {
    let e = startup_error(vec![Send(auth(3, &[]))], "user=u");
    assert!(e.contains("password missing"), "{e}");
    let e = startup_error(vec![Send(auth(5, b"salt"))], "user=u");
    assert!(e.contains("password missing"), "{e}");
    let e = startup_error(vec![Send(auth(10, b"SCRAM-SHA-256\0\0"))], "user=u");
    assert!(e.contains("password missing"), "{e}");
}

#[test]
fn errors_and_strays_before_authentication() {
    let e = startup_error(vec![Send(error("28000"))], "user=u");
    assert!(e.contains("fake error"), "{e}");
    let e = startup_error(vec![Send(ready())], "user=u");
    assert!(e.contains("unexpected message"), "{e}");
}

#[test]
fn errors_and_strays_after_a_password() {
    let script = |reply: Vec<u8>| vec![Send(auth(3, &[])), Expect(b'p'), Send(reply)];
    let e = startup_error(script(error("28P01")), "user=u password=p");
    assert!(e.contains("fake error"), "{e}");
    let e = startup_error(script(ready()), "user=u password=p");
    assert!(e.contains("unexpected message"), "{e}");
}

#[test]
fn sasl_without_a_supported_mechanism() {
    let e = startup_error(
        vec![Send(auth(10, b"SCRAM-SHA-1\0\0"))],
        "user=u password=p",
    );
    assert!(e.contains("unsupported SASL mechanism"), "{e}");
}

#[test]
fn sasl_plus_is_used_without_channel_binding() {
    // Offered SCRAM-SHA-256-PLUS alone, the client still picks SCRAM-SHA-256 (no binding).
    let e = startup_error(
        vec![
            Send(auth(10, b"SCRAM-SHA-256-PLUS\0\0")),
            Expect(b'p'),
            Send(error("28P01")),
        ],
        "user=u password=p",
    );
    assert!(e.contains("fake error"), "{e}");
}

#[test]
fn sasl_errors_and_strays_at_each_stage() {
    let first = |reply: Vec<u8>| {
        vec![
            Send(auth(10, b"SCRAM-SHA-256\0\0")),
            Expect(b'p'),
            Send(reply),
        ]
    };
    let e = startup_error(first(error("28P01")), "user=u password=p");
    assert!(e.contains("fake error"), "{e}");
    let e = startup_error(first(ready()), "user=u password=p");
    assert!(e.contains("unexpected message"), "{e}");
    // A server-first message that does not extend the client's nonce.
    let e = startup_error(
        first(auth(11, b"r=nope,s=c2FsdA==,i=4096")),
        "user=u password=p",
    );
    assert!(e.contains("authentication"), "{e}");

    let second = |reply: Vec<u8>| {
        vec![
            Send(auth(10, b"SCRAM-SHA-256\0\0")),
            ScramServerFirst,
            Expect(b'p'),
            Send(reply),
        ]
    };
    let e = startup_error(second(error("28P01")), "user=u password=p");
    assert!(e.contains("fake error"), "{e}");
    let e = startup_error(second(ready()), "user=u password=p");
    assert!(e.contains("unexpected message"), "{e}");
    // A server signature that does not verify.
    let e = startup_error(second(auth(12, b"v=AAAA")), "user=u password=p");
    assert!(e.contains("authentication"), "{e}");
}

#[test]
fn errors_and_strays_after_authentication() {
    let e = startup_error(
        vec![Send(cat(&[auth_ok(), notice(), error("57P03")]))],
        "user=u",
    );
    assert!(e.contains("fake error"), "{e}");
    let e = startup_error(vec![Send(cat(&[auth_ok(), no_data()]))], "user=u");
    assert!(e.contains("unexpected message"), "{e}");
}

#[test]
fn hang_up_during_startup() {
    let e = startup_error(vec![Close], "user=u");
    assert!(!e.is_empty());
}

fn connect(fake: &Fake, extra: &str) -> Result<Client, Error> {
    may_postgres::connect(&format!("host=127.0.0.1 port={} user=u {extra}", fake.port))
}

fn read_only_answer(rows: &[Vec<u8>]) -> Vec<Step> {
    let mut answer = vec![text_column("transaction_read_only")];
    answer.extend_from_slice(rows);
    answer.push(command_complete("SHOW"));
    answer.push(ready());
    vec![handshake(), Expect(b'Q'), Send(cat(&answer))]
}

#[test]
fn read_write_sessions_refuse_a_read_only_server() {
    let fake = Fake::start(read_only_answer(&[data_row("on")]));
    let e = connect(&fake, "target_session_attrs=read-write")
        .map(drop)
        .expect_err("a read-only server");
    assert!(e.to_string().contains("does not allow writes"), "{e}");
    fake.finish();

    let fake = Fake::start(read_only_answer(&[data_row("off")]));
    drop(connect(&fake, "target_session_attrs=read-write").unwrap());
    fake.finish();

    let fake = Fake::start(read_only_answer(&[]));
    let e = connect(&fake, "target_session_attrs=read-write")
        .map(drop)
        .expect_err("no answer");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    fake.finish();
}

#[test]
fn connect_tries_each_host_in_turn() {
    // Nothing listens on the first port; the second is the fake.
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_port = closed.local_addr().unwrap().port();
    drop(closed);
    let fake = Fake::start(vec![handshake()]);
    let client = may_postgres::connect(&format!(
        "host=127.0.0.1,127.0.0.1 port={closed_port},{} user=u",
        fake.port
    ))
    .unwrap();
    drop(client);
    fake.finish();

    let e = may_postgres::connect(&format!("host=127.0.0.1 port={closed_port} user=u"))
        .map(drop)
        .expect_err("refused");
    assert!(!e.to_string().is_empty());
}

#[test]
fn prepare_with_stray_messages() {
    let mut script = vec![handshake()];
    for answer in [
        vec![no_data(), ready()],
        vec![parse_complete(), no_data(), ready()],
        vec![parse_complete(), no_parameters(), parse_complete(), ready()],
    ] {
        script.push(Expect(b'S'));
        script.push(Send(cat(&answer)));
    }
    // and then a good one, on the same connection
    script.extend(prepared());
    let fake = Fake::start(script);
    let client = fake.connect_raw("user=u").unwrap();
    for _ in 0..3 {
        let e = client
            .prepare("SELECT")
            .map(drop)
            .expect_err("a stray message");
        assert!(e.to_string().contains("unexpected message"), "{e}");
    }
    client.prepare("SELECT").unwrap();
    drop(client);
    fake.finish();
}

#[test]
fn execute_and_query_with_stray_messages() {
    let mut script = vec![handshake()];
    script.extend(prepared());
    // execute: a stray message after BindComplete
    script.push(Expect(b'S'));
    script.push(Send(cat(&[bind_complete(), parse_complete(), ready()])));
    // query: the same, through the row stream
    script.push(Expect(b'S'));
    script.push(Send(cat(&[bind_complete(), parse_complete(), ready()])));
    // query: a server error
    script.push(Expect(b'S'));
    script.push(Send(cat(&[bind_complete(), error("XX000"), ready()])));
    let fake = Fake::start(script);
    let client = fake.connect_raw("user=u").unwrap();
    let statement = client.prepare("SELECT").unwrap();

    let e = client
        .execute(&statement, &[])
        .map(drop)
        .expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");

    let mut rows = client.query_raw(&statement, &[]).unwrap();
    let e = rows.next().unwrap().map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    drop(rows);

    let mut rows = client.query_raw(&statement, &[]).unwrap();
    let e = rows.next().unwrap().map(drop).expect_err("server error");
    assert!(e.to_string().contains("fake error"), "{e}");
    drop(rows);

    drop(client);
    fake.finish();
}

#[test]
fn simple_queries_with_stray_messages() {
    let mut script = vec![handshake()];
    for answer in [
        vec![parse_complete(), ready()],
        vec![data_row("x"), ready()],
        vec![parse_complete(), ready()],
    ] {
        script.push(Expect(b'Q'));
        script.push(Send(cat(&answer)));
    }
    let fake = Fake::start(script);
    let client = fake.connect_raw("user=u").unwrap();

    let e = client.batch_execute("x").map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    // A data row without a row description.
    let e = client.simple_query("x").map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    let e = client.simple_query("x").map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");

    drop(client);
    fake.finish();
}

#[test]
fn copies_with_stray_messages() {
    let mut script = vec![handshake()];
    let copy_answers = [
        vec![parse_complete(), ready()],
        vec![bind_complete(), parse_complete(), ready()],
    ];
    // copy out: before BindComplete, before CopyOutResponse, inside the data
    for answer in &copy_answers {
        script.extend(prepared());
        script.push(Expect(b'S'));
        script.push(Send(cat(answer)));
    }
    script.extend(prepared());
    script.push(Expect(b'S'));
    script.push(Send(cat(&[
        bind_complete(),
        copy_response(b'H'),
        parse_complete(),
        ready(),
    ])));
    // copy in: the same two, then a stray answer to the finished copy
    for answer in &copy_answers {
        script.extend(prepared());
        script.push(Expect(b'S'));
        script.push(Send(cat(answer)));
    }
    script.extend(prepared());
    script.push(Expect(b'S'));
    script.push(Send(cat(&[bind_complete(), copy_response(b'G')])));
    script.push(Expect(b'c'));
    script.push(Send(cat(&[parse_complete(), ready()])));

    let fake = Fake::start(script);
    let client = fake.connect_raw("user=u").unwrap();

    for _ in 0..2 {
        let e = client.copy_out("COPY").map(drop).expect_err("stray");
        assert!(e.to_string().contains("unexpected message"), "{e}");
    }
    let mut stream = client.copy_out("COPY").unwrap();
    let e = stream.next().unwrap().map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    drop(stream);

    for _ in 0..2 {
        let e = client
            .copy_in::<_, bytes::Bytes>("COPY")
            .map(drop)
            .expect_err("stray");
        assert!(e.to_string().contains("unexpected message"), "{e}");
    }
    let mut sink = client.copy_in::<_, bytes::Bytes>("COPY").unwrap();
    sink.send(bytes::Bytes::from_static(b"1\n")).unwrap();
    let e = sink.finish().map(drop).expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");

    drop(client);
    fake.finish();
}

#[test]
fn asynchronous_messages_between_queries() {
    let fake = Fake::start(vec![
        handshake(),
        Send(cat(&[notice(), parameter("TimeZone", "UTC")])),
        Expect(b'Q'),
        Send(cat(&[command_complete("SET"), ready()])),
    ]);
    let client = fake.connect_raw("user=u").unwrap();
    let messages = client.simple_query("x").unwrap();
    assert!(matches!(
        messages.as_slice(),
        [SimpleQueryMessage::CommandComplete(0)]
    ));
    drop(client);
    fake.finish();
}

#[test]
fn connect_needs_a_host() {
    let e = may_postgres::connect("user=u")
        .map(drop)
        .expect_err("no host");
    assert!(e.to_string().contains("host missing"), "{e}");
}

fn copy_data(bytes: &[u8]) -> Vec<u8> {
    message(b'd', bytes)
}

fn binary_header(flags: i32) -> Vec<u8> {
    let mut header = b"PGCOPY\n\xff\r\n\0".to_vec();
    header.extend_from_slice(&flags.to_be_bytes());
    header.extend_from_slice(&0i32.to_be_bytes());
    header
}

/// A binary copy out whose data is `chunks`, each sent as one CopyData.
fn binary_copy_out(chunks: &[Vec<u8>]) -> Fake {
    let mut answer = vec![bind_complete(), copy_response(b'H')];
    answer.extend(chunks.iter().map(|c| copy_data(c)));
    answer.push(message(b'c', &[]));
    answer.push(command_complete("COPY 0"));
    answer.push(ready());
    let mut script = vec![handshake()];
    script.extend(prepared());
    script.push(Expect(b'S'));
    script.push(Send(cat(&answer)));
    Fake::start(script)
}

fn first_binary_row(
    fake: &Fake,
    types: &[may_postgres::types::Type],
) -> Option<Result<may_postgres::binary_copy::BinaryCopyOutRow, Error>> {
    let client = fake.connect_raw("user=u").unwrap();
    let stream = client.copy_out("COPY").unwrap();
    may_postgres::binary_copy::BinaryCopyOutStream::new(stream, types).next()
}

#[test]
fn binary_copy_out_decoding() {
    use may_postgres::types::Type;

    // A wrong magic value.
    let fake = binary_copy_out(&[b"NOTCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0".to_vec()]);
    let e = first_binary_row(&fake, &[Type::INT4])
        .unwrap()
        .map(drop)
        .expect_err("bad magic");
    assert!(e.to_string().contains("invalid magic value"), "{e}");
    fake.finish();

    // The data ends without the trailer.
    let fake = binary_copy_out(&[]);
    assert!(first_binary_row(&fake, &[Type::INT4]).unwrap().is_err());
    fake.finish();

    // The server fails the copy after a row.
    let mut chunk = binary_header(0);
    chunk.extend_from_slice(&1i16.to_be_bytes());
    chunk.extend_from_slice(&4i32.to_be_bytes());
    chunk.extend_from_slice(&5i32.to_be_bytes());
    let mut script = vec![handshake()];
    script.extend(prepared());
    script.push(Expect(b'S'));
    script.push(Send(cat(&[
        bind_complete(),
        copy_response(b'H'),
        copy_data(&chunk),
        error("XX000"),
        ready(),
    ])));
    let fake = Fake::start(script);
    let client = fake.connect_raw("user=u").unwrap();
    let stream = client.copy_out("COPY").unwrap();
    let mut rows = may_postgres::binary_copy::BinaryCopyOutStream::new(stream, &[Type::INT4]);
    assert_eq!(rows.next().unwrap().unwrap().get::<i32>(0), 5);
    let e = rows
        .next()
        .unwrap()
        .map(drop)
        .expect_err("the server's error");
    assert!(e.to_string().contains("fake error"), "{e}");
    drop(rows);
    drop(client);
    fake.finish();

    // Rows with OIDs (servers before 12): the OID counts as a leading column.
    let mut chunk = binary_header(1 << 16);
    chunk.extend_from_slice(&1i16.to_be_bytes());
    for value in [16384i32, 7] {
        chunk.extend_from_slice(&4i32.to_be_bytes());
        chunk.extend_from_slice(&value.to_be_bytes());
    }
    let fake = binary_copy_out(&[chunk, (-1i16).to_be_bytes().to_vec()]);
    let row = first_binary_row(&fake, &[Type::OID, Type::INT4])
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<u32>(0), 16384);
    assert_eq!(row.get::<i32>(1), 7);
    fake.finish();
}

#[test]
fn bind_with_a_stray_message() {
    let mut script = vec![
        handshake(),
        Expect(b'Q'),
        Send(cat(&[command_complete("BEGIN"), ready()])),
    ];
    script.extend(prepared());
    script.push(Expect(b'S'));
    script.push(Send(cat(&[parse_complete(), ready()])));
    let fake = Fake::start(script);
    let mut client = fake.connect_raw("user=u").unwrap();
    let transaction = client.transaction().unwrap();
    let statement = transaction.prepare("SELECT").unwrap();
    let e = transaction
        .bind(&statement, &[])
        .map(drop)
        .expect_err("stray");
    assert!(e.to_string().contains("unexpected message"), "{e}");
    std::mem::forget(transaction); // its rollback would wait on the fake
    drop(client);
    fake.finish();
}
