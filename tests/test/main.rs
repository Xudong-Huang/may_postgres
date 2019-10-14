use may::net::TcpStream;
use may::{go, join};
use may_postgres::error::SqlState;
use may_postgres::types::{Kind, Type};
use may_postgres::{Client, Config, Error, SimpleQueryMessage};
use std::fmt::Write;
use std::time::Duration;

mod parse;
mod runtime;
mod types;

fn connect_raw(s: &str) -> Result<Client, Error> {
    let socket = TcpStream::connect("127.0.0.1:5433").unwrap();
    let config = s.parse::<Config>().unwrap();
    config.connect_raw(socket)
}

fn connect(s: &str) -> Client {
    connect_raw(s).unwrap()
}

#[test]
fn plain_password_missing() {
    connect_raw("user=pass_user dbname=postgres").err().unwrap();
}

#[test]
fn plain_password_wrong() {
    match connect_raw("user=pass_user password=foo dbname=postgres") {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn plain_password_ok() {
    connect("user=pass_user password=password dbname=postgres");
}

#[test]
fn md5_password_missing() {
    connect_raw("user=md5_user dbname=postgres").err().unwrap();
}

#[test]
fn md5_password_wrong() {
    match connect_raw("user=md5_user password=foo dbname=postgres") {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn md5_password_ok() {
    connect("user=md5_user password=password dbname=postgres");
}

#[test]
fn scram_password_missing() {
    connect_raw("user=scram_user dbname=postgres")
        .err()
        .unwrap();
}

#[test]
fn scram_password_wrong() {
    match connect_raw("user=scram_user password=foo dbname=postgres") {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn scram_password_ok() {
    connect("user=scram_user password=password dbname=postgres");
}

#[test]
fn pipelined_prepare() {
    let client = connect("user=postgres");

    let statement1 = client.prepare("SELECT $1::HSTORE[]").unwrap();
    let statement2 = client.prepare("SELECT $1::BIGINT").unwrap();

    assert_eq!(statement1.params()[0].name(), "_hstore");
    assert_eq!(statement1.columns()[0].type_().name(), "_hstore");

    assert_eq!(statement2.params()[0], Type::INT8);
    assert_eq!(statement2.columns()[0].type_(), &Type::INT8);
}

#[test]
fn insert_select() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)")
        .unwrap();

    let insert = client
        .prepare("INSERT INTO foo (name) VALUES ($1), ($2)")
        .unwrap();
    let select = client
        .prepare("SELECT id, name FROM foo ORDER BY id")
        .unwrap();

    let _ = client.execute(&insert, &[&"alice", &"bob"]).unwrap();
    let rows = client.query(&select, &[]).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "alice");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "bob");
}

#[test]
fn custom_enum() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TYPE pg_temp.mood AS ENUM (
                'sad',
                'ok',
                'happy'
            )",
        )
        .unwrap();

    let select = client.prepare("SELECT $1::mood").unwrap();

    let ty = &select.params()[0];
    assert_eq!("mood", ty.name());
    assert_eq!(
        &Kind::Enum(vec![
            "sad".to_string(),
            "ok".to_string(),
            "happy".to_string(),
        ]),
        ty.kind(),
    );
}

#[test]
fn custom_domain() {
    let client = connect("user=postgres");

    client
        .batch_execute("CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16)")
        .unwrap();

    let select = client.prepare("SELECT $1::session_id").unwrap();

    let ty = &select.params()[0];
    assert_eq!("session_id", ty.name());
    assert_eq!(&Kind::Domain(Type::BYTEA), ty.kind());
}

#[test]
fn custom_array() {
    let client = connect("user=postgres");

    let select = client.prepare("SELECT $1::HSTORE[]").unwrap();

    let ty = &select.params()[0];
    assert_eq!("_hstore", ty.name());
    match ty.kind() {
        Kind::Array(ty) => {
            assert_eq!("hstore", ty.name());
            assert_eq!(&Kind::Simple, ty.kind());
        }
        _ => panic!("unexpected kind"),
    }
}

#[test]
fn custom_composite() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC
            )",
        )
        .unwrap();

    let select = client.prepare("SELECT $1::inventory_item").unwrap();

    let ty = &select.params()[0];
    assert_eq!(ty.name(), "inventory_item");
    match ty.kind() {
        Kind::Composite(fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::TEXT);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::INT4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::NUMERIC);
        }
        _ => panic!("unexpected kind"),
    }
}

#[test]
fn custom_range() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TYPE pg_temp.floatrange AS RANGE (
                subtype = float8,
                subtype_diff = float8mi
            )",
        )
        .unwrap();

    let select = client.prepare("SELECT $1::floatrange").unwrap();

    let ty = &select.params()[0];
    assert_eq!("floatrange", ty.name());
    assert_eq!(&Kind::Range(Type::FLOAT8), ty.kind());
}

#[test]
fn simple_query() {
    let client = connect("user=postgres");

    let messages = client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('steven'), ('joe');
            SELECT * FROM foo ORDER BY id;",
        )
        .unwrap();

    match messages[0] {
        SimpleQueryMessage::CommandComplete(0) => {}
        _ => panic!("unexpected message"),
    }
    match messages[1] {
        SimpleQueryMessage::CommandComplete(2) => {}
        _ => panic!("unexpected message"),
    }
    match &messages[2] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("1"));
            assert_eq!(row.get(1), Some("steven"));
        }
        _ => panic!("unexpected message"),
    }
    match &messages[3] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("2"));
            assert_eq!(row.get(1), Some("joe"));
        }
        _ => panic!("unexpected message"),
    }
    match messages[4] {
        SimpleQueryMessage::CommandComplete(2) => {}
        _ => panic!("unexpected message"),
    }
    assert_eq!(messages.len(), 5);
}

#[test]
fn cancel_query_raw() {
    let client = connect("user=postgres");

    join! {
        {
            let ret = client.batch_execute("SELECT pg_sleep(100)");
            assert!(ret.is_err());
            let err = ret.err().unwrap();
            let err = err.code();
            assert_eq!(err, Some(&SqlState::QUERY_CANCELED));
        },
        {
            let socket = TcpStream::connect("127.0.0.1:5433").unwrap();
            may::coroutine::sleep(Duration::from_millis(100));
            let ret = client.cancel_query_raw(socket);
            assert!(ret.is_ok());
        }
    }
}

#[test]
fn transaction_commit() {
    let mut client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .unwrap();

    let transaction = client.transaction().unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .unwrap();
    transaction.commit().unwrap();

    let stmt = client.prepare("SELECT name FROM foo").unwrap();
    let rows = client.query(&stmt, &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "steven");
}

#[test]
fn transaction_rollback() {
    let mut client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .unwrap();

    let transaction = client.transaction().unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .unwrap();
    transaction.rollback().unwrap();

    let stmt = client.prepare("SELECT name FROM foo").unwrap();
    let rows = client.query(&stmt, &[]).unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn transaction_rollback_drop() {
    let mut client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .unwrap();

    let transaction = client.transaction().unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .unwrap();
    drop(transaction);

    let stmt = client.prepare("SELECT name FROM foo").unwrap();
    let rows = client.query(&stmt, &[]).unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_in() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .unwrap();

    let stmt = client.prepare("COPY foo FROM STDIN").unwrap();
    let stream = vec![b"1\tjim\n".to_vec(), b"2\tjoe\n".to_vec()]
        .into_iter()
        .map(Ok::<_, String>);
    let rows = client.copy_in(&stmt, &[], stream).unwrap();
    assert_eq!(rows, 2);

    let stmt = client
        .prepare("SELECT id, name FROM foo ORDER BY id")
        .unwrap();
    let rows = client.query(&stmt, &[]).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "jim");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "joe");
}

#[test]
fn copy_in_large() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .unwrap();

    let stmt = client.prepare("COPY foo FROM STDIN").unwrap();

    let a = "0\tname0\n".to_string();
    let mut b = String::new();
    for i in 1..5_000 {
        writeln!(b, "{0}\tname{0}", i).unwrap();
    }
    let mut c = String::new();
    for i in 5_000..10_000 {
        writeln!(c, "{0}\tname{0}", i).unwrap();
    }
    let stream = vec![a, b, c].into_iter().map(Ok::<_, String>);

    let rows = client.copy_in(&stmt, &[], stream).unwrap();
    assert_eq!(rows, 10_000);
}

#[test]
fn copy_in_error() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .unwrap();

    let stmt = client.prepare("COPY foo FROM STDIN").unwrap();
    let stream = vec![Ok(b"1\tjim\n".to_vec()), Err("asdf")].into_iter();
    let error = client.copy_in(&stmt, &[], stream).unwrap_err();
    assert!(error.to_string().contains("asdf"));

    let stmt = client
        .prepare("SELECT id, name FROM foo ORDER BY id")
        .unwrap();
    let rows = client.query(&stmt, &[]).unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_out() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
            id SERIAL,
            name TEXT
        );

        INSERT INTO foo (name) VALUES ('jim'), ('joe');",
        )
        .unwrap();

    let stmt = client.prepare("COPY foo TO STDOUT").unwrap();
    let data = client
        .copy_out(&stmt, &[])
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .concat();
    assert_eq!(&data[..], b"1\tjim\n2\tjoe\n");
}

// #[test]
// fn notifications() {
//     let (mut client, mut connection) = connect_raw("user=postgres").unwrap();

//     let (tx, rx) = mpsc::unbounded();
//     let stream = stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!(e));
//     let connection = stream.forward(tx).map(|r| r.unwrap());
//     tokio::spawn(connection);

//     client
//         .batch_execute(
//             "LISTEN test_notifications;
//              NOTIFY test_notifications, 'hello';
//              NOTIFY test_notifications, 'world';",
//         )
//         .unwrap();

//     drop(client);

//     let notifications = rx
//         .filter_map(|m| match m {
//             AsyncMessage::Notification(n) => future::ready(Some(n)),
//             _ => future::ready(None),
//         })
//         .collect::<Vec<_>>();
//     assert_eq!(notifications.len(), 2);
//     assert_eq!(notifications[0].channel(), "test_notifications");
//     assert_eq!(notifications[0].payload(), "hello");
//     assert_eq!(notifications[1].channel(), "test_notifications");
//     assert_eq!(notifications[1].payload(), "world");
// }

#[test]
fn query_portal() {
    let mut client = connect("user=postgres");

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );

            INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('charlie');",
        )
        .unwrap();

    let stmt = client
        .prepare("SELECT id, name FROM foo ORDER BY id")
        .unwrap();

    let transaction = client.transaction().unwrap();

    let portal = transaction.bind(&stmt, &[]).unwrap();
    let r1 = transaction.query_portal(&portal, 2).unwrap();
    let r2 = transaction.query_portal(&portal, 2).unwrap();
    let r3 = transaction.query_portal(&portal, 2).unwrap();

    assert_eq!(r1.len(), 2);
    assert_eq!(r1[0].get::<_, i32>(0), 1);
    assert_eq!(r1[0].get::<_, &str>(1), "alice");
    assert_eq!(r1[1].get::<_, i32>(0), 2);
    assert_eq!(r1[1].get::<_, &str>(1), "bob");

    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].get::<_, i32>(0), 3);
    assert_eq!(r2[0].get::<_, &str>(1), "charlie");

    assert_eq!(r3.len(), 0);
}

#[test]
fn query_one() {
    let client = connect("user=postgres");

    client
        .batch_execute(
            "
        CREATE TEMPORARY TABLE foo (
            name TEXT
        );
        INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('carol');
    ",
        )
        .unwrap();

    client
        .query_one("SELECT * FROM foo WHERE name = 'dave'", &[])
        .err()
        .unwrap();
    client
        .query_one("SELECT * FROM foo WHERE name = 'alice'", &[])
        .unwrap();
    client.query_one("SELECT * FROM foo", &[]).err().unwrap();
}
