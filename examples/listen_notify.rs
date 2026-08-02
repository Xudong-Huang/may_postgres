//! `LISTEN` / `NOTIFY` — receiving asynchronous notifications.
//!
//! One connection subscribes to a channel and drains notifications; another
//! raises them. This is the shape behind change-data-capture and cache
//! invalidation: a trigger calls `pg_notify()` when a row changes, and a
//! listener reacts without polling the table.
//!
//! Run with a PostgreSQL on localhost:
//!
//! ```sh
//! cargo run --example listen_notify
//! ```

use may_postgres::{Client, Error};
use std::time::Duration;

fn main() -> Result<(), Error> {
    let listener = may_postgres::connect("host=localhost user=postgres")?;
    let notifier = may_postgres::connect("host=localhost user=postgres")?;

    // Only channels this connection has subscribed to are delivered.
    listener.batch_execute("LISTEN orders")?;
    println!("listening on 'orders'");

    notifier.batch_execute(
        "NOTIFY orders, 'order-1 created';
         NOTIFY orders, 'order-2 created';",
    )?;

    // Notifications are decoded by the connection coroutine during I/O, so the
    // listener has to do some work before they appear. Draining inside the same
    // loop that does the work is the natural pattern — there is no separate
    // reactor to run.
    let mut seen = 0;
    for _ in 0..50 {
        listener.batch_execute("SELECT 1")?;

        while let Some(notification) = listener.notifications().pop() {
            println!(
                "  [{}] {} -> {}",
                notification.process_id(),
                notification.channel(),
                notification.payload(),
            );
            seen += 1;
        }

        if seen >= 2 {
            break;
        }
        may::coroutine::sleep(Duration::from_millis(20));
    }

    println!("received {seen} notification(s)");
    Ok(())
}

/// A long-running listener, as a cache invalidator or job runner would use it.
///
/// The queue is unbounded and only the caller drains it, so a process that
/// subscribes and then stops reading will grow it without limit. Draining every
/// iteration, as here, keeps it bounded.
#[allow(dead_code)]
fn run_forever(listener: &Client) -> Result<(), Error> {
    listener.batch_execute("LISTEN row_changes")?;

    loop {
        listener.batch_execute("SELECT 1")?;

        while let Some(notification) = listener.notifications().pop() {
            handle(notification.channel(), notification.payload());
        }

        may::coroutine::sleep(Duration::from_millis(100));
    }
}

#[allow(dead_code)]
fn handle(channel: &str, payload: &str) {
    println!("{channel}: {payload}");
}
