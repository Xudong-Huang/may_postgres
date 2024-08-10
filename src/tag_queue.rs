//! A tagged channel that allows sending items with tags.

use may::queue::spsc::Queue;
use may::sync::{Condvar, Mutex};

use std::sync::Arc;
use std::usize;

/// trait for tagged type
pub trait Tagged {
    /// get the tag of the item
    fn tag(&self) -> usize;
}

/// A spsc queue that allows tagging items.
/// make sure the tag is always increased by 1
struct TagQueue<T: Tagged> {
    queue: Queue<T>,
    tag: Mutex<usize>,
    cvar: Condvar,
}

impl<T: Tagged> TagQueue<T> {
    fn new() -> TagQueue<T> {
        TagQueue {
            queue: Queue::new(),
            tag: Mutex::new(usize::MAX),
            cvar: Condvar::new(),
        }
    }
}

/// Create a new tagged channel.
pub fn tag_channel<T: Tagged>() -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(TagQueue::new());
    (
        Sender {
            queue: queue.clone(),
        },
        Receiver { queue },
    )
}

/// A sender for a tagged channel.
pub struct Sender<T: Tagged> {
    queue: Arc<TagQueue<T>>,
}

/// A receiver for a tagged channel.
pub struct Receiver<T: Tagged> {
    queue: Arc<TagQueue<T>>,
}

impl<T: Tagged> Sender<T> {
    /// Send an item to the receiver with tag.
    pub fn send(&self, item: T) {
        let tag = item.tag();
        let mut tag_lock = self.queue.tag.lock().unwrap();
        // assert_eq!(tag, tag_lock.wrapping_add(1));
        *tag_lock = tag;
        self.queue.queue.push(item);
        self.queue.cvar.notify_one();
    }
}

impl<T: Tagged> Receiver<T> {
    /// wait a tag ready
    pub fn wait(&self, tag: usize) {
        let mut tag_lock = self.queue.tag.lock().unwrap();
        while tag_lock.wrapping_add(1) <= tag {
            tag_lock = self.queue.cvar.wait(tag_lock).unwrap();
        }
    }

    /// Try receive an item from the sender.
    pub fn try_recv(&self) -> Option<T> {
        self.queue.queue.pop()
    }

    /// Receive an item from the sender.
    pub fn recv(&self, tag: usize) -> T {
        loop {
            let item = match self.try_recv() {
                Some(item) => item,
                None => {
                    self.wait(tag);
                    match self.try_recv() {
                        Some(item) => item,
                        None => {
                            // let self_tag = *self.queue.tag.lock().unwrap();
                            // println!("tag = {tag}, self_tag = {self_tag}");
                            may::coroutine::yield_now();
                            continue;
                        }
                    }
                }
            };
            if item.tag() < tag {
                // println!("tag = {tag}, item.tag = {}", item.tag());
                continue;
            }
            return item;
        }
    }
}
