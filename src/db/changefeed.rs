use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

const DEFAULT_BUFFER_SIZE: usize = 10_000;

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl std::fmt::Display for ChangeOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeOp::Insert => write!(f, "insert"),
            ChangeOp::Update => write!(f, "update"),
            ChangeOp::Delete => write!(f, "delete"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub sequence: u64,
    pub database: String,
    pub collection: String,
    pub operation: ChangeOp,
    pub doc_id: String,
    pub document: Option<Value>,
    pub timestamp: DateTime<Utc>,
}

pub struct ChangeFeed {
    buffer: Mutex<VecDeque<ChangeEvent>>,
    max_size: usize,
    next_seq: AtomicU64,
    sender: broadcast::Sender<ChangeEvent>,
}

impl ChangeFeed {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(DEFAULT_BUFFER_SIZE)),
            max_size: DEFAULT_BUFFER_SIZE,
            next_seq: AtomicU64::new(1),
            sender,
        }
    }

    /// Emit a change event. Appends to the ring buffer and broadcasts to subscribers.
    pub fn emit(
        &self,
        database: &str,
        collection: &str,
        operation: ChangeOp,
        doc_id: &str,
        document: Option<Value>,
    ) {
        let sequence = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let event = ChangeEvent {
            sequence,
            database: database.to_string(),
            collection: collection.to_string(),
            operation,
            doc_id: doc_id.to_string(),
            document,
            timestamp: Utc::now(),
        };

        {
            let mut buf = self.buffer.lock();
            if buf.len() >= self.max_size {
                buf.pop_front();
            }
            buf.push_back(event.clone());
        }

        // Broadcast to subscribers (ignore error if no receivers)
        let _ = self.sender.send(event);
    }

    /// Get a broadcast receiver for live events.
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    /// Replay buffered events starting from `since_sequence` (exclusive)
    /// filtered by database and collection.
    pub fn replay(
        &self,
        database: &str,
        collection: &str,
        since_sequence: u64,
    ) -> Vec<ChangeEvent> {
        let buf = self.buffer.lock();
        buf.iter()
            .filter(|e| {
                e.sequence > since_sequence
                    && e.database == database
                    && e.collection == collection
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emit_and_replay() {
        let feed = ChangeFeed::new();
        feed.emit("db1", "col1", ChangeOp::Insert, "doc1", None);
        feed.emit("db1", "col1", ChangeOp::Update, "doc1", None);
        feed.emit("db1", "col2", ChangeOp::Delete, "doc2", None);

        let events = feed.replay("db1", "col1", 0);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].operation, ChangeOp::Insert);
        assert_eq!(events[1].operation, ChangeOp::Update);
    }

    #[test]
    fn test_since_sequence_filtering() {
        let feed = ChangeFeed::new();
        feed.emit("db1", "col1", ChangeOp::Insert, "d1", None);
        feed.emit("db1", "col1", ChangeOp::Insert, "d2", None);
        feed.emit("db1", "col1", ChangeOp::Insert, "d3", None);

        // Since sequence 1 (exclusive) → get events 2 and 3
        let events = feed.replay("db1", "col1", 1);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[1].sequence, 3);
    }

    #[test]
    fn test_ring_buffer_overflow() {
        let feed = ChangeFeed {
            buffer: Mutex::new(VecDeque::with_capacity(5)),
            max_size: 5,
            next_seq: AtomicU64::new(1),
            sender: broadcast::channel(16).0,
        };

        for i in 0..7 {
            feed.emit("db", "col", ChangeOp::Insert, &format!("d{}", i), None);
        }

        let events = feed.replay("db", "col", 0);
        assert_eq!(events.len(), 5);
        // Oldest two (seq 1, 2) should be dropped
        assert_eq!(events[0].sequence, 3);
        assert_eq!(events[4].sequence, 7);
    }

    #[test]
    fn test_broadcast_subscriber() {
        let feed = ChangeFeed::new();
        let mut rx = feed.subscribe();

        feed.emit("db", "col", ChangeOp::Insert, "d1", None);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.doc_id, "d1");
        assert_eq!(event.operation, ChangeOp::Insert);
    }
}
