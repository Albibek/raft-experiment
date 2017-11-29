use std::{ops, fmt, net, sync};
use error::*;
use std::sync::Arc;

use capnp::message::{DEFAULT_READER_OPTIONS, Reader, ReaderOptions};
use capnp_futures::serialize::OwnedSegments;

use uuid::Uuid;
use capnp::message::{Builder, HeapAllocator};
use capnp::OutputSegments;
pub type RaftIngress = Arc<Reader<OwnedSegments>>;

pub struct RaftEgress(pub Arc<Builder<HeapAllocator>>);

use capnp_futures::serialize::AsOutputSegments;

// TODO configurable
pub const TERM_TIMEOUT: u64 = 3000;


impl AsOutputSegments for RaftEgress {
    fn as_output_segments<'a>(&'a self) -> OutputSegments<'a> {
        self.0.get_segments_for_output()
    }
}

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(pub u64);
impl Term {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl From<u64> for Term {
    fn from(val: u64) -> Term {
        Term(val)
    }
}
impl Into<u64> for Term {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for Term {
    type Output = Term;
    fn add(self, rhs: u64) -> Term {
        Term(self.0.checked_add(rhs).expect(
            "overflow while incrementing Term",
        ))
    }
}
impl ops::Sub<u64> for Term {
    type Output = Term;
    fn sub(self, rhs: u64) -> Term {
        Term(self.0.checked_sub(rhs).expect(
            "underflow while decrementing Term",
        ))
    }
}
impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The index of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(pub u64);
impl LogIndex {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl From<u64> for LogIndex {
    fn from(val: u64) -> LogIndex {
        LogIndex(val)
    }
}
impl Into<u64> for LogIndex {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_add(rhs).expect(
            "overflow while incrementing LogIndex",
        ))
    }
}
impl ops::Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_sub(rhs).expect(
            "underflow while decrementing LogIndex",
        ))
    }
}
/// Find the offset between two log indices.
impl ops::Sub for LogIndex {
    type Output = u64;
    fn sub(self, rhs: LogIndex) -> u64 {
        self.0.checked_sub(rhs.0).expect(
            "underflow while subtracting LogIndex",
        )
    }
}
impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft server. Must be unique among the participants in a
/// consensus group.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct ServerId(pub u64);

impl ServerId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl From<u64> for ServerId {
    fn from(val: u64) -> ServerId {
        ServerId(val)
    }
}
impl Into<u64> for ServerId {
    fn into(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft client.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct ClientId(pub Uuid);
impl ClientId {
    fn new() -> ClientId {
        ClientId(Uuid::new_v4())
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    fn from_bytes(bytes: &[u8]) -> Result<ClientId> {
        Uuid::from_bytes(bytes).map(ClientId).map_err(|_| {
            RaftError::InvalidClientId.into()
        })
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
