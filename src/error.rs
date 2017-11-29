use std;
use std::{io, net, fmt};
use capnp;
/// A simple convienence type.
pub type Result<T> = std::result::Result<T, Error>;

wrapped_enum!{
    #[doc = "The generic `raft::Error` is composed of one of the errors that can originate from the"]
    #[doc = "various libraries consumed by the library."]
    #[doc = "With the exception of the `Raft` variant these are generated from `try!()` macros invoking"]
    #[doc = "on `io::Error` or `capnp::Error` by using"]
    #[doc = "[`FromError`](https://doc.rust-lang.org/std/error/#the-fromerror-trait)."]
    #[derive(Debug)]
    pub enum Error {
/// An error originating from the [Cap'n Proto](https://github.com/dwrensha/capnproto-rust) library.
        CapnProto(capnp::Error),
/// A specific error produced when a bad Cap'n proto message is discovered.
        SchemaError(capnp::NotInSchema),
/// Errors originating from `std::io`.
        Io(io::Error),
/// Raft specific errors.
        Raft(RaftError),
/// Errors related to parsing addresses.
        AddrParse(net::AddrParseError),
    }
}

/// A Raft Error represents a Raft specific error that consuming code is expected to handle
/// gracefully.
#[derive(Debug)]
pub enum RaftError {
    /// The server ran out of slots in the slab for new connections
    ConnectionLimitReached,
    /// A client reported an invalid client id
    InvalidClientId,
    /// A consensus module reported back a leader not in the cluster.
    ClusterViolation,
    /// A remote connection attempted to use an unknown connection type in the connection preamble
    UnknownConnectionType,
    /// An invalid peer in in the peer set. Returned Server::new().
    InvalidPeerSet,
    /// Registering a connection failed
    ConnectionRegisterFailed,
    /// Failed to find a leader in the cluster. Try again later.
    LeaderSearchExhausted,

    /// Packet send or receive timeout
    Timeout,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::CapnProto(ref error) => fmt::Display::fmt(error, f),
            Error::SchemaError(ref error) => fmt::Display::fmt(error, f),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
            Error::Raft(ref error) => fmt::Debug::fmt(error, f),
            Error::AddrParse(ref error) => fmt::Debug::fmt(error, f),
        }
    }
}
