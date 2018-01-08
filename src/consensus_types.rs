use {LogIndex, Term, ServerId, ClientId};

use std::collections::HashMap;
use persistent_log::fs::Entry;
use rand::{self, Rng};
use state::ConsensusState;

//================= Peer messages

#[derive(Debug)]
pub enum PeerMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
}

#[derive(Clone, Debug)]
pub struct AppendEntriesRequest {
    /// The leader's term.
    pub term: Term,

    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,

    /// Term of prevLogIndex entry
    pub prev_log_term: Term,

    /// The Leader’s commit log index.
    pub leader_commit: LogIndex,

    /// Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<Entry>,
    // TODO: custom Clone or Cow to avoid cloning vector
}

#[derive(Debug)]
pub enum AppendEntriesResponse {
    Success(Term, LogIndex),
    StaleTerm(Term),
    InconsistentPrevEntry(Term, LogIndex),
    StaleEntry,
    InternalError(String), // TODO who returns this?
}

#[derive(Clone, Debug)]
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: Term,

    /// The index of the candidate's last log entry.
    pub last_log_index: LogIndex,

    /// The term of the candidate's last log entry.
    pub last_log_term: Term,
}

#[derive(Debug)]
pub enum RequestVoteResponse {
    StaleTerm(Term),
    InconsistentLog(Term),
    Granted(Term),
    AlreadyVoted(Term),
}

impl RequestVoteResponse {
    pub fn voter_term(&self) -> Term {
        match self {
            &RequestVoteResponse::StaleTerm(t) |
            &RequestVoteResponse::InconsistentLog(t) |
            &RequestVoteResponse::Granted(t) |
            &RequestVoteResponse::AlreadyVoted(t) => t,
        }
    }
}

//================= Client messages
#[derive(Debug)]
pub enum ClientRequest {
    Ping,
    Proposal(Vec<u8>),
    Query(Vec<u8>),
}


#[derive(Debug)]
pub enum ClientResponse {
    Ping(PingResponse),
    Proposal(CommandResponse),
    Query(CommandResponse),
}

#[derive(Debug)]
pub struct PingResponse {
    /// The server's current term
    pub(crate) term: Term,

    /// The server's current index
    pub(crate) index: LogIndex,

    /// The server's current state
    pub(crate) state: ConsensusState,
}

#[derive(Debug)]
pub enum CommandResponse {
    Success(Vec<u8>),

    // The proposal failed because the Raft node is not the leader, and does
    // not know who the leader is.
    UnknownLeader,

    // The client request failed because the Raft node is not the leader.
    // The value returned may be the address of the current leader.
    NotLeader(ServerId),
}

//================= other messages

/// Consensus timeout types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ConsensusTimeout {
    // An election timeout. Randomized value.
    Election,
    // A heartbeat timeout. Stable value.
    Heartbeat(ServerId),
}

pub struct TimeoutConfiguration {
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub heartbeat_ms: u64,
}

impl ConsensusTimeout {
    /// Returns the timeout period in milliseconds.
    pub fn duration_ms(&self, config: &TimeoutConfiguration) -> u64 {
        match *self {
            ConsensusTimeout::Election => {
                rand::thread_rng().gen_range::<u64>(config.election_min_ms, config.election_max_ms)
            }
            ConsensusTimeout::Heartbeat(..) => config.heartbeat_ms,
        }
    }
}
