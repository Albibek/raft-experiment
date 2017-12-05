use {LogIndex, Term, ServerId, ClientId};

use std::collections::HashMap;
use persistent_log::fs::Entry;
use rand::{self, Rng};

//================= Requests and responses

pub enum PeerMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest,
    RequestVoteResponse,
    /// Must not be sent
    Ignore(IgnoreReason),
}

pub struct AppendEntriesRequest {
    /// The leader's term.
    pub term: Term,

    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,

    /// Term of prevLogIndex entry
    pub prev_log_term: Term,

    /// Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<Entry>,

    /// The Leader’s commit log index.
    pub leader_commit: LogIndex,
}

pub enum AppendEntriesResponse {
    Success(Term, LogIndex),
    StaleTerm(Term),
    InconsistentPrevEntry(Term, LogIndex),
    StaleEntry,
    InternalError(String), // TODO who returns this?
}

pub type CommitsData = HashMap<LogIndex, Vec<u8>>;
pub enum CommandResponse {
    Success(CommitsData),

    // The proposal failed because the Raft node is not the leader, and does
    // not know who the leader is.
    UnknownLeader,

    // The client request failed because the Raft node is not the leader.
    // The value returned may be the address of the current leader.
    NotLeader(ServerId),
}

pub enum RequestVoteResponse {
    StaleTerm(Term),
    InconsistentLog(Term),
    Granted(Term),
    AlreadyVoted(Term),
}

pub enum IgnoreReason {
}

pub enum ConsensusResponse {
    AppendEntriesSuccess(Term, LogIndex),

    // returned from advance_commit_index
    CommandResponseSuccess,
}

pub enum ConsensusError {
    //
}

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
