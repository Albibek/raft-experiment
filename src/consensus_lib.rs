use std::collections::HashMap;
use std::net::SocketAddr;

use {LogIndex, Term, ServerId, ClientId, rand};
use std::cmp;
use state::{ConsensusState, LeaderState, CandidateState, FollowerState};
use consensus_types::*;
use consensus_shared::*;

use state_machine::StateMachine;
use persistent_log::Log;


/// An instance of a Raft state machine. The Consensus controls a client state machine, to which it
/// applies e/ntries in a globally consistent order.
#[derive(Debug)]
pub struct Consensus<L, M> {
    /// The ID of this consensus instance.
    id: ServerId,
    /// The IDs of peers in the consensus group.
    peers: HashMap<ServerId, SocketAddr>,
    peer_status: HashMap<ServerId, PeerStatus>,
    /// The persistent log.
    log: L,
    /// The client state machine to which client commands are applied.
    state_machine: M,

    /// Index of the latest entry known to be committed.
    commit_index: LogIndex,
    /// Index of the latest entry applied to the state machine.
    last_applied: LogIndex,

    /// The current state of the `Consensus` (`Leader`, `Candidate`, or `Follower`).
    state: ConsensusState,
    /// State necessary while a `Leader`. Should not be used otherwise.
    leader_state: LeaderState,
    /// State necessary while a `Candidate`. Should not be used otherwise.
    candidate_state: CandidateState,
    /// State necessary while a `Follower`. Should not be used otherwise.
    follower_state: FollowerState,
}

impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Creates a `Consensus`.
    pub fn new(
        id: ServerId,
        peers: HashMap<ServerId, SocketAddr>,
        log: L,
        state_machine: M,
    ) -> Self {
        let leader_state = LeaderState::new(
            log.latest_log_index().unwrap(),
            &peers.keys().cloned().collect(),
        );
        Self {
            id: id,
            peers: peers,
            peer_status: HashMap::new(),
            log: log,
            state_machine: state_machine,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            state: ConsensusState::Follower,
            leader_state: leader_state,
            candidate_state: CandidateState::new(),
            follower_state: FollowerState::new(),
        }
    }

    /// Returns the consenus peers.
    pub fn peers(&self) -> &HashMap<ServerId, SocketAddr> {
        &self.peers
    }

    /// Returns the set of initial action which should be executed upon startup.
    pub fn init(&self) -> ConsensusTimeout {
        ConsensusTimeout::Election
    }

    /// Applies a peer message to the consensus state machine.
    pub fn apply_peer_message(
        &mut self,
        from: ServerId,
        message: PeerMessage,
    ) -> Result<PeerMessage, ()> {
        // process error
        match message {
            PeerMessage::AppendEntriesRequest(request) => {
                //FIXME process return values from these
                self.append_entries_request(from, request).map(|response| {
                    PeerMessage::AppendEntriesResponse(response)
                })
            }
            //message::Which::AppendEntriesResponse(Ok(response)) => {
            //consensus.append_entries_response(from, response);
            //}
            //message::Which::RequestVoteRequest(Ok(request)) => {
            //consensus.request_vote_request(from, request);
            //}
            //message::Which::RequestVoteResponse(Ok(response)) => {
            //consensus.request_vote_response(from, response);
            //}
            _ => unimplemented!(),
        }
    }

    /// Apply an append entries request to the consensus state machine.
    fn append_entries_request(
        &mut self,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, ()> {
        let leader_term = request.term;
        let current_term = self.current_term();

        if leader_term < current_term {
            return Ok(AppendEntriesResponse::StaleTerm(current_term));
        }

        match self.state {
            ConsensusState::Follower => {
                let message = {
                    if current_term < leader_term {
                        self.log.set_current_term(leader_term).unwrap();
                        self.follower_state.set_leader(from);
                    }

                    let leader_prev_log_index = request.prev_log_index;
                    let leader_prev_log_term = request.prev_log_term;

                    let latest_log_index = self.latest_log_index();
                    if latest_log_index < leader_prev_log_index {
                        // If the previous entries index was not the same we'd leave a gap! Reply failure.
                        Ok(AppendEntriesResponse::InconsistentPrevEntry(
                            self.current_term(),
                            leader_prev_log_index,
                        ))
                    } else {
                        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                            Term::from(0)
                        } else {
                            self.log.entry(leader_prev_log_index).unwrap().0
                        };

                        if existing_term != leader_prev_log_term {
                            // If an existing entry conflicts with a new one (same index but different terms),
                            // delete the existing entry and all that follow it
                            Ok(AppendEntriesResponse::InconsistentPrevEntry(
                                self.current_term(),
                                leader_prev_log_index,
                            ))
                        } else {
                            if request.entries.len() > 0 {
                                let entries = request.entries; // TODO remove
                                let num_entries = entries.len();
                                let new_latest_log_index = leader_prev_log_index +
                                    num_entries as u64;
                                if new_latest_log_index < self.follower_state.min_index {
                                    // Stale entry; ignore. This guards against overwriting a
                                    // possibly committed part of the log if messages get
                                    // rearranged; see ktoso/akka-raft#66.
                                    return Ok(AppendEntriesResponse::StaleEntry);
                                }
                                // TODO: remove this?
                                let entries_vec: Vec<(Term, &[u8])> = entries
                                    .iter()
                                    .map(|&(term, data)| (term, data.as_slice()))
                                    .collect();

                                self.log
                                    .append_entries(leader_prev_log_index + 1, &entries_vec)
                                    .unwrap();
                                self.follower_state.min_index = new_latest_log_index;
                                // We are matching the leader's log up to and including `new_latest_log_index`.
                                self.commit_index =
                                    cmp::min(request.leader_commit, new_latest_log_index);
                                self.apply_commits();
                            } else {
                                // FIXME: remove panic
                                // return Err()
                                panic!("AppendEntriesRequest: no entry list")
                            }
                            Ok(ConsensusResponse::AppendEntriesSuccess(
                                self.current_term(),
                                self.log.latest_log_index().unwrap(),
                            ))
                        }
                    }
                };
                message
                //FIXME: start/reset the election timeout
                //actions.timeouts.push(ConsensusTimeout::Election);
            }
            ConsensusState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!(
                    "received AppendEntriesRequest from Consensus {{ id: {}, term: {} \
                              }} with newer term; transitioning to Follower",
                    from,
                    leader_term
                );
                self.transition_to_follower(leader_term, from);
                self.append_entries_request(from, request)
            }
            ConsensusState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    // TODO make error out of this
                    panic!(
                        "{:?}: peer leader {} with matching term {:?} detected.",
                        self,
                        from,
                        current_term
                    );
                }

                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!(
                    "received AppendEntriesRequest from Consensus {{ id: {}, term: {} \
                              }} with newer term; transitioning to Follower",
                    from,
                    leader_term
                );
                self.transition_to_follower(leader_term, from);
                self.append_entries_request(from, request)
            }
        }
    }
}

//==================== Utility functions
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Returns whether the consensus state machine is currently a Leader.
    fn is_leader(&self) -> bool {
        self.state == ConsensusState::Leader
    }

    /// Returns whether the consensus state machine is currently a Follower.
    fn is_follower(&self) -> bool {
        self.state == ConsensusState::Follower
    }

    /// Returns whether the consensus state machine is currently a Candidate.
    fn is_candidate(&self) -> bool {
        self.state == ConsensusState::Candidate
    }

    /// Returns the current term.
    fn current_term(&self) -> Term {
        self.log.current_term().unwrap()
    }

    /// Returns the term of the latest applied log entry.
    fn latest_log_term(&self) -> Term {
        self.log.latest_log_term().unwrap()
    }

    /// Returns the index of the latest applied log entry.
    fn latest_log_index(&self) -> LogIndex {
        self.log.latest_log_index().unwrap()
    }

    /// Get the cluster quorum majority size.
    fn majority(&self) -> usize {
        let peers = self.peers.len();
        // FIXME error processing
        let cluster_members = peers.checked_add(1).expect(&format!(
            "unable to support {} cluster members",
            peers
        ));
        (cluster_members >> 1) + 1
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub fn consensus_lib() {
        //
    }
}
