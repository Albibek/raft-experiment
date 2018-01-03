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
    peers: Vec<ServerId>,

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

// Most of the functions return the message type to answer and a timeout
// timeout means caller should reset the previous consensus timeout
// and set the new one to the one returned by function
// see ConsensusTimeout docs for timeout types
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Creates a `Consensus`.
    pub fn new(id: ServerId, peers: Vec<ServerId>, log: L, state_machine: M) -> Self {
        let leader_state = LeaderState::new(
            log.latest_log_index().unwrap(),
            &peers.iter().cloned().collect(),
        );
        Self {
            id: id,
            peers: peers,
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

    /// Returns the set of initial action which should be executed upon startup.
    pub fn init(&self) -> ConsensusTimeout {
        ConsensusTimeout::Election
    }

    /// Applies a peer message to the consensus state machine.
    pub fn apply_peer_message(
        &mut self,
        from: ServerId,
        message: PeerMessage,
    ) -> (Option<PeerMessage>, Option<ConsensusTimeout>) {
        // process error
        match message {
            PeerMessage::AppendEntriesRequest(request) => {
                //FIXME process return values from these
                let (response, timeout) = self.append_entries_request(from, request);
                (Some(PeerMessage::AppendEntriesResponse(response)), timeout)
            }

            PeerMessage::AppendEntriesResponse(response) => {
                let (request, timeout) = self.append_entries_response(from, response);
                (request.map(PeerMessage::AppendEntriesRequest), timeout)
            }
            PeerMessage::RequestVoteRequest(request) => {
                let response = self.request_vote_request(from, request);
                (response.map(PeerMessage::RequestVoteResponse), None)
            }

            PeerMessage::RequestVoteResponse(response) => {
                self.request_vote_response(from, response);
                (None, None)
            }
        }
    }

    /// Apply an append entries request to the consensus state machine.
    pub(crate) fn append_entries_request(
        &mut self,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> (AppendEntriesResponse, Option<ConsensusTimeout>) {
        let leader_term = request.term;
        let current_term = self.current_term();

        if leader_term < current_term {
            return (AppendEntriesResponse::StaleTerm(current_term), None);
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
                        AppendEntriesResponse::InconsistentPrevEntry(
                            self.current_term(),
                            leader_prev_log_index,
                        )
                    } else {
                        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                            Term::from(0)
                        } else {
                            self.log.entry(leader_prev_log_index).unwrap().0
                        };

                        if existing_term != leader_prev_log_term {
                            // If an existing entry conflicts with a new one (same index but different terms),
                            // delete the existing entry and all that follow it
                            AppendEntriesResponse::InconsistentPrevEntry(
                                self.current_term(),
                                leader_prev_log_index,
                            )
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
                                    // TODO: should not be sent?
                                    return (AppendEntriesResponse::StaleEntry, None);
                                }
                                // TODO: remove this?
                                let entries_vec: Vec<(Term, &[u8])> = entries
                                    .iter()
                                    .map(|&(term, ref data)| (term, data.as_slice()))
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
                            AppendEntriesResponse::Success(
                                self.current_term(),
                                self.log.latest_log_index().unwrap(), // TODO: should we retake olg index?
                            )
                        }
                    }
                };
                (message, Some(ConsensusTimeout::Election))
            }
            ConsensusState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                // previously the latter did set the timeout to true and pushed election timeout to
                // actions
                // TODO: check if this logic was correct because second call to
                // append_entries_request can return with timeout seto to None
                let (message, timeout) = self.append_entries_request(from, request);
                (message, Some(ConsensusTimeout::Election))
            }
            ConsensusState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    // FIXME make error out of this
                    panic!(
                        "{:?}: peer leader {} with matching term {:?} detected.",
                        self,
                        from,
                        current_term
                    );
                }

                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                let (message, timeout) = self.append_entries_request(from, request);
                (message, Some(ConsensusTimeout::Election))
            }
        }
    }

    /// Apply an append entries response to the consensus state machine.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    pub(crate) fn append_entries_response(
        &mut self,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> (Option<AppendEntriesRequest>, Option<ConsensusTimeout>) {
        let local_term = self.current_term();
        //let responder_term = Term::from(response.get_term());
        let local_latest_log_index = self.latest_log_index();

        /*
        if local_term < responder_term {
            // Responder has a higher term number. Relinquish leader position (if it is held), and
            // return to follower status.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            self.transition_to_follower(responder_term, from);
            return (None, ConsensusTimeout::Election);
        } else if local_term > responder_term {
            // scoped_debug!(
            //"AppendEntriesResponse from peer {} with a different term: {}",
            //from,
            //responder_term
            //);
            // Responder is responding to an AppendEntries request from a different term. Ignore
            // the response.
            return (None, None);
        } */

        match response {
            AppendEntriesResponse::Success(term, _) |
            AppendEntriesResponse::StaleTerm(term) |
            AppendEntriesResponse::InconsistentPrevEntry(term, _) if local_term < term => {
                self.transition_to_follower(term, from);
                return (None, Some(ConsensusTimeout::Election));
            }
            AppendEntriesResponse::Success(term, _) |
            AppendEntriesResponse::StaleTerm(term) |
            AppendEntriesResponse::InconsistentPrevEntry(term, _) if local_term > term => {
                return (None, None);
            }  
            AppendEntriesResponse::Success(_, follower_latest_log_index) => {
                scoped_assert!(self.is_leader());
                let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                scoped_assert!(follower_latest_log_index <= local_latest_log_index);
                self.leader_state.set_match_index(
                    from,
                    follower_latest_log_index,
                );
                self.advance_commit_index();
                // Adds command_response_success
            }
            AppendEntriesResponse::InconsistentPrevEntry(_, next_index) => {
                scoped_assert!(self.is_leader());
                self.leader_state.set_next_index(
                    from,
                    LogIndex::from(next_index),
                );
            }
            AppendEntriesResponse::StaleEntry => {
                return (None, None);
            }
            AppendEntriesResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.

                return (None, None);
            }
            AppendEntriesResponse::InternalError(error_result) => {
                //let error = error_result.unwrap_or("[unable to decode internal error]");
                // TODO
                return (None, None);
            }
            // Err(error) => {
            //scoped_warn!(
            //"AppendEntriesResponse from peer {}: unable to deserialize \
            //response: {}",
            //from,
            //error
            //);
            //}
        }

        let next_index = self.leader_state.next_index(&from);
        if next_index <= local_latest_log_index {
            // If the peer is behind, send it entries to catch up.
            //"AppendEntriesResponse: peer {} is missing at least {} entries; \
            //          sending missing entries",
            let prev_log_index = next_index - 1;
            let prev_log_term = if prev_log_index == LogIndex(0) {
                Term(0)
            } else {
                self.log.entry(prev_log_index).unwrap().0
            };

            let from_index = next_index;
            let until_index = local_latest_log_index + 1;

            let entries = self.log
                .entries(LogIndex::from(from_index), LogIndex::from(until_index))
                .unwrap()
                .into_iter()
                .map(|(t, slice)| (t, slice.to_vec()))
                .collect();

            let message = AppendEntriesRequest {
                term: local_term,
                prev_log_index,
                prev_log_term,
                entries: entries,
                leader_commit: self.commit_index,
            };

            self.leader_state.set_next_index(
                from,
                local_latest_log_index + 1,
            );
            (Some(message), None)
        } else {
            // If the peer is caught up, set a heartbeat timeout.
            // / TODO deal with "from"
            (None, Some(ConsensusTimeout::Heartbeat(from)))
        }
    }

    fn advance_commit_index(&mut self) -> Option<CommandResponse> {
        scoped_assert!(self.is_leader());
        let majority = self.majority();
        // TODO: Figure out failure condition here.
        while self.commit_index < self.log.latest_log_index().unwrap() {
            if self.leader_state.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
            //scoped_debug!("commit index advanced to {}", self.commit_index);
            } else {
                break; // If there isn't a majority now, there won't be one later.
            }
        }

        // this only happens when called from proposal_request
        let results = self.apply_commits();
        // in general we should reply to all clients' proposals with success
        while let Some(&(client, index)) = self.leader_state.proposals.get(0) {
            if index <= self.commit_index {
                //scoped_trace!("responding to client {} for entry {}", client, index);

                // We know that there will be an index here since it was commited
                // and the index is less than that which has been commited.
                let result = &results[&index];
            //let message = messages::command_response_success(result);
            //actions.client_messages.push((client, message));
            //self.leader_state.proposals.pop_front();
            // Some(CommandResponse::Success)
            } else {
                break;
            }
        }

        return None;
    }

    /// Applies a peer request vote request to the consensus state machine.
    pub(crate) fn request_vote_request(
        &mut self,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Option<RequestVoteResponse> {
        // TODO return Option is needless?
        // TODO remove
        let candidate_term = request.term;
        let candidate_log_term = request.last_log_term;
        let candidate_log_index = request.last_log_index;
        scoped_debug!(
            "RequestVoteRequest from Consensus {{ id: {}, term: {}, latest_log_term: \
                       {}, latest_log_index: {} }}",
            &candidate,
            candidate_term,
            candidate_log_term,
            candidate_log_index
        );
        let local_term = self.current_term();

        let new_local_term = if candidate_term > local_term {
            scoped_info!(
                "received RequestVoteRequest from Consensus {{ id: {}, term: {} }} \
                         with newer term; transitioning to Follower",
                candidate,
                candidate_term
            );
            self.transition_to_follower(candidate_term, candidate);
            candidate_term
        } else {
            local_term
        };

        let message = if candidate_term < local_term {
            Some(RequestVoteResponse::StaleTerm(new_local_term))
        } else if candidate_log_term < self.latest_log_term() ||
                   candidate_log_index < self.latest_log_index()
        {
            Some(RequestVoteResponse::InconsistentLog(new_local_term))
        } else {
            match self.log.voted_for().unwrap() { // TODO deal with unwrap
                None => {
                    self.log.set_voted_for(candidate).unwrap(); // TODO deal with unwrap
                    Some(RequestVoteResponse::Granted(new_local_term))
                }
                Some(voted_for) if voted_for == candidate => {
                    Some(RequestVoteResponse::Granted(new_local_term))
                }
                // FIXME: deal with the "_" wildcard
                _ => Some(RequestVoteResponse::AlreadyVoted(new_local_term)),
            }
        };
        message
    }

    /// Applies a request vote response to the consensus state machine.
    pub(crate) fn request_vote_response(
        &mut self,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> HashMap<ServerId, PeerMessage> {
        scoped_debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term();
        // TODO remove
        let voter_term = response.voter_term();

        let majority = self.majority();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            scoped_info!(
                "received RequestVoteResponse from Consensus {{ id: {}, term: {} }} \
                         with newer term; transitioning to Follower",
                from,
                voter_term
            );
            self.transition_to_follower(voter_term, from);
            HashMap::new()
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
            HashMap::new()
        } else if self.is_candidate() {
            // A vote was received!
            if let RequestVoteResponse::Granted(_) = response {
                self.candidate_state.record_vote(from);
                if self.candidate_state.count_votes() >= majority {
                    scoped_info!(
                        "election for term {} won; transitioning to Leader",
                        local_term
                    );
                    self.transition_to_leader()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            }
        } else {
            unreachable!()
        }
    }
}

//==================== Client messages processing
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Applies a client message to the consensus state machine.
    pub fn apply_client_message(
        &mut self,
        from: ClientId,
        message: ClientRequest,
    ) -> (Option<ClientResponse>, HashMap<ServerId, PeerMessage>) {
        //push_log_scope!("{:?}", self);
        match message {
            ClientRequest::Ping => (
                Some(ClientResponse::Ping(self.ping_request())),
                HashMap::new(),
            ),
            ClientRequest::Proposal(data) => {
                let (response, peer_msg) = self.proposal_request(from, data);
                (response.map(ClientResponse::Proposal), peer_msg)
            }
            ClientRequest::Query(data) => (
                Some(
                    ClientResponse::Query(self.query_request(from, data)),
                ),
                HashMap::new(),
            ),
        }
    }

    fn ping_request(&self) -> PingResponse {
        PingResponse {
            term: self.current_term(),
            index: self.latest_log_index(),
            state: self.state.clone(),
        }
    }

    /// Applies a client proposal to the consensus state machine.
    /// In addition to the client response this function requires peer messages to be sent
    fn proposal_request(
        &mut self,
        from: ClientId,
        request: Vec<u8>,
    ) -> (Option<CommandResponse>, HashMap<ServerId, PeerMessage>) {
        let leader = self.follower_state.leader;
        let mut peer_messages = HashMap::new();
        match self.state {
            ConsensusState::Candidate => (Some(CommandResponse::UnknownLeader), peer_messages),
            ConsensusState::Follower if leader.is_none() => (
                Some(CommandResponse::UnknownLeader),
                peer_messages,
            ),
            ConsensusState::Follower => {
                //&self.peers[&self.follower_state.leader.unwrap()],
                (
                    Some(CommandResponse::NotLeader(
                        self.follower_state.leader.unwrap().clone(),
                    )),
                    peer_messages,
                )
            }
            ConsensusState::Leader => {
                let prev_log_index = self.latest_log_index();
                let prev_log_term = self.latest_log_term();
                let term = self.current_term();
                let log_index = prev_log_index + 1;
                let leader_commit = self.commit_index;
                self.log
                    .append_entries(log_index, &[(term, &request)])
                    .unwrap();
                self.leader_state.proposals.push_back((from, log_index));
                if self.peers.is_empty() {
                    scoped_debug!("ProposalRequest from client {}: entry {}", from, log_index);
                    (self.advance_commit_index(), peer_messages)
                } else {
                    let message = AppendEntriesRequest {
                        term,
                        prev_log_index,
                        prev_log_term,
                        leader_commit,
                        entries: vec![(term, request)],
                    };
                    for &peer in self.peers.iter() {
                        if self.leader_state.next_index(&peer) == log_index {
                            let mut entry =
                                peer_messages.insert(
                                    peer,
                                    PeerMessage::AppendEntriesRequest(message.clone()),
                                );
                            self.leader_state.set_next_index(peer, log_index + 1);
                        }
                    }
                    (self.advance_commit_index(), peer_messages)
                }

            }
        }
    }

    /// Applies a client query to the state machine.
    fn query_request(&mut self, from: ClientId, request: Vec<u8>) -> CommandResponse {
        scoped_trace!("query from Client({})", from);
        let leader = self.follower_state.leader;
        match self.state {
            ConsensusState::Candidate => CommandResponse::UnknownLeader,
            ConsensusState::Follower if leader.is_none() => CommandResponse::UnknownLeader,
            ConsensusState::Follower => {
                //&self.peers[&self.follower_state.leader.unwrap()],
                CommandResponse::NotLeader(self.follower_state.leader.unwrap().clone())
            }
            ConsensusState::Leader => {
                // TODO(from original raft): This is probably not exactly safe.
                let result = self.state_machine.query(&request);
                CommandResponse::Success(result)
            }
        }
    }
}

//==================== Timeouts
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Triggers a heartbeat timeout for the peer.
    pub fn heartbeat_timeout(&mut self, peer: ServerId) -> AppendEntriesRequest {
        // FIXME do we really need to hold the queue for disconnected peer
        // (the one that has heartbeat timeout triggered)?
        // how is that different from ping?
        scoped_assert!(self.is_leader());
        scoped_debug!("HeartbeatTimeout for peer: {}", peer);
        AppendEntriesRequest {
            term: self.current_term(),
            prev_log_index: self.latest_log_index(),
            prev_log_term: self.log.latest_log_term().unwrap(), // TODO: error
            leader_commit: self.commit_index,
            entries: Vec::new(),
        }
    }

    /// Triggers an election timeout.
    pub fn election_timeout(&mut self) -> HashMap<ServerId, PeerMessage> {
        scoped_assert!(!self.is_leader());
        if self.peers.is_empty() {
            // Solitary replica special case; jump straight to Leader state.
            scoped_info!("ElectionTimeout: transitioning to Leader");
            scoped_assert!(self.is_follower());
            scoped_assert!(self.log.voted_for().unwrap().is_none());
            self.log.inc_current_term().unwrap();
            self.log.set_voted_for(self.id).unwrap();
            let latest_log_index = self.latest_log_index();
            self.state = ConsensusState::Leader;
            self.leader_state.reinitialize(latest_log_index);
            HashMap::new()
        } else {
            scoped_info!("ElectionTimeout: transitioning to Candidate");
            self.transition_to_candidate()
        }
    }
}

//==================== State transitions
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Transitions the consensus state machine to Follower state with the provided term. The
    /// `voted_for` field will be reset. The provided leader hint will replace the last known
    /// leader.
    fn transition_to_follower(&mut self, term: Term, leader: ServerId) {
        self.log.set_current_term(term).unwrap();
        self.state = ConsensusState::Follower;
        self.follower_state.set_leader(leader);
        //     actions.clear_peer_messages = true;
    }

    /// Transitions this consensus state machine to Leader state.
    fn transition_to_leader(&mut self) -> HashMap<ServerId, PeerMessage> {
        scoped_trace!("transitioning to Leader");
        let latest_log_index = self.log.latest_log_index().unwrap();
        self.state = ConsensusState::Leader;
        self.leader_state.reinitialize(latest_log_index);

        let message = AppendEntriesRequest {
            term: self.current_term(),
            prev_log_index: latest_log_index,
            prev_log_term: self.log.latest_log_term().unwrap(),
            leader_commit: self.commit_index,
            entries: Vec::new(),
        };

        let mut peer_messages = HashMap::new();
        for &peer in self.peers.iter() {
            let mut entry =
                peer_messages.insert(peer, PeerMessage::AppendEntriesRequest(message.clone()));
        }
        peer_messages

        // TODO: deal with timeouts
        //actions.clear_timeouts = true;
        //actions.clear_peer_messages = true;
    }

    /// Transitions the consensus state machine to Candidate state.
    fn transition_to_candidate(&mut self) -> HashMap<ServerId, PeerMessage> {
        scoped_trace!("transitioning to Candidate");
        self.log.inc_current_term().unwrap();
        self.log.set_voted_for(self.id).unwrap();
        self.state = ConsensusState::Candidate;
        self.candidate_state.clear();
        self.candidate_state.record_vote(self.id);

        let message = RequestVoteRequest {
            term: self.current_term(),
            last_log_index: self.latest_log_index(),
            last_log_term: self.log.latest_log_term().unwrap(),
        };

        let mut peer_messages = HashMap::new();
        for &peer in self.peers.iter() {
            let mut entry =
                peer_messages.insert(peer, PeerMessage::RequestVoteRequest(message.clone()));
        }
        peer_messages
        // TODO timeouts
        //actions.timeouts.push(ConsensusTimeout::Election);
        // actions.clear_peer_messages = true;
    }
}
//==================== Utility functions
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Applies all committed but unapplied log entries to the state machine.  Returns the set of
    /// return values from the commits applied.
    fn apply_commits(&mut self) -> HashMap<LogIndex, Vec<u8>> {
        let mut results = HashMap::new();
        while self.last_applied < self.commit_index {
            // Unwrap justified here since we know there is an entry here.
            let (_, entry) = self.log.entry(self.last_applied + 1).unwrap();

            if !entry.is_empty() {
                let result = self.state_machine.apply(entry);
                results.insert(self.last_applied + 1, result);
            }
            self.last_applied = self.last_applied + 1;
        }
        results
    }
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

    /// Returns current state of consensus state machine
    pub fn get_state(&self) -> ConsensusState {
        self.state.clone()
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
