use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;

use {ClientId, LogIndex, ServerId, Term};
use std::cmp;
use state::{CandidateState, ConsensusState, FollowerState, LeaderState};
use consensus_types::*;
use consensus_shared::*;

use state_machine::StateMachine;
use persistent_log::Log;

/// Handler for actions returned from consensus
pub trait ConsensusHandler: Debug {
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage);
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse);
    fn set_timeout(&mut self, timeout: ConsensusTimeout);
    fn clear_timeout(&mut self, timeout: ConsensusTimeout);

    /// Called when the particular event has been fully processed
    /// Useful for doing actions in batches
    fn done(&mut self) {}
}

/// An instance of a Raft state machine. The Consensus controls a client state machine, to which it
/// applies e/ntries in a globally consistent order.
#[derive(Debug, Clone)]
pub struct Consensus<L, M, H> {
    // The ID of this consensus instance.
    id: ServerId,

    // The IDs of peers in the consensus group.
    peers: Vec<ServerId>,

    // The persistent log.
    log: L,
    // The client state machine to which client commands are applied.
    state_machine: M,

    // External handler of consensus responses
    pub handler: H,

    // Index of the latest entry known to be committed.
    commit_index: LogIndex,

    // Index of the latest entry applied to the state machine.
    last_applied: LogIndex,

    // The current state of the `Consensus` (`Leader`, `Candidate`, or `Follower`).
    state: ConsensusState,

    // State necessary while a `Leader`. Should not be used otherwise.
    leader_state: LeaderState,

    // State necessary while a `Candidate`. Should not be used otherwise.
    candidate_state: CandidateState,

    // State necessary while a `Follower`. Should not be used otherwise.
    follower_state: FollowerState,
}

// Most of the functions return the message type to answer and a timeout
// timeout means caller should reset the previous consensus timeout
// and set the new one to the one returned by function
// see ConsensusTimeout docs for timeout types
impl<L, M, H> Consensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    /// Creates a `Consensus`.
    pub fn new(id: ServerId, peers: Vec<ServerId>, log: L, state_machine: M, handler: H) -> Self {
        let leader_state = LeaderState::new(
            log.latest_log_index().unwrap(),
            &peers.iter().cloned().collect(),
        );
        Self {
            id: id,
            peers: peers,
            log: log,
            state_machine: state_machine,
            handler: handler,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            state: ConsensusState::Follower,
            leader_state: leader_state,
            candidate_state: CandidateState::new(),
            follower_state: FollowerState::new(),
        }
    }

    /// Calls initial actions which should be executed upon startup.
    pub fn init(&mut self) {
        self.handler.set_timeout(ConsensusTimeout::Election);
    }

    /// Applies a peer message to the consensus state machine.
    pub fn apply_peer_message(
        &mut self,
        from: ServerId,
        message: PeerMessage,
    ) -> Result<(), InternalError> {
        push_log_scope!("{:?}", self.id);
        let response = match message {
            PeerMessage::AppendEntriesRequest(request) => {
                let response = self.append_entries_request(from, request)?;
                Some(PeerMessage::AppendEntriesResponse(response))
            }

            PeerMessage::AppendEntriesResponse(response) => {
                let request = self.append_entries_response(from, response)?;
                request.map(PeerMessage::AppendEntriesRequest)
            }
            PeerMessage::RequestVoteRequest(request) => {
                let response = self.request_vote_request(from, request)?;
                Some(PeerMessage::RequestVoteResponse(response))
            }

            PeerMessage::RequestVoteResponse(response) => {
                self.request_vote_response(from, response)?;
                None
            }
        };
        response.map(|response| self.handler.send_peer_message(from, response));
        self.handler.done();
        Ok(())
    }

    /// Apply an append entries request to the consensus state machine.
    pub(crate) fn append_entries_request(
        &mut self,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, InternalError> {
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
                                let new_latest_log_index =
                                    leader_prev_log_index + num_entries as u64;
                                if new_latest_log_index < self.follower_state.min_index {
                                    // Stale entry; ignore. This guards against overwriting a
                                    // possibly committed part of the log if messages get
                                    // rearranged; see ktoso/akka-raft#66.
                                    return Ok(AppendEntriesResponse::StaleEntry);
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
                            }
                            AppendEntriesResponse::Success(
                                self.current_term(),
                                self.log.latest_log_index().unwrap(), // TODO: should we retake old index?
                            )
                        }
                    }
                };
                self.handler.set_timeout(ConsensusTimeout::Election);
                Ok(message)
            }
            ConsensusState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                // previously the latter ^^ did set the timeout to true and pushed election timeout to
                // actions
                self.handler.set_timeout(ConsensusTimeout::Election);
                self.append_entries_request(from, request)
            }
            ConsensusState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.

                    // Even implementation bugs should not break the whole process probably

                    return Err(InternalError::AnotherLeader(from, current_term));
                }

                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                self.append_entries_request(from, request)
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
    ) -> Result<Option<AppendEntriesRequest>, InternalError> {
        let local_term = self.current_term();
        let local_latest_log_index = self.latest_log_index();

        match response {
            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _) if local_term < term =>
            {
                self.transition_to_follower(term, from);
                self.handler.set_timeout(ConsensusTimeout::Election);
                return Ok(None);
            }
            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _) if local_term > term =>
            {
                return Ok(None);
            }
            AppendEntriesResponse::Success(_, follower_latest_log_index) => {
                self.assert_leader()?;
                let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                //scoped_assert!(follower_latest_log_index <= local_latest_log_index);
                if follower_latest_log_index > local_latest_log_index {
                    return Err(InternalError::BadFollowerIndex);
                }

                self.leader_state
                    .set_match_index(from, follower_latest_log_index);
                self.advance_commit_index()?;
            }
            AppendEntriesResponse::InconsistentPrevEntry(_, next_index) => {
                self.assert_leader()?;
                self.leader_state
                    .set_next_index(from, LogIndex::from(next_index));
            }
            AppendEntriesResponse::StaleEntry => {
                return Ok(None);
            }
            AppendEntriesResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.

                return Ok(None);
            }
        }

        let next_index = self.leader_state.next_index(&from);
        if next_index <= local_latest_log_index {
            // If the peer is behind, send it entries to catch up.
            scoped_trace!(
                "AppendEntriesResponse: peer {} is missing at least {} entries; \
                 sending missing entries",
                from,
                local_latest_log_index - next_index
            );
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

            self.leader_state
                .set_next_index(from, local_latest_log_index + 1);
            Ok(Some(message))
        } else {
            // If the peer is caught up, set a heartbeat timeout.
            self.handler.set_timeout(ConsensusTimeout::Heartbeat(from));
            Ok(None)
        }
    }

    fn advance_commit_index(&mut self) -> Result<(), InternalError> {
        if !self.is_leader() {
            return Err(InternalError::MustLeader);
        }

        // Here we try to move commit index to one the majority of peers in cluster already have
        let majority = self.majority();
        while self.commit_index < self.log.latest_log_index().unwrap() {
            if self.leader_state.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
                scoped_debug!("commit index advanced to {}", self.commit_index);
            } else {
                break; // If there isn't a majority now, there won't be one later.
            }
        }

        // As long as we know it, we send the connected clients the notification
        // about their proposals being committed
        let results = self.apply_commits();
        // TODO: fix client proposals being out of order (think if it is possible)
        while let Some(&(client, index)) = self.leader_state.proposals.get(0) {
            if index <= self.commit_index {
                scoped_trace!("responding to client {} for entry {}", client, index);

                // We know that there will be an index here since it was commited
                // and the index is less than that which has been commited.
                let result = &results[&index];
                self.handler.send_client_response(
                    client,
                    ClientResponse::Proposal(CommandResponse::Success(result.clone())),
                );
                self.leader_state.proposals.pop_front();
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Applies a peer request vote request to the consensus state machine.
    pub(crate) fn request_vote_request(
        &mut self,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, InternalError> {
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
            RequestVoteResponse::StaleTerm(new_local_term)
        } else if candidate_log_term < self.latest_log_term()
            || candidate_log_index < self.latest_log_index()
        {
            RequestVoteResponse::InconsistentLog(new_local_term)
        } else {
            // TODO: match self.log.voted_for().map_err(|e|InternalError::PersistentLog(format!("{:?}", e))? {
            match self.log.voted_for().unwrap() {
                None => {
                    //self.log.set_voted_for(candidate).map_err(|e| InternalError::PersistentLog(format!("{:?}", e)))?;
                    self.log.set_voted_for(candidate).unwrap();
                    RequestVoteResponse::Granted(new_local_term)
                }
                Some(voted_for) if voted_for == candidate => {
                    RequestVoteResponse::Granted(new_local_term)
                }
                // FIXME: deal with the "_" wildcard
                _ => RequestVoteResponse::AlreadyVoted(new_local_term),
            }
        };
        Ok(message)
    }

    /// Applies a request vote response to the consensus state machine.
    pub(crate) fn request_vote_response(
        &mut self,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> Result<(), InternalError> {
        scoped_debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term();
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
            self.transition_to_follower(voter_term, from)
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
            Ok(())
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
                    Ok(())
                }
            } else {
                Ok(())
            }
        } else {
            // received response with local_term = voter_term, but state is not candidate
            Err(InternalError::MustCandidate)
        }
    }
}

//==================== Client messages processing
impl<L, M, H> Consensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    /// Applies a client message to the consensus state machine.
    pub fn apply_client_message(&mut self, from: ClientId, message: ClientRequest) {
        push_log_scope!("{:?}", self.id);
        let response = match message {
            ClientRequest::Ping => Some(ClientResponse::Ping(self.ping_request())),
            ClientRequest::Proposal(data) => {
                let response = self.proposal_request(from, data);
                response.map(ClientResponse::Proposal)
            }
            ClientRequest::Query(data) => {
                Some(ClientResponse::Query(self.query_request(from, data)))
            }
        };

        response.map(|response| self.handler.send_client_response(from, response));
        self.handler.done();
    }

    fn ping_request(&self) -> PingResponse {
        PingResponse {
            term: self.current_term(),
            index: self.latest_log_index(),
            state: self.state.clone(),
        }
    }

    /// Applies a client proposal to the consensus state machine.
    fn proposal_request(&mut self, from: ClientId, request: Vec<u8>) -> Option<CommandResponse> {
        let leader = self.follower_state.leader;
        match self.state {
            ConsensusState::Candidate => Some(CommandResponse::UnknownLeader),
            ConsensusState::Follower if leader.is_none() => Some(CommandResponse::UnknownLeader),
            ConsensusState::Follower => {
                //&self.peers[&self.follower_state.leader.unwrap()],
                Some(CommandResponse::NotLeader(
                    self.follower_state.leader.unwrap().clone(),
                ))
            }
            ConsensusState::Leader => {
                let prev_log_index = self.latest_log_index();
                let prev_log_term = self.latest_log_term();
                let term = self.current_term();
                let log_index = prev_log_index + 1;
                scoped_debug!("ProposalRequest from client {}: entry {}", from, log_index);
                let leader_commit = self.commit_index;
                self.log
                    .append_entries(log_index, &[(term, &request)])
                    .unwrap(); // TODO error
                self.leader_state.proposals.push_back((from, log_index));

                // the order of messages could broken here if we return Queued after calling
                // advance_commit_index since it queues some more packets for client,
                // we must first of all let client know that proposal has been received
                // and only confirm commits after that
                self.handler
                    .send_client_response(from, ClientResponse::Proposal(CommandResponse::Queued));
                if self.peers.is_empty() {
                    self.advance_commit_index();
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
                            let mut entry = self.handler.send_peer_message(
                                peer,
                                PeerMessage::AppendEntriesRequest(message.clone()),
                            );
                            self.leader_state.set_next_index(peer, log_index + 1);
                        }
                    }
                    self.advance_commit_index();
                }
                // Since queued is already sent, we need no more messages for client
                None
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
impl<L, M, H> Consensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    /// Triggers a timeout for the peer.
    pub fn apply_timeout(&mut self, timeout: ConsensusTimeout) -> Result<(), InternalError> {
        push_log_scope!("{:?}", self.id);
        let response = match timeout {
            ConsensusTimeout::Election => {
                self.election_timeout();
            }
            ConsensusTimeout::Heartbeat(id) => {
                let request = self.heartbeat_timeout(id)?;
                let request = PeerMessage::AppendEntriesRequest(request);
                self.handler.send_peer_message(id, request);
            }
        };
        self.handler.done();
        Ok(())
    }

    /// Triggers a heartbeat timeout for the peer.
    pub fn heartbeat_timeout(
        &mut self,
        peer: ServerId,
    ) -> Result<AppendEntriesRequest, InternalError> {
        self.assert_leader()?;
        scoped_debug!("HeartbeatTimeout for peer: {}", peer);
        Ok(AppendEntriesRequest {
            term: self.current_term(),
            prev_log_index: self.latest_log_index(),
            prev_log_term: self.log.latest_log_term().unwrap(), // TODO: error
            leader_commit: self.commit_index,
            entries: Vec::new(),
        })
    }

    /// Triggers an election timeout.
    pub fn election_timeout(&mut self) -> Result<(), InternalError> {
        if self.is_leader() {
            return Err(InternalError::MustNotLeader);
        }
        if self.peers.is_empty() {
            // TODO: unwraps
            // Solitary replica special case; jump straight to Leader state.
            scoped_info!("ElectionTimeout: transitioning to Leader");
            scoped_assert!(self.is_follower());
            scoped_assert!(self.log.voted_for().unwrap().is_none());
            self.log.inc_current_term().unwrap();
            self.log.set_voted_for(self.id).unwrap();
            let latest_log_index = self.latest_log_index();
            self.state = ConsensusState::Leader;
            self.leader_state.reinitialize(latest_log_index);
            self.handler.clear_timeout(ConsensusTimeout::Election);
        } else {
            scoped_info!("ElectionTimeout: transitioning to Candidate");
            self.transition_to_candidate();
        }
        self.handler.done();
        Ok(())
    }
}

//==================== State transitions
impl<L, M, H> Consensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    /// Transitions the consensus state machine to Follower state with the provided term. The
    /// `voted_for` field will be reset. The provided leader hint will replace the last known
    /// leader.
    fn transition_to_follower(
        &mut self,
        term: Term,
        leader: ServerId,
    ) -> Result<(), InternalError> {
        // TODO: deal with log unwraps
        self.log.set_current_term(term).unwrap();
        self.state = ConsensusState::Follower;
        self.follower_state.set_leader(leader);
        Ok(())
    }

    /// Transitions this consensus state machine to Leader state.
    fn transition_to_leader(&mut self) -> Result<(), InternalError> {
        // TODO: deal with log unwraps
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

        for &peer in self.peers.iter() {
            self.handler
                .send_peer_message(peer, PeerMessage::AppendEntriesRequest(message.clone()));

            self.handler
                .clear_timeout(ConsensusTimeout::Heartbeat(peer));
        }
        self.handler.clear_timeout(ConsensusTimeout::Election);
        Ok(())
    }

    /// Transitions the consensus state machine to Candidate state.
    fn transition_to_candidate(&mut self) -> Result<(), InternalError> {
        // TODO: deal with log unwraps
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

        for &peer in self.peers.iter() {
            self.handler
                .send_peer_message(peer, PeerMessage::RequestVoteRequest(message.clone()));

            self.handler
                .clear_timeout(ConsensusTimeout::Heartbeat(peer));
        }
        self.handler.set_timeout(ConsensusTimeout::Election);
        Ok(())
    }
}
//==================== Utility functions
impl<L, M, H> Consensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    pub fn peer_connected(&mut self, peer: ServerId) {
        if !self.peers.iter().any(|&p| p == peer) {
            scoped_debug!("New peer detected: {:?}", peer);
            unimplemented!("Adding new peers is not supported");
            //TODO: adding new peers will require changing code in states
            //     self.peers.push(peer);
        }
        match self.state {
            ConsensusState::Leader => {
                // Send any outstanding entries to the peer, or an empty heartbeat if there are no
                // outstanding entries.
                let peer_index = self.leader_state.next_index(&peer);
                let until_index = self.latest_log_index() + 1;

                let prev_log_index = peer_index - 1;
                let prev_log_term = if prev_log_index == LogIndex::from(0) {
                    Term::from(0)
                } else {
                    self.log.entry(prev_log_index).unwrap().0
                };

                let entries = self.log.entries(peer_index, until_index).unwrap();
                // TODO: fix this
                let entries_vec = entries
                    .iter()
                    .map(|&(term, ref data)| (term, data.to_vec()))
                    .collect();

                let message = AppendEntriesRequest {
                    term: self.current_term(),
                    prev_log_index: prev_log_index,
                    prev_log_term: prev_log_term,
                    leader_commit: self.commit_index,
                    entries: entries_vec,
                };

                // For stateless/lossy  connections we cannot be sure if peer has received
                // our entries, so we call set_next_index only after response, which
                // is done in response processing code
                //self.leader_state.set_next_index(peer, until_index);
                self.handler
                    .send_peer_message(peer, PeerMessage::AppendEntriesRequest(message));
            }
            ConsensusState::Candidate => {
                // Resend the request vote request if a response has not yet been receieved.
                if self.candidate_state.peer_voted(peer) {
                    return;
                }

                let message = RequestVoteRequest {
                    term: self.current_term(),
                    last_log_index: self.latest_log_index(),
                    last_log_term: self.log.latest_log_term().unwrap(),
                };
                self.handler
                    .send_peer_message(peer, PeerMessage::RequestVoteRequest(message));
            }
            ConsensusState::Follower => {
                // No message is necessary; if the peer is a leader or candidate they will send a
                // message.
            }
        }

        self.handler.done();
    }

    /// Applies all committed but unapplied log entries to the state machine. Returns the set of
    /// return values from the commits applied.
    fn apply_commits(&mut self) -> HashMap<LogIndex, Vec<u8>> {
        let mut results = HashMap::new();
        while self.last_applied < self.commit_index {
            // Unwrap justified here since we know there is an entry in the log.
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

    fn assert_leader(&self) -> Result<(), InternalError> {
        if !self.is_leader() {
            Err(InternalError::MustLeader)
        } else {
            Ok(())
        }
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
        let cluster_members = peers
            .checked_add(1)
            .expect(&format!("unable to support {} cluster members", peers));
        (cluster_members >> 1) + 1
    }

    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use persistent_log::mem::MemLog;
    use state_machine::null::NullStateMachine;
    use std::collections::VecDeque;

    use log::{self, LogLevel, LogLevelFilter, LogMetadata, LogRecord};

    struct TestLogger;

    impl log::Log for TestLogger {
        fn enabled(&self, metadata: &LogMetadata) -> bool {
            metadata.level() <= LogLevel::Trace
        }

        fn log(&self, record: &LogRecord) {
            if self.enabled(record.metadata()) {
                println!("{} - {}", record.level(), record.args());
            }
        }
    }

    type TestPeer = Consensus<MemLog, NullStateMachine, CollectHandler>;

    #[derive(Debug)]
    struct TestCluster {
        pub peers: HashMap<ServerId, TestPeer>,
    }

    impl TestCluster {
        fn new(size: usize) -> Self {
            log::set_logger(|max_log_level| {
                max_log_level.set(LogLevelFilter::Trace);
                Box::new(TestLogger)
            }).unwrap_or_else(|_| ());
            let ids: Vec<ServerId> = (0..size).map(|i| (i as u64).into()).collect();
            let mut peers = HashMap::with_capacity(size);
            for i in 0..size {
                let mut ids = ids.clone();
                ids.remove(i); // remove self
                let id = ServerId(i as u64);
                let store = MemLog::new();
                let handler = CollectHandler::new();
                let mut consensus = Consensus::new(id, ids, store, NullStateMachine, handler);
                consensus.init();
                peers.insert(id, consensus);
            }
            Self { peers }
        }

        // Applies the actions to the consensus peers (recursively applying any resulting
        // actions), and TODO: returns any client messages.
        fn apply_peer_messages(&mut self) -> HashMap<ServerId, HashSet<ConsensusTimeout>> {
            scoped_trace!("apply peer messages");
            let mut queue: VecDeque<(ServerId, ServerId, PeerMessage)> = VecDeque::new();
            let mut timeouts: HashMap<ServerId, HashSet<ConsensusTimeout>> = HashMap::new();
            // TODO timeouts
            for (peer, mut consensus) in self.peers.iter_mut() {
                for (to, messages) in consensus.handler.peer_messages.drain() {
                    for message in messages.into_iter() {
                        queue.push_back((peer.clone(), to, message));
                    }
                }

                let mut entry = timeouts.entry(peer.clone()).or_insert(HashSet::new());

                for timeout in consensus.handler.timeouts.clone() {
                    if let ConsensusTimeout::Election = timeout {
                        entry.insert(timeout);
                    }
                }

                consensus.handler.clear();
            }
            scoped_trace!("Queue: {:?}", queue);
            while let Some((from, to, message)) = queue.pop_front() {
                let mut peer_consensus = self.peers.get_mut(&to).unwrap();
                peer_consensus.apply_peer_message(from, message);
                for (to, messages) in peer_consensus.handler.peer_messages.drain() {
                    for message in messages.into_iter() {
                        queue.push_back((peer_consensus.id.clone(), to, message));
                    }
                }

                scoped_trace!("Queue: {:?}", queue);
                let mut entry = timeouts
                    .entry(peer_consensus.id.clone())
                    .or_insert(HashSet::new());
                for timeout in peer_consensus.handler.timeouts.clone() {
                    if let ConsensusTimeout::Election = timeout {
                        entry.insert(timeout);
                    }
                }

                peer_consensus.handler.clear();
            }
            timeouts
        }

        fn into_peers(self) -> HashMap<ServerId, TestPeer> {
            self.peers
        }

        // Elect `leader` as the leader of a cluster with the provided followers.
        // The leader and the followers must be in the same term.
        fn elect_leader(&mut self, leader: ServerId) {
            {
                let leader_peer = self.peers.get_mut(&leader).unwrap();
                leader_peer.apply_timeout(ConsensusTimeout::Election);
            }
            //let client_messages = apply_actions(leader, actions, peers);
            //let client_messages = self.apply_peer_messages();
            self.apply_peer_messages();
            // TODO client messages
            // assert!(client_messages.is_empty());
            assert!(self.peers[&leader].is_leader());
        }
    }

    // Tests the majority function.
    #[test]
    fn test_majority() {
        let peers = TestCluster::new(1).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(1, majority);

        let peers = TestCluster::new(2).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(2, majority);

        let peers = TestCluster::new(3).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(2, majority);
        let peers = TestCluster::new(4).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(3, majority);
    }

    // Tests that a consensus state machine with no peers will transitition immediately to the
    // leader state upon the first election timeout.
    #[test]
    fn test_solitary_consensus_transition_to_leader() {
        let (_, mut peer) = TestCluster::new(1).into_peers().into_iter().next().unwrap();
        assert!(peer.is_follower());

        peer.apply_timeout(ConsensusTimeout::Election);
        assert!(peer.is_leader());
        assert!(peer.handler.peer_messages.is_empty());
        assert!(peer.handler.client_messages.is_empty());
        assert!(peer.handler.timeouts.is_empty());
    }

    /// A simple election test over multiple group sizes.
    #[test]
    fn test_election() {
        for group_size in 1..10 {
            scoped_trace!("Group size: {}", group_size);
            let mut cluster = TestCluster::new(group_size);
            let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
            let leader = &peer_ids[0];
            cluster.elect_leader(leader.clone());
            assert!(cluster.peers[leader].is_leader());
            for follower in peer_ids.iter().skip(1) {
                assert!(cluster.peers[follower].is_follower());
            }
        }
    }

    /// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
    /// timeout, and in response sends an AppendEntries message to the follower.
    /// The follower in turn resets its election timout, and replies to the
    /// leader.
    #[test]
    fn test_heartbeat() {
        let mut cluster = TestCluster::new(2);
        let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
        let leader_id = &peer_ids[0];
        let follower_id = &peer_ids[1];
        cluster.elect_leader(leader_id.clone());

        let peer_message = {
            // Leader pings with a heartbeat timeout
            let leader = cluster.peers.get_mut(&leader_id).unwrap();
            leader.apply_timeout(ConsensusTimeout::Heartbeat(follower_id.clone()));

            let (to, peer_messages) = leader.handler.peer_messages.iter().next().unwrap();
            assert_eq!(*to, follower_id.clone());
            peer_messages[0].clone()
        };


        // Follower responds
        let follower_response = {
            let follower = cluster.peers.get_mut(&follower_id).unwrap();

            // Ensure follower has set it's election timeout
            follower.apply_peer_message(leader_id.clone(), peer_message);
            assert_eq!(follower.handler.timeouts[0], ConsensusTimeout::Election);

            let (to, peer_messages) = follower.handler.peer_messages.iter().next().unwrap();
            assert_eq!(*to, leader_id.clone());
            peer_messages[0].clone()
        };

        // Leader applies and sends back a heartbeat to establish leadership.
        let leader = cluster.peers.get_mut(&leader_id).unwrap();
        leader
            .apply_peer_message(follower_id.clone(), follower_response)
            .unwrap();
        let heartbeat_timeout = leader.handler.timeouts.pop().unwrap();
        assert_eq!(
            heartbeat_timeout,
            ConsensusTimeout::Heartbeat(follower_id.clone())
        );
    }

    ///// Emulates a slow heartbeat message in a two-node cluster.
    /////
    ///// The initial leader (Consensus 0) sends a heartbeat, but before it is received by the follower
    ///// (Consensus 1), Consensus 1's election timeout fires. Consensus 1 transitions to candidate state
    ///// and attempts to send a RequestVote to Consensus 0. When the partition is fixed, the
    ///// RequestVote should prompt Consensus 0 to step down. Consensus 1 should send a stale term
    ///// message in response to the heartbeat from Consensus 0.
    //#[test]
    //fn test_slow_heartbeat() {
    //setup_test!("test_heartbeat");
    //let mut peers = new_cluster(2);
    //let peer_ids: Vec<ServerId> = peers.keys().cloned().collect();
    //let peer_0 = &peer_ids[0];
    //let peer_1 = &peer_ids[1];
    //elect_leader(peer_0.clone(), &mut peers);

    //let mut peer_0_actions = Actions::new();
    //peers.get_mut(peer_0).unwrap().apply_timeout(
    //ConsensusTimeout::Heartbeat(
    //*peer_1,
    //),
    //&mut peer_0_actions,
    //);
    //assert!(peers[peer_0].is_leader());

    //let mut peer_1_actions = Actions::new();
    //peers.get_mut(peer_1).unwrap().apply_timeout(
    //ConsensusTimeout::Election,
    //&mut peer_1_actions,
    //);
    //assert!(peers[peer_1].is_candidate());

    //// Apply candidate messages.
    //assert!(apply_actions(*peer_1, peer_1_actions, &mut peers).is_empty());
    //assert!(peers[peer_0].is_follower());
    //assert!(peers[peer_1].is_leader());

    //// Apply stale heartbeat.
    //assert!(apply_actions(*peer_0, peer_0_actions, &mut peers).is_empty());
    //assert!(peers[peer_0].is_follower());
    //assert!(peers[peer_1].is_leader());
    //}

    ///// Tests that a client proposal is correctly replicated to peers, and the client is notified
    ///// of the success.
    //#[test]
    //fn test_proposal() {
    //setup_test!("test_proposal");
    //// Test various sizes.
    //for i in 1..7 {
    //scoped_debug!("testing size {} cluster", i);
    //let mut peers = new_cluster(i);
    //let peer_ids: Vec<ServerId> = peers.keys().cloned().collect();
    //let leader = peer_ids[0];
    //elect_leader(leader, &mut peers);

    //let value: &[u8] = b"foo";
    //let proposal = into_reader(&messages::proposal_request(value));
    //let mut actions = Actions::new();

    //let client = ClientId::new();

    //peers.get_mut(&leader).unwrap().apply_client_message(
    //client,
    //&proposal,
    //&mut actions,
    //);

    //let client_messages = apply_actions(leader, actions, &mut peers);
    //assert_eq!(1, client_messages.len());
    //for peer in peers.values() {
    //assert_eq!((Term(1), value), peer.log.entry(LogIndex(1)).unwrap());
    //}
    //}
    //}

    //#[test]
    //// Verify that out-of-order appends don't lead to the log tail being
    //// dropped. See https://github.com/ktoso/akka-raft/issues/66; it's
    //// not actually something that can happen in practice with TCP, but
    //// wise to avoid it altogether.
    //fn test_append_reorder() {
    //setup_test!("test_append_reorder");
    //let mut peers = new_cluster(2);
    //let peer_ids: Vec<ServerId> = peers.keys().cloned().collect();
    //let mut actions = Actions::new();
    //let mut follower = peers.get_mut(&peer_ids[0]).unwrap();
    //let value: &[u8] = b"foo";
    //let entries = vec![(Term(1), value), (Term(1), value)];
    //let msg1 = into_reader(&*messages::append_entries_request(
    //Term(1),
    //LogIndex(0),
    //Term(0),
    //&entries,
    //LogIndex(0),
    //));
    //let msg2 = into_reader(&*messages::append_entries_request(
    //Term(1),
    //LogIndex(0),
    //Term(0),
    //&entries[0..1],
    //LogIndex(0),
    //));
    //follower.apply_peer_message(peer_ids[1], &msg1, &mut actions);
    //follower.apply_peer_message(peer_ids[1], &msg2, &mut actions);

    //assert_eq!((Term(1), value), follower.log.entry(LogIndex(1)).unwrap());
    //assert_eq!((Term(1), value), follower.log.entry(LogIndex(2)).unwrap());
    //}

    //#[bench]
    //fn bench_proposal_1(b: &mut test::Bencher) {
    //bench_n(b, 1)
    //}

    //#[bench]
    //fn bench_proposal_3(b: &mut test::Bencher) {
    //bench_n(b, 3)
    //}

    //#[bench]
    //fn bench_proposal_5(b: &mut test::Bencher) {
    //bench_n(b, 5)
    //}

    //fn bench_n(b: &mut test::Bencher, size: u64) {
    //let mut peers = new_cluster(size);
    //let peer_ids: Vec<ServerId> = peers.keys().cloned().collect();
    //let leader = peer_ids[0];
    //elect_leader(leader, &mut peers);

    //let value: &[u8] = b"foo";
    //let proposal = into_reader(&messages::proposal_request(value));
    //let client = ClientId::new();


    //b.iter(|| {
    //let mut actions = Actions::new();
    //peers.get_mut(&leader).unwrap().apply_client_message(
    //client,
    //&proposal,
    //&mut actions,
    //);

    //let client_messages = apply_actions(leader, actions, &mut peers);
    //assert_eq!(1, client_messages.len());
    //});
    //}
    //}
}
