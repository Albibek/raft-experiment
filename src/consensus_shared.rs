use std::{cmp, fmt};
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use capnp::message::{Builder, HeapAllocator, Reader, ReaderSegments};
use rand::{self, Rng};

use {LogIndex, Term, ServerId, ClientId, messages};
use messages_capnp::{append_entries_request, append_entries_response, client_request,
                     proposal_request, query_request, message, request_vote_request,
                     request_vote_response};
use state::{ConsensusState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use persistent_log::Log;

use futures::{Future, Stream, Sink, IntoFuture};
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio_service::Service;

use error::*;
use consensus_lib::*;
use consensus_types::*;

use {RaftEgress, RaftIngress};

#[derive(Clone)]
pub enum PeerStatus {
    Connecting,
    Connected(UnboundedSender<RaftEgress>),
}

impl PartialEq for PeerStatus {
    fn eq(&self, other: &PeerStatus) -> bool {
        match (self, other) {
            (&PeerStatus::Connecting, &PeerStatus::Connecting) => true,
            (&PeerStatus::Connected(_), &PeerStatus::Connected(_)) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for PeerStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &PeerStatus::Connecting => write!(fmt, "PeerStatus::Connecting"),
            &PeerStatus::Connected(_) => write!(fmt, "PeerStatus::Connected"),
        }
    }
}



impl From<RequestVoteResponse> for RaftEgress {
    fn from(r: RequestVoteResponse) -> RaftEgress {
        RaftEgress(match r {
            RequestVoteResponse::StaleTerm(term) => messages::request_vote_response_stale_term(
                term,
            ),
            RequestVoteResponse::InconsistentLog(term) => {
                messages::request_vote_response_inconsistent_log(term)
            }
            RequestVoteResponse::Granted(term) => messages::request_vote_response_granted(term),
            RequestVoteResponse::AlreadyVoted(term) => {
                messages::request_vote_response_already_voted(term)
            }
        })
    }
}

//impl From<ConsensusResponse> for RaftEgress {
//fn from(r: ConsensusResponse) -> RaftEgress {
//let message = match r {
//ConsensusResponse::StaleTerm(term) => {
//messages::append_entries_response_stale_term(term)
//}
//ConsensusResponse::InconsistentPrevEntry(cur, log) => {
//messages::append_entries_response_inconsistent_prev_entry(cur, log)
//}
//ConsensusResponse::AppendEntriesSuccess(cur, latest) => {
//messages::append_entries_response_success(cur, latest)
//}
//_ => unimplemented!(),
//};
//RaftEgress(message)
//}
//}

pub struct SharedConsensus<L, M> {
    inner: Arc<RwLock<Consensus<L, M>>>,
}

impl<L, M> SharedConsensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    pub fn new(
        id: ServerId,
        peers: HashMap<ServerId, SocketAddr>,
        log: L,
        state_machine: M,
    ) -> Self {

        let consensus = Consensus::new(id, peers, log, state_machine);
        consensus.init();

        Self { inner: Arc::new(RwLock::new(consensus)) }

    }

    pub fn apply_peer_message<S>(&self, from: ServerId, message: &Reader<S>)
    where
        S: ReaderSegments,
    {

        let mut consensus = self.inner.write().unwrap();
        let reader = message
            .get_root::<message::Reader>()
            .unwrap()
            .which()
            .unwrap();
        match reader {
            message::Which::AppendEntriesRequest(Ok(request)) => {
                //FIXME process return values from these
                consensus.append_entries_request(from, request);
            }
            message::Which::AppendEntriesResponse(Ok(response)) => {
                consensus.append_entries_response(from, response);
            }
            message::Which::RequestVoteRequest(Ok(request)) => {
                consensus.request_vote_request(from, request);
            }
            message::Which::RequestVoteResponse(Ok(response)) => {
                consensus.request_vote_response(from, response);
            }
            // FIXME return error correctly
            _ => unimplemented!(),
            //_ => panic!("cannot handle message"),
        }
    }

    //pub fn peer_connection_reset(&self, peer: ServerId, addr: SocketAddr, actions: &mut Actions) {
    //let mut consensus = self.inner.write().unwrap();
    //consensus.peer_connection_reset(peer, addr, actions)
    //}

    pub fn transition(&self, state: ConsensusState) {
        let mut consensus = self.inner.write().unwrap();
        consensus.state = state;
    }

    pub fn get_state(&self) -> ConsensusState {
        let mut consensus = self.inner.read().unwrap();
        consensus.state.clone()
    }


    /// returns the result of peer status update operation,
    /// true returned if this particular server id is inserted/updated to connected state
    pub fn set_peer_status(&self, id: ServerId, status: Option<PeerStatus>) -> bool {
        let mut consensus = self.inner.write().unwrap();
        match status {
            Some(status) => {
                use std::collections::hash_map::Entry;
                match consensus.peer_status.entry(id) {
                    Entry::Vacant(entry) => {
                        entry.insert(status);
                        true
                    }
                    Entry::Occupied(mut entry) => {
                        match entry.get_mut() {
                            // there was somebody connecting, but it didn't connect yet
                            e @ &mut PeerStatus::Connecting => {
                                if let PeerStatus::Connected(_) = status {
                                    *e = status;
                                    true
                                } else {
                                    false
                                }
                            }
                            // ther is already a connection
                            //e @ &mut PeerStatus::Connected => false,
                            // currently the latter case is the only one where we should update
                            _ => false,
                        }
                    }
                }
            }
            None => consensus.peer_status.remove(&id).is_some(),
        }
    }
}

impl<L, M> Clone for SharedConsensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

pub struct ConsensusService<L, M>
where
    L: Log,
    M: StateMachine,
{
    consensus: SharedConsensus<L, M>,
    id: ServerId,
}

impl<L, M> ConsensusService<L, M>
where
    L: Log,
    M: StateMachine,
{
    pub fn new(consensus: SharedConsensus<L, M>, id: ServerId) -> Self {
        Self { consensus, id }
    }
}

impl<L, M> Service for ConsensusService<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Request = RaftIngress;
    type Response = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;
    fn call(&self, req: Self::Request) -> Self::Future {
        println!("CALL");
        //let mut actions = Actions::new();
        use std::borrow::Borrow;
        let message = self.consensus.apply_peer_message(
            self.id,
            req.borrow(),
            //&mut actions,
        );
        //println!("AC: {:?}", actions);
        Box::new(::futures::future::ok(message).then(|_| Ok(())))
    }
}
