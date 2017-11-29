use error::*;
use common::*;
use state::{ConsensusState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use persistent_log::Log;

use messages_capnp::{connection_preamble, preamble_response, ConnectionResponse};
use messages::preamble_response;

use std::{ops, fmt, net, sync};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;

use uuid::Uuid;
use tokio_core::reactor::{Core, Timeout, Handle};
use tokio_core::net::TcpListener;

use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_timer::Timer;
use futures::{Future, Stream, Sink, IntoFuture};
use futures::future::{ok, loop_fn, Loop, err, result};
use consensus::{ConsensusService, SharedConsensus};
use consensus::PeerStatus;

//use capnp::serialize::OwnedSegments;
use capnp_futures::serialize::OwnedSegments;
use capnp::message::{DEFAULT_READER_OPTIONS, Reader, ReaderOptions};
use futures::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use capnp_futures::serialize::{read_message, write_message, Transport};
use smartconn::TimedTransport;

/// A future that processes follower logic
pub struct FollowerFuture<L, M> {
    consensus: SharedConsensus<L, M>,
    handle: Handle,
    transport: Transport<TcpStream, RaftEgress>,
}

impl<L, M> FollowerFuture<L, M> {
    pub fn new(
        transport: Transport<TcpStream, RaftEgress>,
        consensus: SharedConsensus<L, M>,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            transport,
            consensus,
            handle,
        }
    }
}
impl<L, M> IntoFuture for FollowerFuture<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = (); // TODO FollowerFutureError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            transport,
            consensus,
            handle,
        } = self;
        let timer = Timeout::new(Duration::from_millis(TERM_TIMEOUT), &handle);
        let reader = transport.for_each(move |message|{
            // TODO check leader ID

        })
        let future = timer.select2(reader)
            .;
        // let future = transport.and_then(|frames| {
        //frames
        //.for_each(|_message| {
        //println!("CLIENT got MESSAGE");
        //Ok(())
        //})
        //.map_err(|e| println!("CLIENT ERR3: {:?}", e))
        // });
        let future = ok(());
        Box::new(future)
    }
}

/// A future that processes follower logic
pub struct CandidateFuture<L, M> {
    consensus: SharedConsensus<L, M>,
    handle: Handle,
    transport: Transport<TcpStream, RaftEgress>,
}

impl<L, M> CandidateFuture<L, M> {
    pub fn new(
        transport: Transport<TcpStream, RaftEgress>,
        peer_addr: SocketAddr,
        consensus: SharedConsensus<L, M>,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            transport,
            consensus,
            handle,
        }
    }
}
impl<L, M> IntoFuture for CandidateFuture<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = (); // TODO CandidateFutureError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            transport,
            consensus,
            handle,
        } = self;
        let future = ok(());
        Box::new(future)
    }
}

/// A future that processes follower logic
pub struct LeaderFuture<L, M> {
    consensus: SharedConsensus<L, M>,
    handle: Handle,
    transport: Transport<TcpStream, RaftEgress>,
}

impl<L, M> LeaderFuture<L, M> {
    pub fn new(
        transport: Transport<TcpStream, RaftEgress>,
        consensus: SharedConsensus<L, M>,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            transport,
            consensus,
            handle,
        }
    }
}
impl<L, M> IntoFuture for LeaderFuture<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = (); // TODO LeaderFutureError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            transport,
            consensus,
            handle,
        } = self;
        let future = ok(());
        Box::new(future)
    }
}
