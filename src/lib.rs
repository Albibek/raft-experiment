extern crate byteorder;
extern crate bytes;
extern crate capnp;
extern crate capnp_futures;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
#[macro_use]
extern crate scoped_log;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_service;
extern crate tokio_timer;
extern crate uuid;
#[macro_use]
extern crate wrapped_enum;

pub mod messages_capnp {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}

pub mod state_machine;
pub mod persistent_log;
pub(crate) mod common;
pub mod messages;
pub mod state;
mod error;
//mod consensus;
pub mod consensus_lib;
pub mod consensus_types;
pub mod consensus_shared;
mod backoff;
mod smartconn;
mod server;

use common::*;
use error::*;
use server::*;

use consensus_lib::*;
use consensus_types::*;
use consensus_shared::*;

use std::{fmt, net, ops, sync};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio_core::reactor::{Core, Timeout};
use tokio_core::net::TcpListener;

use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_timer::Timer;
use futures::{Future, IntoFuture, Sink, Stream};
//use futures::future::{ok, loop_fn, Loop, err, result};

//use capnp::serialize::OwnedSegments;
use capnp_futures::serialize::OwnedSegments;
//use capnp::message::{DEFAULT_READER_OPTIONS, Reader, ReaderOptions};
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
//use capnp_futures::serialize::{read_message, write_message, Transport};
use smartconn::TimedTransport;
//use state::ConsensusState;

fn main() {
    let peers: Vec<net::SocketAddr> = vec!["127.0.0.1:5000", "127.0.0.1:5001"]
        .into_iter()
        .map(|p| p.parse().unwrap())
        .collect();

    let mut id = 5;
    let mut cons_peers = HashMap::new();
    for addr in peers.iter().cloned() {
        id += 1;
        let srvid = ServerId::from(id);
        cons_peers.insert(srvid.into(), addr);
    }

    let mut th = Vec::new();

    let machine = state_machine::NullStateMachine;
    let store = persistent_log::MemLog::new();
    let cons_peers = cons_peers.clone();

    let srvid = 10.into();
    let addr = "127.0.0.1:5000".parse().unwrap();
    let h = ::std::thread::spawn(move || {
        let base_consensus =
            SharedConsensus::new(srvid, cons_peers.keys().cloned().collect(), store, machine);
        let mut core = Core::new().unwrap();
        let base_handle = core.handle();

        let peer_conns = BTreeMap::<
            ServerId,
            (UnboundedSender<RaftEgress>, UnboundedReceiver<RaftEgress>),
        >::new();
        let peer_conns = Arc::new(sync::RwLock::new(peer_conns));

        let server = Server::new(srvid, addr, base_consensus.clone(), &base_handle);

        let timer = Timer::default();

        for (srvid, addr) in cons_peers
            .clone()
            .into_iter()
            .filter(|&(_, addr)| addr.port() == 5000)
        {
            //FIXME: deal with correct ID
            let srvid: ServerId = 0.into();
            let client = ClientHandshake::new(
                srvid,
                addr.clone(),
                base_consensus.clone(),
                timer.clone(),
                &base_handle,
            );

            let handle = base_handle.clone();
            let consensus = base_consensus.clone();
            let follower = base_handle.spawn(
                client
                    .into_future()
                    .and_then(move |conn| {
                        let service = ConsensusService::new(consensus.clone(), srvid);
                        conn
                            .map_err(|_|println!("connection error"))// TODO: report disconnect
                            .for_each(move |message| {
                            service
                                .call(Arc::new(message))
                                .map_err(|_| println!("SERVICE ERR"))
                                .and_then(|messages| {
                                    println!("AC: {:?}", messages);
                                    Ok(())
                                })
                        })
                    })
                    .then(|_| Ok(())),
            );
        }
        core.run(server.into_future()).unwrap();
    });

    th.push(h);

    th.into_iter().map(|h| h.join()).last();
}
