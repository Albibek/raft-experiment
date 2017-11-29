#[macro_use]
extern crate wrapped_enum;
extern crate capnp;
extern crate uuid;
extern crate capnp_futures;
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_io;
extern crate tokio_timer;
extern crate rand;
extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate scoped_log;
#[macro_use]
extern crate log;

pub mod messages_capnp {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}

pub mod state_machine;
pub mod persistent_log;
pub(crate) mod common;
mod messages;
mod state;
mod error;
mod consensus;
mod backoff;
mod smartconn;
mod server;

use common::*;
use error::*;
use server::*;

use std::{ops, fmt, net, sync};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio_core::reactor::{Core, Timeout};
use tokio_core::net::TcpListener;

use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_timer::Timer;
use futures::{Future, Stream, Sink, IntoFuture};
use futures::future::{ok, loop_fn, Loop, err, result};
use consensus::{ConsensusService, SharedConsensus};

//use capnp::serialize::OwnedSegments;
use capnp_futures::serialize::OwnedSegments;
use capnp::message::{DEFAULT_READER_OPTIONS, Reader, ReaderOptions};
use futures::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use capnp_futures::serialize::{read_message, write_message, Transport};
use smartconn::TimedTransport;
use state::ConsensusState;

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
        let base_consensus = SharedConsensus::new(srvid, cons_peers.clone(), store, machine);
        let mut core = Core::new().unwrap();
        let base_handle = core.handle();

        let peer_conns = BTreeMap::<
            ServerId,
            (UnboundedSender<RaftEgress>, UnboundedReceiver<RaftEgress>),
        >::new();
        let peer_conns = Arc::new(sync::RwLock::new(peer_conns));

        let server = Server::new(srvid, addr, base_consensus.clone(), &base_handle);

        let timer = Timer::default();

        for (srvid, addr) in cons_peers.clone().into_iter().filter(|&(_, addr)| {
            addr.port() == 5000
        })
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
