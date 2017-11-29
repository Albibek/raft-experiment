use error::*;
use common::*;
use state::{ConsensusState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use persistent_log::Log;

use messages_capnp::{connection_preamble, preamble_response, ConnectionResponse};
use messages::{preamble_response, server_connection_preamble};

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
use futures::sync::mpsc::{self, UnboundedSender, UnboundedReceiver, unbounded};
use capnp_futures::serialize::{read_message, write_message, Transport};
use smartconn::TimedTransport;


/// Raft server processing connections
pub struct Server<L, M> {
    consensus: SharedConsensus<L, M>,
    listen: SocketAddr,
    id: ServerId,
    handle: Handle,
}

impl<L, M> Server<L, M> {
    pub fn new(
        id: ServerId,
        listen: SocketAddr,
        consensus: SharedConsensus<L, M>,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            consensus,
            listen,
            id,
            handle,
        }
    }
}

impl<L, M> IntoFuture for Server<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = (); // TODO ServerError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            consensus,
            listen,
            id,
            handle,
        } = self;

        let listener = TcpListener::bind(&listen, &handle).unwrap();
        let future = listener
            .incoming()
            .map_err(|e| println!("Accept error: {:?}", e))
            .for_each(move |(socket, peer_addr)| {
                let transport: Transport<_, RaftEgress> =
                    Transport::new(socket, DEFAULT_READER_OPTIONS);
                ServerHandshake::new(transport, peer_addr, consensus.clone(), &handle.clone())
                    .into_future()
                    .then(|_| Ok(())) // shun connection error to not affect server
            });
        Box::new(future)
    }
}


/// A future that processes single server connection
pub struct ServerHandshake<L, M> {
    consensus: SharedConsensus<L, M>,
    handle: Handle,
    transport: Transport<TcpStream, RaftEgress>,
    peer_addr: SocketAddr,
}

impl<L, M> ServerHandshake<L, M> {
    pub fn new(
        transport: Transport<TcpStream, RaftEgress>,
        peer_addr: SocketAddr,
        consensus: SharedConsensus<L, M>,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            transport,
            peer_addr,
            consensus,
            handle,
        }
    }
}

impl<L, M> IntoFuture for ServerHandshake<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = (); // TODO ServerHandshakeError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            transport,
            peer_addr,
            consensus,
            handle,
        } = self;

        let hs_consensus = consensus.clone();
        let fhandle = handle.clone();
        let handshake = transport
            .map_err(|e| println!("reading error: {:?}", e))
            .into_future()
            .map_err(|e| println!("decoding error"))
            .and_then(move |(message, frames)| {
                if message.is_none() {
                    // connection closed with no message received
                    println!("NO MESSAGES");
                    return err(());
                }
                let message = message.unwrap();
                let preamble = message.get_root::<connection_preamble::Reader>().unwrap();

                match preamble.get_id().which().unwrap() {
                    connection_preamble::id::Which::Server(peer) => {
                        let sid = ServerId(peer.unwrap().get_id());
                        let (tx, rx) = unbounded();
                        let peer_set =
                            hs_consensus.set_peer_status(sid, Some(PeerStatus::Connected(tx)));
                        if peer_set {
                            ok((sid, peer_addr, frames, rx))
                        } else {
                            let message = preamble_response(ConnectionResponse::AlreadyConnected);
                            handle.spawn(frames.send(RaftEgress(message)).then(|_| Ok(())));
                            err(())
                        }
                    }
                    connection_preamble::id::Which::Client(Ok(id)) => {
                        let message = preamble_response(ConnectionResponse::ClientOnPeer);
                        handle.spawn(frames.send(RaftEgress(message)).then(|_| Ok(())));
                        return err(());
                    }
                    _ => {
                        let message = preamble_response(ConnectionResponse::PreambleExpected);
                        handle.spawn(frames.send(RaftEgress(message)).then(|_| Ok(())));
                        return err(());
                    }
                }
            });

        // if server_id got right and there was no previous connections
        let future = handshake
            .map_err(|_| println!("handshake error"))
            .and_then(move |(sid, peer_addr, frames, rx)| {
                // Send the OK as a response
                let message = preamble_response(ConnectionResponse::Ok);
                frames
                    .send(RaftEgress(message))
                    .map_err(|e| {
                        println!("sending response OK error {:?}", e);
                    })
                    .map(move |frames| (sid, peer_addr, frames, rx))
            })
            .and_then(move |(sid, peer_addr, frames, rx)| {
                // for each new packet spawn a service
                let service = ConsensusService::new(consensus.clone(), sid);
                println!("Client {:?} connected from {:?}", sid, peer_addr);
                frames.for_each(move |message| {
                    service
                        .call(Arc::new(message))
                        .map_err(|_| println!("SERVICE ERR"))
                        .and_then(|messages| {
                            println!("AC: {:?}", messages);
                            Ok(())
                        })
                })
                // TODO report disconnect here - i.e. after
            });
        Box::new(future)
    }
}

/// A future that processes single server connection
pub struct ClientHandshake<L, M> {
    consensus: SharedConsensus<L, M>,
    id: ServerId,
    handle: Handle,
    addr: SocketAddr,
    timer: Timer,
}

impl<L, M> ClientHandshake<L, M> {
    pub fn new(
        id: ServerId,
        addr: SocketAddr,
        consensus: SharedConsensus<L, M>,
        timer: Timer,
        handle: &Handle,
    ) -> Self {
        let handle = handle.clone();
        Self {
            id,
            addr,
            consensus,
            handle,
            timer,
        }
    }
}

impl<L, M> IntoFuture for ClientHandshake<L, M>
where
    L: Log,
    M: StateMachine,
{
    type Item = Transport<TcpStream, RaftEgress>;
    type Error = (); // TODO ClientHandshakeError
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            addr,
            timer,
            consensus,
            handle,
        } = self;


        let handshake = move || {
            let addr = addr.clone();
            move |stream: TcpStream| {
                let cl_addr = stream.local_addr().unwrap();
                let transport = Transport::new(stream, DEFAULT_READER_OPTIONS);

                let message = server_connection_preamble(id, &addr);
                transport
                    .send(RaftEgress(message))
                    .map_err(|e| {
                        println!("CLIENT ERR2: {:?}", e);
                    })
                    .and_then(|transport| {
                        transport
                            .into_future()
                            .map_err(|_| println!("decode error"))
                            .map(move |(message, transport)| {
                                if message.is_none() {
                                    // connection closed with no message received
                                    println!("NO MESSAGES");
                                    return err(());
                                }
                                let message = message.unwrap();

                                match message.get_root::<preamble_response::Reader>() {
                                    Ok(m) => {
                                        if m.get_response().unwrap() != ConnectionResponse::Ok {
                                            err(())
                                        } else {

                                            println!("OK");
                                            ok(transport)
                                        }
                                    }
                                    Err(_) => err(()),
                                }
                            })
                    })
            }
        };

        let connect = loop_fn(
            (addr.clone(), handle.clone(), timer.clone()),
            move |(addr, handle, timer)| {
                let timer1 = timer.clone();
                let erraddr = addr.clone();
                let conn = TcpStream::connect(&addr, &handle)
                    .map_err(move |e| println!("conn error for {:?}: {:?}", erraddr, e))
                    .and_then(handshake())
                    .or_else(move |_| {
                        println!("connection error");
                        timer1.sleep(Duration::from_millis(1000)).then(|_| Err(()))
                    });
                timer.timeout(conn, Duration::from_millis(3000)).then(
                    move |res| match res {
                        Err(_) => Ok(Loop::Continue((addr.clone(), handle.clone(), timer))),
                        Ok(conn) => Ok::<_, ()>(Loop::Break(conn)),
                    },
                )
            },
        ).and_then(|res| res);

        Box::new(connect)
    }
}
