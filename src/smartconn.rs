use std::fmt;
use std::net::SocketAddr;

use tokio_core::net::{TcpStream, TcpStreamNew};
use error::*;

use std::sync::Arc;
use futures::{Stream, Poll, Sink, StartSend, Async};
use std::time::Duration;
use tokio_core::reactor::{Handle, Interval};

use {RaftEgress, RaftIngress};
use capnp_futures::serialize::{read_message, write_message, Transport};
use futures::future::Either;

use capnp::message::{DEFAULT_READER_OPTIONS, Reader};

/// Note: Timeout is not really presise in current scheme for performance reasons.
/// All the timers fire not RIGHT after duration specified but drop interval is
/// distrubuted between timeout and 2*timeout with some probability
pub struct TimedTransport {
    upstream: Transport<TcpStream, RaftEgress>,
    ticker: Interval,
    ticked: bool,
}

// TODO: pong-only option to reset timer only when pong received

impl TimedTransport {
    pub fn new(socket: TcpStream, handle: &Handle) -> Self {
        let dur = Duration::from_millis(3000);
        Self {
            upstream: Transport::new(socket, DEFAULT_READER_OPTIONS),
            ticker: Interval::new(dur, &handle).unwrap(),
            ticked: false,
        }
    }

    fn poll_timer(&mut self) -> bool {
        // the parameter to set up is coefficient K the real duration should be divided to
        // so tick would happen more often
        if self.ticker.poll().unwrap().is_ready() {
            if self.ticked {
                // Timeout already fired once without packet from/to socket
                return true;
            } else {
                // timeout came only for the first time, this doesn't mean real timeout
                // TODO: process error
                self.ticker.poll().unwrap(); // make timer return NotReady again, remembering we need it's tick again
                self.ticked = true;
            }
        }

        return false;
    }
}

// A reader part
impl Stream for TimedTransport {
    type Item = RaftIngress;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.poll_timer() {
            //return Ok(Async::Ready(None));
            return Err(RaftError::Timeout.into());
        }

        // Poll the upstream transport. `try_ready!` will bubble up errors automatically converting
        // them (wow, Rust is cool) and Async::NotReady.
        let poll = try_ready!(self.upstream.poll());
        match poll {
            Some(message) => {
                self.ticked = false;
                Ok(Async::Ready(Some(Arc::new(message))))
            }
            // none means upstream has ended, this is the same as timeout
            // i.e. we need to reconnect
            None => Err(RaftError::Timeout.into()),
            //m => Ok(Async::Ready(Some(Some(Arc::new(m))))),
        }
    }
}

// A writer part
impl Sink for TimedTransport {
    type SinkItem = RaftEgress;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        // start_send just forwards data to upstream
        let res = self.upstream.start_send(item).map_err(|e| e.into());
        try!(self.poll_complete());
        res
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.poll_timer() {
            println!("TIMEOUT");
            return Err(RaftError::Timeout.into());
        }
        self.upstream.poll_complete().map_err(|e| e.into())
    }
}
