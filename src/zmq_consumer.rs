/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use crate::decoder::{create_decoder, Decoder};
use crate::utils::{Decoded, Encoded, FcResult};

pub struct ZmqConsumer {
    ctx: zmq::Context,
    socket: zmq::Socket,
    decoder: Option<Box<dyn Decoder>>,
}

impl ZmqConsumer {
    pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).expect("Error in creating zmq socket");
        socket.connect(endpoint).unwrap_or_else(
            |_| panic!("Error in connecting to endpoint: {}", endpoint));

        if sock_type == zmq::SocketType::SUB {
            socket.set_subscribe(b"").unwrap();
        }

        ZmqConsumer {
            ctx,
            socket,
            decoder: None
        }
    }

    pub fn set_decoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.decoder = Some(create_decoder(name, schema));
    }

    pub fn consume(&self) -> FcResult<Vec<Decoded>> {
        let bytes: Encoded = self.socket.recv_bytes(0)?;
        self.decoder.as_ref().unwrap().unpack(&bytes)
    }
}

#[cfg(test)]
mod tests {

    // use std::thread;

    // struct ZmqProducer {
    //     ctx: zmq::Context,
    //     socket: zmq::Socket,
    // }
    //
    // impl ZmqProducer {
    //     pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
    //         let ctx = zmq::Context::new();
    //         let socket = ctx.socket(sock_type).unwrap();
    //
    //         socket.bind(endpoint).unwrap();
    //
    //         ZmqProducer {
    //             ctx,
    //             socket,
    //         }
    //     }
    //
    //     pub fn start() {
    //         thread::spawn(|| {
    //
    //         });
    //     }
    // }

    #[test]
    fn test_zmq_consumer() {

    }
}