/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::any::Any;
use std::collections::HashMap;
use apache_avro::{AvroResult, Reader};

pub struct ZmqConsumer<'a> {
    ctx: zmq::Context,
    socket: zmq::Socket,
    schema: &'a apache_avro::Schema
}

impl<'a> ZmqConsumer<'a> {
    pub fn new(endpoint: &str, sock_type: zmq::SocketType, schema: &'a apache_avro::Schema) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).unwrap();
        socket.connect(endpoint).unwrap();

        if sock_type == zmq::SocketType::SUB {
            socket.set_subscribe(b"").unwrap();
        }

        ZmqConsumer {
            ctx,
            socket,
            schema
        }
    }

    pub fn next(&self) -> HashMap<String, dyn Any> {
        let data = self.socket.recv_bytes(0).unwrap();

        let reader = Reader::new(&data[..]).unwrap();
        for value in reader {
            println!("{:?}", value.unwrap());
        }
    }
}