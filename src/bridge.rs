/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
pub mod zmq_consumer;
pub mod redis_producer;

use crate::bridge::zmq_consumer::ZmqConsumer;
use crate::bridge::redis_producer::RedisProducer;


pub struct FoamBridge {
    schema: serde_json::Value,
    zmq_endpoint: String,
    zmq_socket: zmq::SocketType,
}

impl FoamBridge {

    pub fn new(schema: serde_json::Value,
               zmq_endpoint: String,
               zmq_sock: &str) -> FoamBridge {
        let zmq_socket = match zmq_sock.to_ascii_lowercase().as_str() {
            "pull" => zmq::SocketType::PULL,
            "sub" => zmq::SocketType::SUB,
            _ => panic!("Unknown ZeroMQ socket type string: {:?}", zmq_sock),
        };

        FoamBridge {
            schema,
            zmq_endpoint,
            zmq_socket
        }
    }

    pub fn start(&self) {

        let consumer = ZmqConsumer::new(&self.zmq_endpoint, self.zmq_socket);

        // let producer = RedisProducer::new();

        loop {
            let data = consumer.next();
            println!("{:?}", data.as_ptr());
        }
    }
}