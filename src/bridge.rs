/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use crate::zmq_clients::zmq_consumer::ZmqConsumer;
use crate::redis_clients::redis_producer::RedisProducer;


pub struct FoamBridge {
    schema: apache_avro::Schema,
    zmq_endpoint: String,
    zmq_socket: zmq::SocketType,
    redis_host: String,
    redis_port: i32,
}

impl FoamBridge {

    pub fn new(schema: apache_avro::Schema,
               zmq_endpoint: String,
               zmq_sock: &str,
               redis_host: String,
               redis_port: i32) -> FoamBridge {
        let zmq_socket = match zmq_sock.to_ascii_lowercase().as_str() {
            "pull" => zmq::SocketType::PULL,
            "sub" => zmq::SocketType::SUB,
            _ => panic!("Unknown ZeroMQ socket type string: {:?}", zmq_sock),
        };

        FoamBridge {
            schema,
            zmq_endpoint,
            zmq_socket,
            redis_host,
            redis_port
        }
    }

    pub fn start(&self) {
        let consumer = ZmqConsumer::new(&self.zmq_endpoint, self.zmq_socket, &self.schema);

        let mut producer = RedisProducer::new(&self.redis_host, self.redis_port);

        let stream = "";
        loop {
            let data = consumer.next();
            producer.produce(stream, data, &self.schema);
        }
    }
}