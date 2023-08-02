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
    schema_: serde_json::Value,
}

impl FoamBridge {

    pub fn new(schema: serde_json::Value) -> FoamBridge {
        FoamBridge {
            schema_: schema
        }
    }

    pub fn start(&self) {

        let consumer = ZmqConsumer::new(zmq::PULL);
        consumer.connect("tcp://127.0.0.1:45454").unwrap();

        let producer = RedisProducer::new();

        loop {
            let data = consumer.next();
        }
    }
}