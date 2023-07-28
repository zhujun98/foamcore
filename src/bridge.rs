/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
mod zmq_consumer;
mod redis_producer;

use std::str;

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
        let ctx = zmq::Context::new();

        let socket = ctx.socket(zmq::PULL).unwrap();
        let _ = socket.connect("tcp://127.0.0.1:45454");
        loop {
            println!("waiting for data");
            let msg = socket.recv_msg(0).unwrap();
            // println!(
            //     "Identity: {:?} Message : {}",
            //     msg[0],
            //     str::from_utf8(&msg[1]).unwrap()
            // );
        }
    }
}