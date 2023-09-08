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
    use std::thread;
    use std::sync::{Arc, Mutex};

    use apache_avro::types::Value;

    use crate::encoder::{create_encoder, Encoder};
    use crate::utils::Decoded;
    use crate::zmq_consumer::ZmqConsumer;

    struct ZmqProducer {
        ctx: zmq::Context,
        socket: Arc<Mutex<zmq::Socket>>,
        encoder: Option<Box<dyn Encoder>>,
    }

    impl ZmqProducer {
        pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
            let ctx = zmq::Context::new();
            let socket = Arc::new(Mutex::new(ctx.socket(sock_type).unwrap()));

            socket.lock().unwrap().bind(endpoint).unwrap();
            ZmqProducer {
                ctx,
                socket,
                encoder: None,
            }
        }

        pub fn set_encoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
            self.encoder = Some(create_encoder(name, schema));
        }

        pub fn start(&self) {
            let socket = Arc::clone(&self.socket);

            let mut records = Vec::new();
            for i in 0..3 {
                let data = Decoded::from([("index".to_string(), Value::Int(i))]);
                let bytes = self.encoder.as_ref().unwrap().pack(&data).unwrap();
                records.push(bytes);
            }

            thread::spawn(move|| {
                for bytes in records {
                    socket.lock().unwrap().send(&bytes, 0).unwrap();
                }
            });
        }
    }

    #[test]
    fn test_zmq_consumer() {
        let raw_schema = r#"
            {
                "namespace": "testcase",
                "type": "record",
                "name": "raw",
                "fields": [
                    {
                      "name": "index",
                      "type": "int"
                    }
                ]
            }"#;
        let schema: serde_json::Value = serde_json::from_str(raw_schema).unwrap();

        let mut producer = ZmqProducer::new("tcp://*:5555", zmq::SocketType::PUSH);
        producer.set_encoder("avro", Some(&schema));

        let mut consumer = ZmqConsumer::new("tcp://localhost:5555", zmq::SocketType::PULL);
        consumer.set_decoder("avro", Some(&schema));

        producer.start();
        for i in 0..3 {
            let ret = consumer.consume().unwrap();
            assert_eq!(ret.len(), 1);
            assert_eq!(ret[0], Decoded::from([("index".to_string(), Value::Int(i))]));
        }
    }
}