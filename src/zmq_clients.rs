/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use crate::decoder::{create_decoder, Decoder};
use crate::encoder::{create_encoder, Encoder};
use crate::schema::{Decoded, Encoded};
use crate::error::{FcResult};

pub struct ZmqConsumer {
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
            socket,
            decoder: None
        }
    }

    pub fn set_decoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.decoder = Some(create_decoder(name, schema));
    }

    pub fn consume(&self) -> FcResult<Decoded> {
        let bytes: Encoded = self.socket.recv_bytes(0)?;
        Ok(self.decoder.as_ref().unwrap().unpack(&bytes)?.into_iter().next().unwrap())
    }
}

pub struct ZmqProducer {
    socket: zmq::Socket,
    encoder: Option<Box<dyn Encoder>>,
}

impl ZmqProducer {
    pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).expect("Error in creating zmq socket");
        socket.bind(endpoint).unwrap_or_else(
            |_| panic!("Error in binding to endpoint: {}", endpoint));

        ZmqProducer {
            socket,
            encoder: None,
        }
    }

    pub fn set_encoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.encoder = Some(create_encoder(name, schema));
    }

    pub fn produce(&self, data: &[Decoded]) -> FcResult<()> {
        data.into_iter().map(|x| {
            let bytes = self.encoder.as_ref().unwrap().pack(&x).unwrap();
            let ack = self.socket.send(bytes, 0)?;
            Ok(ack)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::types::Value;

    use crate::schema::Decoded;
    use crate::zmq_clients::{ZmqConsumer, ZmqProducer};

    #[test]
    fn test_zmq_consumer_and_producer() {
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
        let json_schema: serde_json::Value = serde_json::from_str(raw_schema).unwrap();

        let mut producer = ZmqProducer::new("tcp://*:5555", zmq::SocketType::PUSH);
        producer.set_encoder("avro", Some(&json_schema));

        let mut consumer = ZmqConsumer::new("tcp://localhost:5555", zmq::SocketType::PULL);
        consumer.set_decoder("avro", Some(&json_schema));

        let mut items = Vec::new();
        for i in 0..3 {
            let item = Decoded::from([("index".to_string(), Value::Int(i))]);
            items.push(item);
        }

        let _ = producer.produce(&items);
        for i in 0..3 {
            let ret = consumer.consume().unwrap();
            assert_eq!(ret, Decoded::from([("index".to_string(), Value::Int(i))]));
        }
    }
}