/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use redis::{Commands};
use redis::streams::{StreamId, StreamReadReply, StreamReadOptions, StreamMaxlen};

use crate::decoder::{create_decoder, Decoder};
use crate::encoder::{create_encoder, Encoder};
use crate::schema::Decoded;
use crate::error::FcResult;

pub struct RedisProducer {
    client: redis::Client,
    maxlen: StreamMaxlen,
    encoder: Option<Box<dyn Encoder + Send>>,
}

impl RedisProducer {

    pub fn new(host: &str, port: i32) -> Self {
        let client = redis::Client::open(
            format!("redis://{}:{}", host, port.to_string())).expect(
            "Failed to open a Redis connection");

        RedisProducer {
            client,
            maxlen: StreamMaxlen::Equals(10),
            encoder: None,
        }
    }

    pub fn set_encoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.encoder = Some(create_encoder(name, schema));
    }

    /// Sets the MAXLEN parameter in XADD
    pub fn set_maxlen(&mut self, maxlen: usize) {
        self.maxlen = StreamMaxlen::Equals(maxlen);
    }

    /// Publish records to a given stream.
    pub fn produce(&mut self, records: &[Decoded], stream: &str)
            -> Vec<FcResult<String>> {
        records.into_iter().map(|x| {
            let encoded = self.encoder.as_ref().unwrap().pack(x)?;

            let entry = self.client.get_connection()?
                .xadd_maxlen(stream, self.maxlen, "*", &[("data", encoded)])?;

            Ok(entry)
        }).collect()
    }
}

pub struct RedisConsumer {
    client: redis::Client,
    block: usize,
    decoder: Option<Box<dyn Decoder + Send>>,
}

impl RedisConsumer {
    pub fn new(host: &str, port: i32) -> Self {
        let client = redis::Client::open(
            format!("redis://{}:{}", host, port.to_string())).expect(
            "Failed to open a Redis connection");

        RedisConsumer {
            client,
            block: 100,
            decoder: None,
        }
    }

    pub fn set_decoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.decoder = Some(create_decoder(name, schema));
    }

    /// Sets the BLOCK parameter in XREAD
    pub fn set_block(&mut self, block: usize) {
        self.block = block;
    }

    /// Consumes a single record from a given stream. and returns a tuple of
    /// (stream ID, decoded record).
    ///
    /// One can set ID to None to get the latest record and use the returned stream ID
    /// as the argument of the next call.
    pub fn consume(&mut self, stream: &str, id: Option<&str>) -> FcResult<(String, Decoded)> {
        const COUNT: usize = 1;
        let opts = StreamReadOptions::default().count(COUNT).block(self.block);

        let ids = match id {
            Some(s) => [s],
            None => ["$"],
        };
        let reply: StreamReadReply = self.client.get_connection()?
            .xread_options(&[stream], &ids, &opts)?;

        let keys = reply.keys;
        assert_eq!(keys.len(), 1); // one stream
        assert_eq!(keys[0].key, stream);

        let ids = keys.into_iter().next().unwrap().ids;
        assert_eq!(ids.len(), COUNT);

        let StreamId {id: sid, map: record} = ids.into_iter().next().unwrap();
        let bytes = match record.get("data").unwrap() {
            redis::Value::Data(s) => s,
            _ => panic!(),
        };

        let decoded = self.decoder.as_ref().unwrap().unpack(bytes)?;
        assert_eq!(decoded.len(), 1);

        Ok((sid, decoded.into_iter().next().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use apache_avro::types::Value;
    use crate::error::FcError;

    use crate::schema::Decoded;
    use crate::redis_clients::{RedisConsumer, RedisProducer};

    #[test]
    fn test_redis_consumer_and_producer() {
        let raw_schema = r#"
            {
                "namespace": "redis_clients_test",
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
        let stream = String::from("redis_clients_test_raw");

        let host = "127.0.0.1";
        let port = 6379;

        let mut producer = RedisProducer::new(host, port);
        producer.set_encoder("avro", Some(&json_schema));

        let mut consumer = RedisConsumer::new(host, port);
        consumer.set_decoder("avro", Some(&json_schema));

        let mut items = Vec::new();
        const NUM_RECORDS: i32 = 3;
        for i in 0..NUM_RECORDS {
            let item = Decoded::from([("index".to_string(), Value::Int(i))]);
            items.push(item);
        }
        let stream_p = stream.clone();
        let t = thread::spawn(move|| {
            let mut sid: Option<String> = None;
            for i in 0..NUM_RECORDS {
                match consumer.consume(&stream, sid.as_deref()) {
                    Ok((new_id, ret)) => {
                        sid = Some(new_id);
                        assert_eq!(ret, Decoded::from([("index".to_string(), Value::Int(i))]));
                    },
                    Err(error) => match error {
                        FcError::RedisError(e) => println!("Test skipped: no Redis connection: {:?}", e),
                        _ => panic!("{:?}", error),
                    },
                }
            }
        });

        let _entries = producer.produce(&items, &stream_p);

        t.join().unwrap();
    }
}