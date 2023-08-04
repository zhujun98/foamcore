/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use crate::redis_client::schema_registry::CachedSchemaRegistry;

pub struct RedisProducer {
    client: redis::Client,
    schema_registry: CachedSchemaRegistry,
}

impl RedisProducer {

    pub fn new(host: &str, port: i32) -> Self {

        let client = redis::Client::open(
            "redis://".to_owned() + host + ":" + port.to_string().as_str()).unwrap();

        let schema_registry = CachedSchemaRegistry::new(&client);
        RedisProducer {
            client,
            schema_registry,
        }
    }

    pub fn produce(&self, stream: &str, data: zmq::Message, schema: &serde_json::Value) {
        let stream_id = self.client.xadd(stream, );
        self.schema_registry.set(stream, schema);
    }

    fn encode_with_schema(&self) {

    }
}