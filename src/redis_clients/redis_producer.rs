/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;

use crate::redis_clients::schema_registry::CachedSchemaRegistry;

pub struct RedisProducer {
    client: Rc<redis::Client>,
    schema_registry: CachedSchemaRegistry,
}

impl RedisProducer {

    pub fn new(host: &str, port: i32) -> Self {

        let client = Rc::new(redis::Client::open(
            "redis://".to_owned() + host + ":" + port.to_string().as_str()).unwrap());

        let schema_registry = CachedSchemaRegistry::new(Rc::clone(&client));

        RedisProducer {
            client: Rc::clone(&client),
            schema_registry,
        }
    }

    pub fn produce(&mut self, stream: &str, item: zmq::Message, schema: &apache_avro::Schema) {
        // let stream_id = self.client.xadd(stream, self.encode_with_schema(item, schema));
        // self.schema_registry.set(stream, schema);
    }
}