/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;

use apache_avro::types::{Value};
use redis::{Commands, RedisError};

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

    pub fn produce(&mut self, stream: &str, item: Value, schema: &apache_avro::Schema) -> Result<String, RedisError> {
        let mut con = self.client.get_connection()?;
        let stream_id = con.xadd(stream, "*", &[("key", "42")])?;

        self.schema_registry.set(stream, schema)?;

        Ok(stream_id)
    }
}