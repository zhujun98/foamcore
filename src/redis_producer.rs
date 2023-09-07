/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;

use redis::{Commands};
use redis::streams::StreamMaxlen::Equals;

use crate::encoder::{create_encoder, Encoder};
use crate::schema_registry::CachedSchemaRegistry;
use crate::utils::{Decoded, FcResult};

pub struct RedisProducer {
    client: Rc<redis::Client>,
   schema_registry: CachedSchemaRegistry,
    encoder: Option<Box<dyn Encoder>>,
}

impl RedisProducer {

    pub fn new(host: &str, port: i32) -> Self {

        let client = Rc::new(redis::Client::open(
            "redis://".to_owned() + host + ":" + port.to_string().as_str()).unwrap());

       let schema_registry = CachedSchemaRegistry::new(Rc::clone(&client));

        RedisProducer {
            client: Rc::clone(&client),
            schema_registry,
            encoder: None,
        }
    }

    pub fn set_encoder(&mut self, name: &str, schema: Option<&serde_json::Value>) {
        self.encoder = Some(create_encoder(name, schema));
    }

    pub fn publish_schema(&mut self, stream: &str, schema: &serde_json::Value) {
        let _ = self.schema_registry.set(stream, schema).unwrap();
    }

    pub fn produce(&mut self, stream: &str, data: &Vec<Decoded>, maxlen: usize)
            -> Vec<FcResult<String>> {
        data.into_iter().map(|x| {
            let encoded = self.encoder.as_ref().unwrap().pack(x)?;

            let mut con = self.client.get_connection()?;

            let entry = con.xadd_maxlen(stream, Equals(maxlen), "*", &[("data", encoded)])?;
            Ok(entry)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_redis_producer() {

    }
}