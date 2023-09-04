/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::collections::HashMap;
use std::rc::Rc;

use apache_avro::{Writer};
use apache_avro::types::{Record, Value};
use apache_avro::schema::Schema;
use redis::{Commands, RedisError};
use redis::streams::StreamMaxlen::Equals;

use crate::schema_registry::CachedSchemaRegistry;

pub struct RedisProducer {
    client: Rc<redis::Client>,
    schema_registry: CachedSchemaRegistry,
    maxlen: usize,
}

impl RedisProducer {

    pub fn new(host: &str, port: i32, maxlen: usize) -> Self {

        let client = Rc::new(redis::Client::open(
            "redis://".to_owned() + host + ":" + port.to_string().as_str()).unwrap());

        let schema_registry = CachedSchemaRegistry::new(Rc::clone(&client));

        RedisProducer {
            client: Rc::clone(&client),
            schema_registry,
            maxlen,
        }
    }

    pub fn produce(&mut self,
                   data: HashMap<String, Value>,
                   schema: &Schema) -> Result<(String, String), RedisError> {
        let mut con = self.client.get_connection()?;

        let mut writer = Writer::new(schema, Vec::new());
        let mut record = Record::new(schema).unwrap();
        for (k, v) in data {
            record.put(&k, v);
        }
        let _ = writer.append(record);

        let encoded = match writer.into_inner() {
            Ok(b) => b,
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        let stream = match schema {
            Schema::Record(s) => format!("{}:{}", &s.name.namespace.clone().unwrap(), &s.name.name),
            _ => unreachable!(),
        };
        let entry = con.xadd_maxlen(&stream, Equals(self.maxlen), "*", &[("data", encoded)])?;

        self.schema_registry.set(&format!("{}:schema", &stream), schema)?;

        Ok((stream, entry))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_redis_producer() {

    }
}