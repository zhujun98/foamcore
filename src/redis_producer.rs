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

use crate::schema_registry::CachedSchemaRegistry;

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

    pub fn produce(&mut self,
                   data: HashMap<String, Value>,
                   schema: &Schema) -> Result<String, RedisError> {
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
            Schema::Record(s) => &s.name.name,
            _ => unreachable!(),
        };
        let stream_id = con.xadd(stream, "*", &[("data", encoded)])?;

        self.schema_registry.set(stream, schema)?;

        Ok(stream_id)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_redis_producer() {

    }
}