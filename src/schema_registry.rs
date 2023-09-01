/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;
use std::collections::HashMap;

use apache_avro::schema::Schema;
use redis::{Commands, RedisError};

pub struct CachedSchemaRegistry {
    client: Rc<redis::Client>,
    schemas: HashMap<String, Schema>,
}

impl<'a> CachedSchemaRegistry {
    pub fn new(client: Rc<redis::Client>) -> Self {
        let schemas : HashMap<String, Schema> = HashMap::new();
        CachedSchemaRegistry {
            client,
            schemas,
        }
    }

    pub fn get(&mut self, stream: &str) -> Result<&Schema, RedisError> {
        if self.schemas.contains_key(stream) {
            return Ok(self.schemas.get(stream).unwrap());
        }

        let mut con = self.client.get_connection()?;
        let s: String = con.hget(stream.to_owned() + ":schema", "schema").unwrap();
        let encoded_schema = serde_json::from_str(s.as_str()).unwrap();
        let schema = Schema::parse(&encoded_schema).unwrap();

        self.schemas.insert(stream.to_owned(), schema).unwrap();

        Ok(self.schemas.get(stream).unwrap())
    }

    pub fn set(&mut self, stream: &str, schema: &Schema) -> Result<(), RedisError> {
        if self.schemas.contains_key(stream) {
            return Ok(());
        }

        let mut con = self.client.get_connection()?;
        let encoded_schema = serde_json::to_string(&schema).unwrap();
        let _ : () = con.hset(stream.to_owned() + ":schema", "schema", encoded_schema)?;
        self.schemas.insert(stream.to_owned(), (*schema).clone());

        Ok(())
    }
}