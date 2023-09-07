/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;
use std::collections::HashMap;

use redis::Commands;

use crate::utils::FcResult;

pub struct CachedSchemaRegistry {
    client: Rc<redis::Client>,
    schemas: HashMap<String, serde_json::Value>,
}

impl<'a> CachedSchemaRegistry {
    pub fn new(client: Rc<redis::Client>) -> Self {
        let schemas : HashMap<String, serde_json::Value> = HashMap::new();
        CachedSchemaRegistry {
            client,
            schemas,
        }
    }

    // pub fn get(&mut self, stream: &str) -> Result<&serde_json::Value, RedisError> {
    //     if self.schemas.contains_key(stream) {
    //         return Ok(self.schemas.get(stream).unwrap());
    //     }
    //
    //     let mut con = self.client.get_connection()?;
    //     let s: String = con.hget(stream.to_owned() + ":schema", "schema").unwrap();
    //     let schema = serde_json::from_str(s.as_str()).unwrap();
    //
    //     self.schemas.insert(stream.to_owned(), schema).unwrap();
    //
    //     Ok(self.schemas.get(stream).unwrap())
    // }

    pub fn set(&mut self, stream: &str, schema: &serde_json::Value) -> FcResult<()> {
        if self.schemas.contains_key(stream) {
            return Ok(());
        }

        let mut con = self.client.get_connection()?;

        let _ : () = con.hset(stream.to_owned() + ":schema", "schema", schema.to_string())?;
        self.schemas.insert(stream.to_owned(), schema.clone());

        Ok(())
    }
}