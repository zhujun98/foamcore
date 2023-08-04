/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::collections::HashMap;

pub struct CachedSchemaRegistry<'a> {
    client: &'a redis::Client,
    schemas: HashMap<String, serde_json::Value>,
}

impl CachedSchemaRegistry {
    pub fn new(client: &redis::Client) -> Self {
        let schemas : HashMap<String, serde_json::Value> = HashMap::new();
        CachedSchemaRegistry {
            client,
            schemas,
        }
    }

    pub fn get(&self, stream: &str) -> &serde_json::Value {
        if self.schemas.contains_key(stream) {
            self.schemas.get(stream)
        }

        self.schemas.get(stream).unwrap()
    }

    pub fn set(&self, stream: &str, schema: serde_json::Value) {
        if self.schemas.contains_key(stream) {
            return
        }


    }
}