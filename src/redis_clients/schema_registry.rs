/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::rc::Rc;
use std::collections::HashMap;

pub struct CachedSchemaRegistry {
    client: Rc<redis::Client>,
    schemas: HashMap<String, apache_avro::Schema>,
}

impl<'a> CachedSchemaRegistry {
    pub fn new(client: Rc<redis::Client>) -> Self {
        let schemas : HashMap<String, apache_avro::Schema> = HashMap::new();
        CachedSchemaRegistry {
            client,
            schemas,
        }
    }

    pub fn get(&self, stream: &str) -> &apache_avro::Schema {
        if self.schemas.contains_key(stream) {
            return self.schemas.get(stream).unwrap();
        }

        self.schemas.get(stream).unwrap()
    }

    pub fn set(&self, stream: &str, schema: apache_avro::Schema) {
        if self.schemas.contains_key(stream) {
            return
        }
    }
}