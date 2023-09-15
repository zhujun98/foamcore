/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::fs;
use std::collections::HashMap;

use apache_avro::types::Value;
use redis::Commands;

use crate::error::FcResult;

pub type Encoded = Vec<u8>;
pub type Decoded = HashMap<String, Value>;

/// Parse Avro schema from Json schema.
pub fn json_to_avro_schema(schema: &serde_json::Value) -> apache_avro::Schema {
    let avro_schema = apache_avro::Schema::parse(schema).unwrap();
    match avro_schema {
        apache_avro::Schema::Record(_) => avro_schema,
        _ => panic!("Expected Schema::Record. Actual: {:?}", avro_schema),
    }
}

/// Load Json schema from file.
///
/// A schema is needed not only for serialization and deserialization,
/// but also for metadata like the namespace and name for the Redis
/// stream name.
pub fn load_schema(path: &str) -> (Option<serde_json::Value>, String) {
    let s = fs::read_to_string(path).unwrap_or_else(
        |_| panic!("Unable to read file: {}", path));
    let raw_schema: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&raw_schema).unwrap());
    }

    let namespace = raw_schema.get("namespace").expect("Schema must contain 'namespace'");
    let name = raw_schema.get("name").expect("Schema must contain 'name'");
    let stream = namespace.as_str().unwrap().to_owned() + ":" + name.as_str().unwrap();

    let schema = raw_schema.clone().get("fields").map_or(None, |_| Some(raw_schema));

    (schema, stream)
}

pub struct SchemaRegistry {
    client: redis::Client,
    schemas: HashMap<String, Option<serde_json::Value>>,
}

impl SchemaRegistry {
    pub fn new(host: &str, port: i32) -> Self {
        let client = redis::Client::open(
            format!("redis://{}:{}", host, port.to_string())).expect(
            "Failed to open a Redis connection");
        let schemas : HashMap<String, Option<serde_json::Value>> = HashMap::new();

        SchemaRegistry {
            client,
            schemas,
        }
    }

    pub fn get(&mut self, stream: &str) -> FcResult<&serde_json::Value> {
        let mut con = self.client.get_connection()?;

        // "0" is the version. Schema evolution has not implemented yet.
        let s: String = con.hget(stream.to_owned() + ":_schema", "0").unwrap();
        let schema = serde_json::from_str(s.as_str()).unwrap();

        self.schemas.insert(stream.to_owned(), schema).unwrap();

        Ok(self.schemas.get(stream).unwrap().as_ref().unwrap())
    }

    pub fn set(&mut self, stream: &str, schema: Option<&serde_json::Value>) -> FcResult<()> {
        let mut con = self.client.get_connection()?;

        // "0" is the version. Schema evolution has not implemented yet.
        let _ : () = con.hset(stream.to_owned() + ":_schema", "0", match schema {
            Some(s) => s.to_string(),
            None => String::new(),
        })?;
        self.schemas.insert(stream.to_owned(), schema.cloned());

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::schema::load_schema;

    #[test]
    #[should_panic(expected = "Unable to read file: abc")]
    fn test_load_avro_schema() {
        load_schema("abc");
    }
}
