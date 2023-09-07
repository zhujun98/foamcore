/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::fs;
use std::collections::HashMap;

use thiserror;
use apache_avro;
use apache_avro::Schema;
use apache_avro::types::{Value};
use redis;
use zmq;

pub type Encoded = Vec<u8>;
pub type Decoded = HashMap<String, Value>;
pub type FcResult<T> = Result<T, FcError>;

#[derive(thiserror::Error, Debug)]
pub enum FcError {
    #[error("Avro error")]
    AvroError(#[from] apache_avro::Error),
    #[error("Redis error")]
    RedisError(#[from] redis::RedisError),
    #[error("Zmq error")]
    ZmqError(#[from] zmq::Error),
}

// A schema is needed not only for serialization and deserialization,
// but also for metadata like the namespace and name for the Redis
// stream name.
pub fn load_schema(path: &str) -> (serde_json::Value, String) {
    let s = fs::read_to_string(path).unwrap_or_else(
        |_| panic!("Unable to read file: {}", path));
    let schema: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
    }

    let namespace = schema.get("namespace").expect("Schema must contain 'namespace'");
    let name = schema.get("name").expect("Schema must contain 'name'");
    let stream = namespace.as_str().unwrap().to_owned() + ":" + name.as_str().unwrap();
    (schema, stream)
}

pub fn json_to_avro_schema(schema: &serde_json::Value) -> Schema {
    let avro_schema = Schema::parse(schema).unwrap();
    match avro_schema {
        Schema::Record(_) => avro_schema,
        _ => panic!("Expected Schema::Record. Actual: {:?}", avro_schema),
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::load_schema;

    #[test]
    #[should_panic(expected = "Unable to read file: abc")]
    fn test_load_avro_schema() {
        load_schema("abc");
    }
}