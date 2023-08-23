/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::fs;

use apache_avro;

pub fn load_schema(path: &str) -> apache_avro::Schema {
    let s = fs::read_to_string(path).unwrap_or_else(
        |_| panic!("Unable to read file: {}", path));
    let json_schema: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&json_schema).unwrap());
    }
    let schema = apache_avro::Schema::parse(&json_schema).unwrap();
    return schema;
}

#[cfg(test)]
mod tests {
    use crate::utils::load_schema;

    #[test]
    #[should_panic(expected = "Unable to read file: abc")]
    fn test_load_schema() {
        load_schema("abc");
    }
}