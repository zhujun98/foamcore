/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::collections::HashMap;

use apache_avro;
use apache_avro::{Reader, Schema};
use apache_avro::types::{Value};

use crate::utils::{json_to_avro_schema, Encoded, Decoded, FcError, FcResult};

pub trait Decoder {
    fn unpack(&self, bytes: &Encoded) -> FcResult<Vec<Decoded>>;
}

pub struct AvroDecoder {
    schema: Schema,
}

impl AvroDecoder {
    pub fn new(schema: &serde_json::Value) -> Self {
        let avro_schema = json_to_avro_schema(&schema);

        AvroDecoder {
            schema: avro_schema,
        }
    }
}

impl Decoder for AvroDecoder {

    fn unpack(&self, bytes: &Encoded) -> FcResult<Vec<Decoded>> {
        let reader = Reader::new(&bytes[..]).unwrap();

        let mut ret = Vec::new();
        for record in reader {
            let data = match record {
                Ok(Value::Record(p)) => {
                    let mut m = HashMap::new();
                    for (k, v) in p {
                        m.insert(k, v);
                    }
                    m
                },
                Ok(_) => return Err(FcError::AvroError(apache_avro::Error::Validation)),
                Err(e) => return Err(FcError::AvroError(e)),
            };
            ret.push(data);
        }
        Ok(ret)
    }
}

pub struct PickleDecoder;

impl Decoder for PickleDecoder {
    fn unpack(&self, _bytes: &Encoded) -> FcResult<Vec<Decoded>> {
        let mut ret = Vec::new();
        let map: HashMap<String, Value> = HashMap::new();
        ret.push(map);
        Ok(ret)
    }
}

pub fn create_decoder(name: &str, schema: Option<&serde_json::Value>) -> Box<dyn Decoder> {
    match name.to_lowercase().as_str() {
        "avro" => Box::new(AvroDecoder::new(schema.unwrap())),
        "pickle" => Box::new(PickleDecoder),
        _ => panic!("Unknown decoder name: {}", name),
    }
}


#[cfg(test)]
mod tests {
    use apache_avro::types::Value;

    use crate::decoder::{create_decoder, Decoded};
    use crate::encoder::create_encoder;

    #[test]
    fn test_avro_decoder() {
        let raw_schema = r#"
            {
                "namespace": "testcase",
                "type": "record",
                "name": "raw",
                "fields": [
                    {
                        "name": "integer",
                        "type": "long"
                    },
                    {
                        "name": "string",
                        "type": "string"
                    },
                    {
                        "name": "array2d",
                        "type": {
                            "type": "record",
                            "logicalType": "ndarray",
                            "name": "NDArray",
                            "fields": [
                                {"name": "shape", "type": {"items": "int", "type": "array"}},
                                {"name": "dtype", "type": "string"},
                                {"name": "data", "type": "bytes"}
                            ]
                        }
                    }

                ]
            }"#;
        let schema: serde_json::Value = serde_json::from_str(raw_schema).unwrap();
        let encoder = create_encoder("avro", Some(&schema));
        let decoder = create_decoder("avro", Some(&schema));

        let raw = Decoded::from([
            ("integer".to_string(), Value::Long(1)),
            ("string".to_string(), Value::String("Hello world!".to_string())),
            ("array2d".to_string(), Value::Record(
                vec![
                    ("shape".to_string(), Value::Array(vec![Value::Int(2), Value::Int(2)])),
                    ("dtype".to_string(), Value::String(">f4".to_string())),
                    ("data".to_string(), Value::Bytes(vec![1, 2, 3, 4].try_into().unwrap()))
                ]
            ))
        ]);
        let bytes = encoder.pack(&raw).unwrap();
        let decoded = decoder.unpack(&bytes).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(raw, decoded[0]);
    }

    #[test]
    fn test_pickle_decoder() {
        let _ = create_decoder("pickle", None);
    }

    #[test]
    #[should_panic(expected = "Unknown decoder name: unknown")]
    fn test_unknown_decoder() {
        create_decoder("unknown", None);
    }
}