/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::collections::HashMap;

use apache_avro;
use apache_avro::Reader;
use apache_avro::types::{Value};

use crate::schema::{Encoded, Decoded, json_to_avro_schema};
use crate::error::{FcError, FcResult};

pub trait Decoder {
    fn unpack(&self, bytes: &Encoded) -> FcResult<Vec<Decoded>>;
}

pub struct AvroDecoder {
    schema: apache_avro::Schema,
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
        let reader = Reader::with_schema(&self.schema, &bytes[..]).unwrap();

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

pub fn create_decoder(name: &str, schema: Option<&serde_json::Value>) -> Box<dyn Decoder + Send> {
    match name.to_lowercase().as_str() {
        "avro" => Box::new(AvroDecoder::new(schema.unwrap())),
        "pickle" => Box::new(PickleDecoder),
        _ => panic!("Unknown decoder name: {}", name),
    }
}

#[cfg(test)]
mod tests {
    use crate::decoder::create_decoder;

    #[test]
    #[should_panic(expected = "Unknown decoder name: unknown")]
    fn test_unknown_decoder() {
        create_decoder("unknown", None);
    }
}