/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use apache_avro::{Writer, Schema};
use apache_avro::types::{Record};

use crate::utils::{json_to_avro_schema, Encoded, Decoded, FcResult};

pub trait Encoder {
    fn pack(&self, data: &Decoded) -> FcResult<Encoded>;
}

pub struct AvroEncoder {
    schema: Schema,
}

impl AvroEncoder {
    pub fn new(schema: &serde_json::Value) -> Self {
        let avro_schema = json_to_avro_schema(&schema);

        AvroEncoder {
            schema: avro_schema,
        }
    }
}

impl Encoder for AvroEncoder {
    fn pack(&self, datum: &Decoded) -> FcResult<Encoded> {
        let mut writer = Writer::new(&self.schema, Vec::new());
        let mut record = Record::new(&self.schema).unwrap();
        for (k, v) in datum {
            record.put(&k, v.clone());
        }
        let _ = writer.append(record);

        let encoded = writer.into_inner()?;
        Ok(encoded)
    }
}

pub struct PickleEncoder;

impl Encoder for PickleEncoder {
    fn pack(&self, _data: &Decoded) -> FcResult<Encoded> {
        Ok(Vec::new())
    }
}

pub fn create_encoder(name: &str, schema: Option<&serde_json::Value>) -> Box<dyn Encoder> {
    match name.to_lowercase().as_str() {
        "avro" => Box::new(AvroEncoder::new(schema.unwrap())),
        "pickle" => Box::new(PickleEncoder),
        _ => panic!("Unknown encoder name: {}", name),
    }
}

#[cfg(test)]
mod tests {
    use crate::encoder::create_encoder;

    #[test]
    fn test_avro_decoder() {

    }

    #[test]
    fn test_pickle_decoder() {

    }

    #[test]
    #[should_panic(expected = "Unknown encoder name: unknown")]
    fn test_unknown_decoder() {
        create_encoder("unknown", None);
    }
}