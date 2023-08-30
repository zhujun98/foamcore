/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::collections::HashMap;

use apache_avro::{Reader};
use apache_avro::types::{Value};

pub trait Decoder {
    fn unpack(&self, bytes: Vec<u8>) -> Vec<HashMap<String, Value>>;
}


pub struct AvroDecoder;

impl Decoder for AvroDecoder {

    fn unpack(&self, bytes: Vec<u8>) -> Vec<HashMap<String, Value>> {
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
                Ok(v) => { panic!("Unknown value type: {:?}", v)},
                Err(e) => {
                    panic!("{}", e);
                },
            };
            ret.push(data);
        }
        ret
    }
}

pub struct PickleDecoder;

impl Decoder for PickleDecoder {
    fn unpack(&self, _bytes: Vec<u8>) -> Vec<HashMap<String, Value>> {
        let mut ret = Vec::new();
        let map: HashMap<String, Value> = HashMap::new();
        ret.push(map);
        ret
    }
}

pub fn create_decoder(name: &str) -> Box<dyn Decoder> {
    let n = name.to_lowercase();
    if n == "avro" {
        return Box::new(AvroDecoder);
    }
    if n == "pickle" {
        return Box::new(PickleDecoder);
    }

    panic!("Unknown decoder name: {}", name);
}



#[cfg(test)]
mod tests {
    use crate::decoder::create_decoder;

    #[test]
    fn test_avro_decoder() {

    }

    #[test]
    fn test_pickle_decoder() {

    }

    #[test]
    #[should_panic(expected = "Unknown decoder name: unknown")]
    fn test_unknown_decoder() {
        create_decoder("unknown");
    }
}