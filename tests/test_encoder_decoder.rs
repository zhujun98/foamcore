use apache_avro::types::Value;

use foamcore::decoder::create_decoder;
use foamcore::encoder::create_encoder;
use foamcore::schema::{Decoded, load_schema};

static SCHEMA1_FILEPATH: &str = "tests/data/schema1.json";

#[test]
fn test_avro_encoder_decoder() {
    let (json_schema, _) =  load_schema(SCHEMA1_FILEPATH);
    let encoder = create_encoder("avro", json_schema.as_ref());
    let decoder = create_decoder("avro", json_schema.as_ref());

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
fn test_pickle_encoder_decoder() {
    let _ = create_decoder("pickle", None);
}
