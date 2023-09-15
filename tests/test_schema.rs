use foamcore::schema::{load_schema, SchemaRegistry};
use foamcore::error::FcError;

static SCHEMA1_FILEPATH: &str = "tests/data/schema1.json";
static SCHEMA2_FILEPATH: &str = "tests/data/schema2.json";

#[test]
fn test_load_schema() {
    {
        let (schema, stream) = load_schema(SCHEMA1_FILEPATH);
        assert_eq!(stream, "schema1:raw");
        assert!(schema.is_some());
    }
    {
        let (schema, stream) = load_schema(SCHEMA2_FILEPATH);
        assert_eq!(stream, "schema2:raw");
        assert!(schema.is_none());
    }
}

#[test]
fn test_schema_registry() {
    let host = "127.0.0.1";
    let port = 6379;

    let mut registry = SchemaRegistry::new(host, port);

    let (schema, stream) = load_schema(SCHEMA1_FILEPATH);

    let mut has_redis: bool = true;
    registry.set(&stream, schema.as_ref()).unwrap_or_else(|error| {
        match error {
            FcError::RedisError(_) => {
                println!("Test skipped: no Redis connection: {:?}", error);
                has_redis = false
            },
            _ => panic!("{:?}", error),
        }
    });

    if has_redis {
        let schema_readback = registry.get(&stream).unwrap();
        assert_eq!(schema_readback, &schema.unwrap());
    }
}