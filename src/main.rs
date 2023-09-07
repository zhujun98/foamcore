/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use clap::Parser;

mod decoder;
mod encoder;
mod redis_producer;
mod schema_registry;
mod utils;
mod zmq_consumer;

use crate::utils::load_schema;
use crate::zmq_consumer::ZmqConsumer;
use crate::redis_producer::RedisProducer;

#[derive(Parser)]
struct Cli {
    /// Path of the Avro schema file
    #[arg(default_value_t = String::from(""))]
    schema_file: String,
    /// Decoder name for the incoming data
    #[arg(long, default_value_t = String::from("avro"))]
    decoder: String,
    /// Encoder name for the data pushed to Redis
    #[arg(long, default_value_t = String::from("avro"))]
    encoder: String,
    /// ZeroMQ endpoint
    #[arg(long, default_value_t = String::from("tcp://127.0.0.1:45454"))]
    zmq_endpoint: String,
    /// ZeroMQ socket type (REQ, PULL or SUB)
    #[arg(long, default_value_t = String::from("SUB"))]
    zmq_sock: String,
    /// Hostname of the Redis server
    #[arg(long, default_value_t = String::from("127.0.0.1"))]
    redis_host: String,
    /// Port of the Redis server
    #[arg(long, default_value_t = 6379)]
    redis_port: i32,
}

fn main() {
    let cli = Cli::parse();

    let (schema, stream) = load_schema(&cli.schema_file);

    let zmq_socket = match cli.zmq_sock.to_ascii_lowercase().as_str() {
        "pull" => zmq::SocketType::PULL,
        "sub" => zmq::SocketType::SUB,
        _ => panic!("Unknown ZeroMQ socket type string: {:?}", cli.zmq_sock),
    };

    let mut consumer = ZmqConsumer::new(&cli.zmq_endpoint, zmq_socket);
    consumer.set_decoder(&cli.decoder, Some(&schema));

    let mut producer = RedisProducer::new(&cli.redis_host, cli.redis_port);
    producer.set_encoder(&cli.encoder, Some(&schema));
    producer.publish_schema(&stream, &schema);

    loop {
        let decoded = match consumer.consume() {
            Ok(x) => x,
            Err(e) => panic!("Error while consuming data: {:?}", e),
        };

        let entries = producer.produce(&stream, &decoded, 10);

        for entry in entries {
            match entry {
                Ok(x) => println!("Published new data ({}) to Redis stream: {}", x, stream),
                Err(e) => println!("Error while publishing data to Redis: {:?}", e),
            }
        };

    }
}
