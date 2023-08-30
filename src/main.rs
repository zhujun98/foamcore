/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use clap::Parser;

mod decoder;
mod redis_producer;
mod schema_registry;
mod utils;
mod zmq_consumer;

use crate::utils::load_schema;
use crate::decoder::create_decoder;
use crate::zmq_consumer::ZmqConsumer;
use crate::redis_producer::RedisProducer;

#[derive(Parser)]
struct Cli {
    /// Path of the Avro schema file
    schema: String,
    /// Decoder name for the incoming data
    #[arg(long, default_value_t = String::from("avro"))]
    decoder: String,
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

    let zmq_socket = match cli.zmq_sock.to_ascii_lowercase().as_str() {
        "pull" => zmq::SocketType::PULL,
        "sub" => zmq::SocketType::SUB,
        _ => panic!("Unknown ZeroMQ socket type string: {:?}", cli.zmq_sock),
    };

    let decoder = create_decoder(&cli.decoder);

    let schema = load_schema(&cli.schema);

    let consumer = ZmqConsumer::new(&cli.zmq_endpoint, zmq_socket);

    let mut producer = RedisProducer::new(&cli.redis_host, cli.redis_port);

    loop {
        let bytes = match consumer.next() {
            Ok(x) => x,
            Err(error) => panic!("Error when receiving the data: {:?}", error),
        };

        let decoded = decoder.unpack(bytes);

        for data in decoded {
            let (stream, entry) = match producer.produce(data, &schema) {
                Ok(x) => x,
                Err(e) => {
                    println!("Error while publishing data to Redis: {:?}", e);
                    continue;
                }
            };
            println!("Published new data ({}) to Redis stream: {}", entry, stream);
        }
    }
}
