/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::fs;

use apache_avro;
use clap::Parser;

mod bridge;
mod redis_clients;
mod zmq_clients;

use crate::bridge::FoamBridge;

fn load_schema(path: &String) -> apache_avro::Schema {
    let s = fs::read_to_string(path).expect("Unable to read file");
    let json_schema: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&json_schema).unwrap());
    }
    let schema = apache_avro::Schema::parse(&json_schema).unwrap();
    return schema;
}

#[derive(Parser)]
struct Cli {
    /// Path of the schema file
    schema: String,
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

    let schema = load_schema(&cli.schema);

    let bridge = FoamBridge::new(
        schema, cli.zmq_endpoint, &cli.zmq_sock, cli.redis_host, cli.redis_port);
    bridge.start();
}
