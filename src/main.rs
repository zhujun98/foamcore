/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use std::fs;

use clap::Parser;

pub mod bridge;

use crate::bridge::FoamBridge;

fn load_schema(path: &String) -> serde_json::Value {
    let s = fs::read_to_string(path).expect("Unable to read file");
    let data: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&data).unwrap());
    }
    return data;
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

    let bridge = FoamBridge::new(schema, cli.zmq_endpoint, &cli.zmq_sock);
    bridge.start();
}