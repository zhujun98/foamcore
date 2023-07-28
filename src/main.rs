/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
pub mod bridge;

use crate::bridge::FoamBridge;

use std::env;
use std::fs;

fn load_schema(path: &String) -> serde_json::Value {
    let s = fs::read_to_string(path).expect("Unable to read file");
    let data: serde_json::Value = serde_json::from_str(&s).expect(
        "JSON does not have correct format");
    if cfg!(debug_assersions) {
        println!("{}", serde_json::to_string_pretty(&data).unwrap());
    }
    return data;
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        panic!("Usage: foamcore <schema file path>");
    }

    let schema = load_schema(&args[1]);

    let bridge = FoamBridge::new(schema);
    bridge.start();
}