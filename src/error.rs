/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use thiserror;
use apache_avro;
use redis;
use zmq;

pub type FcResult<T> = Result<T, FcError>;

#[derive(thiserror::Error, Debug)]
pub enum FcError {
    #[error("Avro error")]
    AvroError(#[from] apache_avro::Error),
    #[error("Redis error")]
    RedisError(#[from] redis::RedisError),
    #[error("Zmq error")]
    ZmqError(#[from] zmq::Error),
}
