/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
use zmq::Result;

pub struct ZmqConsumer {
    ctx: zmq::Context,
    socket: zmq::Socket,
}

impl ZmqConsumer {
    pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).expect("Error in creating zmq socket");
        socket.connect(endpoint).unwrap_or_else(
            |_| panic!("Error in connecting to endpoint: {}", endpoint));

        if sock_type == zmq::SocketType::SUB {
            socket.set_subscribe(b"").unwrap();
        }

        ZmqConsumer {
            ctx,
            socket,
        }
    }

    pub fn next(&self) -> Result<Vec<u8>>  {
        self.socket.recv_bytes(0)
    }
}

#[cfg(test)]
mod tests {

    use std::thread;
    use crate::zmq_consumer::ZmqConsumer;

    struct ZmqProducer {
        ctx: zmq::Context,
        socket: zmq::Socket,
    }

    impl ZmqProducer {
        pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
            let ctx = zmq::Context::new();
            let socket = ctx.socket(sock_type).unwrap();

            socket.bind("tcp://*.5555").unwrap();

            ZmqProducer {
                ctx,
                socket,
            }
        }

        pub fn start() {
            thread::spawn(|| {

            });
        }
    }

    #[test]
    fn test_zmq_consumer() {

    }
}