/**
 * Distributed under the terms of the BSD 3-Clause License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 *
 * Author: Jun Zhu
 */
pub struct ZmqConsumer {
    ctx: zmq::Context,
    socket: zmq::Socket,
}

impl ZmqConsumer {
    pub fn new(sock_type: zmq::SocketType) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).unwrap();
        ZmqConsumer {
            ctx,
            socket
        }
    }

    pub fn connect(&self, endpoint: &str) -> zmq::Result<()> {
        self.socket.connect(endpoint)
    }

    pub fn next(&self) {
        println!("waiting for data");
        let _msg = self.socket.recv_msg(0).unwrap();
        // println!(
        //     "Identity: {:?} Message : {}",
        //     msg[0],
        //     str::from_utf8(&msg[1]).unwrap()
        // );
        // }
    }
}