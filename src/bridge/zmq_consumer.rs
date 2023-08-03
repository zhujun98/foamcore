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
    pub fn new(endpoint: &str, sock_type: zmq::SocketType) -> Self {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(sock_type).unwrap();
        socket.connect(endpoint).unwrap();

        ZmqConsumer {
            ctx,
            socket
        }
    }

    pub fn next(&self) -> zmq::Message {
        let msg: zmq::Message = self.socket.recv_msg(0).unwrap();
        msg
    }
}