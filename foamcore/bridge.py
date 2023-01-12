"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from foamclient import (
    DeserializerType, RedisProducer, SerializerType, ZmqConsumer
)

from .logger import logger


class FoamBridge:
    def __init__(self, schema: dict, *,
                 zmq_endpoint: str, zmq_sock: str,
                 redis_host: str, redis_port: int, redis_password: str):
        """Initialization.

        :param schema: schema of the raw data.
        :param zmq_endpoint: ZMQ endpoint.
        :param zmq_sock: ZMQ socket type.
        :param redis_host: Redis hostname.
        :param redis_port: Redis port number.
        :param redis_password: Redis password
        """
        self._schema = schema

        self._zmq_endpoint = zmq_endpoint
        self._zmq_sock = zmq_sock
        self._zmq_timeout = 0.1

        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_password = redis_password

    def start(self) -> None:
        try:
            self._run()
        except KeyboardInterrupt:
            logger.info("Bridge terminated from the keyboard")

    def _run(self) -> None:
        stream = f"{self._schema['namespace']}:{self._schema['name']}"
        with ZmqConsumer(self._zmq_endpoint,
                         deserializer=DeserializerType.SLS,
                         sock=self._zmq_sock,
                         timeout=self._zmq_timeout) as consumer:
            producer = RedisProducer(self._redis_host, self._redis_port,
                                     serializer=SerializerType.SLS,
                                     password=self._redis_password)

            while True:
                try:
                    data = consumer.next()
                except TimeoutError:
                    continue

                transformed = self._transform(data, self._schema)

                try:
                    stream_id = producer.produce(stream, transformed, self._schema)
                    logger.info(f"Published new data to STREAM: "
                                f"{stream}, {stream_id}")
                except TimeoutError:
                    continue
                except RuntimeError as e:
                    logger.info(str(e))

    def _transform(self, data, schema):
        """Transform data to match the schema.

        Data received from the DAQ can have different formats. For example,
        we don't want to store a serialized numpy array in Redis because
        it is cannot be recognized by other languages except for Python.
        Instead, we will serialize the array's data, shape and dtype separately
        according to the schema.
        """
        # TODO:
        return data
