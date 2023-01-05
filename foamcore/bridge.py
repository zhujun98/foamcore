"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import json
import time

from foamclient import (
    DeserializerType, RedisProducer, SerializerType, ZmqConsumer
)

from .logger import logger


raw_schema = {
  "namespace": "debye",
  "type": "record",
  "name": "raw",
  "fields": [
    {
      "name": "index",
      "type": "long"
    },
    {
      "name": "encoder",
      "type": "record",
      "logicalType": "ndarray",
      "fields": [
        {"name": "shape", "type": {"items": "int", "type": "array"}},
        {"name": "typestr", "type": "string"},
        {"name": "data", "type": "bytes"}
      ]
    },
    {
      "name": "samples",
      "type": "record",
      "logicalType": "ndarray",
      "fields": [
        {"name": "shape", "type": {"items": "int", "type": "array"}},
        {"name": "typestr", "type": "string"},
        {"name": "data", "type": "bytes"}
      ]
    }
  ]
}


class FoamBridge:
    def __init__(self, domain, *,
                 zmq_endpoint: str, zmq_sock: str,
                 redis_host: str, redis_port: int, redis_password: str):
        """Initialization.

        :param domain: domain name.
        :param zmq_endpoint: ZMQ endpoint.
        :param zmq_sock: ZMQ socket type.
        :param redis_host: Redis hostname.
        :param redis_port: Redis port number.
        :param redis_password: Redis password
        """
        self._domain = domain

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

    def _update_schema(self, schema_registry, stream: str):
        while True:
            schema = schema_registry.get(stream)
            if schema is not None:
                # TODO: validate schema
                return schema

            time.sleep(1.0)
            logger.info(f"Schema for data stream '{stream}' not published")

            # FIXME: schema should be set in the main GUI
            schema_registry.set(stream, raw_schema)

    def _run(self) -> None:
        stream = f"{self._domain}:raw"
        with ZmqConsumer(self._zmq_endpoint,
                         deserializer=DeserializerType.SLS,
                         sock=self._zmq_sock,
                         timeout=self._zmq_timeout) as consumer:
            producer = RedisProducer(self._redis_host, self._redis_port,
                                     serializer=SerializerType.SLS,
                                     password=self._redis_password)

            schema = self._update_schema(producer.schema_registry, stream)

            while True:
                try:
                    data = consumer.next()

                    # TODO: validation

                    stream_id = producer.produce(stream, data, schema=schema)
                    logger.info(f"Published new data to STREAM: "
                                f"{stream}, {stream_id}")
                except TimeoutError:
                    ...
