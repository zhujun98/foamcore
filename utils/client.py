"""
Python script for testing the server.
"""
import argparse
from pathlib import Path

from fastavro.schema import load_schema

from foamclient import SerializerType, ZmqConsumer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test client')

    parser.add_argument('--port', default="45454", type=int,
                        help="ZMQ socket port (default=45454)")
    parser.add_argument('--sock', default='sub', type=str,
                        help="ZMQ socket type (default=SUB)")

    args = parser.parse_args()

    schema = load_schema(Path(__file__).parent.joinpath("../schemas/debye"))

    with ZmqConsumer(f"tcp://localhost:{args.port}",
                     deserializer=SerializerType.AVRO,
                     schema=schema,
                     sock=args.sock) as client:
        for _ in range(10):
            print(client.next())
