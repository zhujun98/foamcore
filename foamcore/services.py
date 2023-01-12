"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import argparse
import faulthandler
import os.path as osp
import itertools
import sys
import time
from typing import Optional

import psutil
import redis

from . import __version__
from .bridge import FoamBridge
from .logger import logger
from .processes import register_foam_process
from .redis_utils import init_redis_connection
from .utils import check_system_resource, load_schema, query_yes_no


# absolute path of the Redis server executable
REDIS_EXECUTABLE = osp.join(osp.abspath(osp.dirname(__file__)),
                            "thirdparty/bin/redis-server")
# maximum attempts to ping the Redis server before shutting down the app
REDIS_MAX_PING_ATTEMPTS = 3
# interval for pinging the Redis server from the main GUI, in milliseconds
REDIS_PING_ATTEMPT_INTERVAL = 5000
# maximum allowed REDIS memory (fraction of system memory)
REDIS_MAX_MEMORY_FRAC = 0.2  # must <= 0.5
# password to access the Redis server
REDIS_PASSWORD = "sls2.0"

cpu_info, gpu_info, memory_info = check_system_resource()


def try_to_connect_redis_server(host: str, port: int, *,
                                password: Optional[str] = None,
                                n_attempts: int = 5):
    """Try to connect to a starting Redis server.

    :param host: IP address of the Redis server.
    :param port: port of the Redis server.
    :param password: password for the Redis server.
    :param n_attempts: Number of attempts to connect to the redis server.

    :return: Redis connection client.

    Raises:
        ConnectionError: raised if the Redis server cannot be connected.
    """
    client = redis.Redis(host=host, port=port, password=password)

    for i in range(n_attempts):
        try:
            logger.info(f"Say hello to Redis server at {host}:{port}")
            client.ping()
        except (redis.ConnectionError, redis.InvalidResponse):
            time.sleep(1)
            logger.info("No response from the Redis server")
        else:
            logger.info("Received response from the Redis server")
            return client

    raise ConnectionError(f"Failed to connect to the Redis server at "
                          f"{host}:{port}.")


def start_redis_client():
    """Start a Redis client.

    This function is for the command line tool: extra-foam-redis-cli.
    """
    exec_ = REDIS_EXECUTABLE.replace("redis-server", "redis-cli")

    if not osp.isfile(exec_):
        raise FileNotFoundError(f"Redis executable file {exec_} not found!")

    command = [exec_]
    command.extend(sys.argv[1:])

    logger.setLevel("INFO")

    proc = psutil.Popen(command)
    proc.wait()


def start_redis_server(host: str = '127.0.0.1', port: int = 6379, *,
                       password: Optional[str] = None):
    """Start a Redis server.

    :param str host: IP address of the Redis server.
    :param int port: port of the Redis server.
    :param str password: password for the Redis server.
    """
    executable = REDIS_EXECUTABLE
    if not osp.isfile(executable):
        logger.error(f"Unable to find the Redis executable file: "
                     f"{executable}!")
        sys.exit(1)

    # Construct the command to start the Redis server.
    # TODO: Add log file
    command = [executable,
               "--port", str(port),
               "--loglevel", "warning"]
    if password is not None:
        command.extend(["--requirepass", password])

    process = psutil.Popen(command)

    try:
        # wait for the Redis server to start
        try_to_connect_redis_server(host, port, password=password)
    except ConnectionError:
        # Allow users to assign the port by themselves is also a disaster!
        logger.error(f"Unable to start a Redis server at {host}:{port}. "
                     f"Please check whether the port is already taken up.")
        sys.exit(1)

    if process.poll() is None:
        client = init_redis_connection(host, port, password=password)

        logger.info(f"Redis server started at {host}:{port}")
        logger.info(f"{cpu_info}, {gpu_info}, {memory_info}")

        register_foam_process("redis", process)

        # subscribe List commands
        # client.config_set("notify-keyspace-events", "Kl")

        try:
            frac = REDIS_MAX_MEMORY_FRAC
            if frac < 0.01 or frac > 0.5:
                frac = 0.3  # in case of evil configuration
            client.config_set("maxmemory", int(frac * memory_info.total_memory))
            mem_in_bytes = int(client.config_get('maxmemory')['maxmemory'])
            logger.info(f"Redis memory is capped at "
                        f"{mem_in_bytes / 1024 ** 3:.1f} GB")
        except Exception as e:
            logger.error(f"Failed to config the Redis server.\n" + repr(e))
            sys.exit(1)

        # Increase the hard and soft limits for the redis client pubsub buffer
        # to 512MB and 128MB, respectively.
        cli_buffer_cfg = (client.config_get("client-output-buffer-limit")[
            "client-output-buffer-limit"]).split()
        assert len(cli_buffer_cfg) == 12
        soft_limit = 128 * 1024 ** 2  # 128 MB
        soft_second = 60  # in second
        hard_limit = 4 * soft_limit
        cli_buffer_cfg[8:] = [
            "pubsub", str(hard_limit), str(soft_limit), str(soft_second)
        ]
        client.config_set("client-output-buffer-limit",
                          " ".join(cli_buffer_cfg))

    else:
        # It is unlikely to happen since we have checked the existing
        # Redis server before trying to start a new one. Nevertheless,
        # it could happen if someone just started a Redis server after
        # the check.
        logger.error(f"Unable to start a Redis server at {host}:{port}. "
                     f"Please check whether the port is already taken up.")
        sys.exit(1)


def check_existing_redis_server(host: str, port: int, password: str):
    """Check existing Redis servers.

    Allow to shut down the Redis server when possible and agreed.
    """
    def wait_after_shutdown(n):
        for _count in range(n):
            print(f"Start new Redis server after {n} seconds "
                  + "." * _count, end="\r")
            time.sleep(1)

    while True:
        try:
            client = try_to_connect_redis_server(
                host, port, password=password, n_attempts=1)
        except ConnectionError:
            break

        logger.warning(
            f"Found Redis server already running on this machine "
            f"using port {port}!")

        if query_yes_no(
                "\nYou can choose to shut down the Redis server. Please "
                "note that the owner of the Redis server will be "
                "informed (your username and IP address).\n\n"
                "Shut down the existing Redis server?"
        ):
            try:
                proc = psutil.Process()
                killer = proc.username()
                killer_from = proc.connections()
                client.publish("log:warning",
                               f"<{killer}> from <{killer_from}> "
                               f"will shut down the Redis server "
                               f"immediately!")
                client.execute_command("SHUTDOWN")

            except redis.exceptions.ConnectionError:
                logger.info("The old Redis server was shut down!")

            # ms -> s, give enough margin
            wait_time = int(REDIS_PING_ATTEMPT_INTERVAL / 1000
                            * REDIS_MAX_PING_ATTEMPTS * 2)
            wait_after_shutdown(wait_time)
            continue

        else:
            # not shutdown the existing Redis server
            sys.exit(0)


def application():
    parser = argparse.ArgumentParser(prog="foamcore")
    parser.add_argument("schema", help="path of the schema file")
    parser.add_argument('-V', '--version',
                        action='version',
                        version="%(prog)s " + __version__)
    parser.add_argument('--debug',
                        action='store_true',
                        help="run in debug mode")
    parser.add_argument("--zmq-endpoint",
                        help="ZMQ endpoint",
                        default="tcp://localhost:45454")
    parser.add_argument("--zmq-sock",
                        help="ZMQ socket type (REQ, PULL or SUB)",
                        default="SUB")
    parser.add_argument("--redis-host",
                        help="hostname of the Redis server",
                        default="127.0.0.1",
                        type=lambda s: s.lower())
    parser.add_argument("--redis-port",
                        help="port of the Redis server",
                        default=6379,
                        type=int)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel("DEBUG")
    # No ideal whether it affects the performance. If it does, enable it only
    # in debug mode.
    faulthandler.enable(all_threads=False)

    redis_host = args.redis_host
    redis_port = args.redis_port
    if redis_host not in ["localhost", "127.0.0.1"]:
        raise NotImplementedError("Connecting to remote Redis server is "
                                  "not supported yet!")

    check_existing_redis_server(redis_host, redis_port, REDIS_PASSWORD)

    start_redis_server(redis_host, redis_port, password=REDIS_PASSWORD)

    schema = load_schema(args.schema)
    bridge = FoamBridge(schema,
                        zmq_endpoint=args.zmq_endpoint,
                        zmq_sock=args.zmq_sock,
                        redis_host=args.redis_host,
                        redis_port=args.redis_port,
                        redis_password=REDIS_PASSWORD)
    bridge.start()


def kill_application():
    """KIll the application processes in case the app was shutdown unusually.

    If there is SEGFAULT, the child processes won't be clean up.
    """
    logger.setLevel('CRITICAL')

    py_procs = []  # Python processes
    thirdparty_procs = []  # thirdparty processes like redis-server
    for proc in psutil.process_iter(attrs=['pid', 'cmdline']):
        cmdline = proc.info['cmdline']
        if cmdline and REDIS_EXECUTABLE in cmdline[0]:
            thirdparty_procs.append(proc)
        elif len(cmdline) > 2 and 'extra_foam.services' in cmdline[2]:
            py_procs.append(proc)

    if not py_procs and not thirdparty_procs:
        print("Found no EXtra-foam process!")
        return

    # kill Python processes first
    for proc in py_procs:
        proc.kill()
        print(f"Sent SIGKILL to {proc} ...")

    for proc in thirdparty_procs:
        proc.kill()
        print(f"Sent SIGKILL to {proc} ...")

    gone, alive = psutil.wait_procs(
        itertools.chain(py_procs, thirdparty_procs), timeout=1.0)

    if alive:
        for p in alive:
            print(f"{p} survived SIGKILL, "
                  f"please try again or kill it manually")
    else:
        print("All the above EXtra-foam processes have been killed!")


if __name__ == "__main__":

    application()
