"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import functools
from typing import Optional
import weakref

import redis


# If the path is given, redis-py will use UnixDomainSocketConnection.
# Otherwise, normal TCP socket connection.
REDIS_UNIX_DOMAIN_SOCKET_PATH = ""


_GLOBAL_REDIS_CONNECTION = None
_GLOBAL_REDIS_CONNECTION_BYTES = None


# keep tracking lazily created connections
_global_connections = dict()


def init_redis_connection(host: str, port: int, *,
                          password: Optional[str] = None) -> redis.Redis:
    """Initialize Redis client connection.

    :param host: IP address of the Redis server.
    :param port:: Port of the Redis server.
    :param password: password for the Redis server.

    :return: Redis connection.
    """
    # reset all connections first
    global _GLOBAL_REDIS_CONNECTION
    _GLOBAL_REDIS_CONNECTION = None
    global _GLOBAL_REDIS_CONNECTION_BYTES
    _GLOBAL_REDIS_CONNECTION_BYTES = None

    for connections in _global_connections.values():
        for ref in connections:
            c = ref()
            if c is not None:
                c.reset()

    # initialize new connection
    if REDIS_UNIX_DOMAIN_SOCKET_PATH:
        raise NotImplementedError(
            "Unix domain socket connection is not supported!")
        # connection = redis.Redis(
        #     unix_socket_path=config["REDIS_UNIX_DOMAIN_SOCKET_PATH"],
        #     decode_responses=decode_responses
        # )
    else:
        # the following two must have different pools
        connection = redis.Redis(
            host, port, password=password, decode_responses=True)
        connection_byte = redis.Redis(
            host, port, password=password, decode_responses=False)

    _GLOBAL_REDIS_CONNECTION = connection
    _GLOBAL_REDIS_CONNECTION_BYTES = connection_byte
    return connection


def redis_connection(decode_responses: bool = True) -> redis.Redis:
    """Return a Redis connection."""
    if decode_responses:
        return _GLOBAL_REDIS_CONNECTION
    return _GLOBAL_REDIS_CONNECTION_BYTES


class MetaRedisConnection(type):
    def __call__(cls, *args, **kw):
        instance = super().__call__(*args, **kw)
        name = cls.__name__
        if name not in _global_connections:
            _global_connections[name] = []
        _global_connections[name].append(weakref.ref(instance))
        return instance


class RedisConnection(metaclass=MetaRedisConnection):
    """Lazily evaluated Redis connection on access."""
    def __init__(self, decode_responses: bool = True):
        self._db = None
        self._decode_responses = decode_responses

    def __get__(self, instance, instance_type):
        if self._db is None:
            self._db = redis_connection(
                decode_responses=self._decode_responses)
        return self._db

    def reset(self):
        self._db = None


class RedisSubscriber(metaclass=MetaRedisConnection):
    """Lazily evaluated Redis subscriber.

    Read the code of pub/sub in Redis:
        https://making.pusher.com/redis-pubsub-under-the-hood/
    """
    def __init__(self, channel, decode_responses: bool = True):
        self._sub = None
        self._decode_responses = decode_responses
        self._channel = channel

    def __get__(self, instance, instance_type):
        if self._sub is None:
            self._sub = redis_connection(
                decode_responses=self._decode_responses).pubsub(
                ignore_subscribe_messages=True)
            try:
                self._sub.subscribe(self._channel)
            except redis.ConnectionError:
                self._sub = None

        return self._sub

    def reset(self):
        if self._sub is not None:
            self._sub.close()
        self._sub = None


class RedisPSubscriber(metaclass=MetaRedisConnection):
    """Lazily evaluated Redis psubscriber."""
    def __init__(self, pattern, decode_responses: bool = True):
        self._sub = None
        self._decode_responses = decode_responses
        self._pattern = pattern

    def __get__(self, instance, instance_type):
        if self._sub is None:
            self._sub = redis_connection(
                decode_responses=self._decode_responses).pubsub(
                ignore_subscribe_messages=True)
            try:
                self._sub.psubscribe(self._pattern)
            except redis.ConnectionError:
                self._sub = None

        return self._sub

    def reset(self):
        if self._sub is not None:
            self._sub.close()
        self._sub = None


def redis_except_handler(func):
    """Handler ConnectionError from Redis."""
    @functools.wraps(func)
    def catched_f(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError:
            # return None if ConnectionError was raised
            return
    return catched_f


class RedisProxyBase:
    """RedisProxyBase.

    Base class for communicate with Redis server.
    """
    _db = RedisConnection()
    _db_nodecode = RedisConnection(decode_responses=False)

    def pubsub(self):
        return self._db.pubsub()

    def reset(self):
        RedisProxyBase.__dict__["_db"].reset()
        RedisProxyBase.__dict__["_db_nodecode"].reset()

    def pipeline(self):
        return self._db.pipeline()

    def execute_command(self, *args, **kwargs):
        return self._db.execute_command(*args, **kwargs)

    @redis_except_handler
    def hset(self, name, key, value):
        """Set a key-value pair of a hash.

        :returns: None if the connection failed;
                  1 if created a new field;
                  0 if set on an old field.
        """
        return self._db.execute_command('HSET', name, key, value)

    @redis_except_handler
    def hmset(self, name, mapping):
        """Set a mapping of a hash.

        :return: None if the connection failed;
                 True if set.
        """
        return self._db.execute_command(
            'HSET', name, *chain.from_iterable(mapping.items()))

    @redis_except_handler
    def hget(self, name, key):
        """Get the value for a given key of a hash.

        :return: None if the connection failed or key was not found;
                 otherwise, the value.
        """
        return self._db.execute_command('HGET', name, key)

    @redis_except_handler
    def hmget(self, name, keys):
        """Get values for a list of keys of a hash.

        :return: None if the connection failed;
                 otherwise, a list of values.
        """
        return self._db.hmget(name, keys)

    @redis_except_handler
    def hdel(self, name, *keys):
        """Delete a number of keys of a hash.

        :return: None if the connection failed;
                 number of keys being found and deleted.
        """
        return self._db.execute_command('HDEL', name, *keys)

    @redis_except_handler
    def hget_all(self, name):
        """Get all key-value pairs of a hash.

        :return: None if the connection failed;
                 otherwise, a dictionary of key-value pairs. If the hash
                 does not exist, an empty dictionary will be returned.
        """
        return self._db.execute_command('HGETALL', name)

    @redis_except_handler
    def hget_all_multi(self, name_list):
        """Get all key-value pairs of a list of hash.

        :return: None if the connection failed;
                 otherwise, a list of dictionaries of key-value pairs.
                 If the hash does not exist, an empty dictionary will
                 be returned.
        """
        pipe = self._db.pipeline()
        for name in name_list:
            pipe.execute_command('HGETALL', name)
        return pipe.execute()

    @redis_except_handler
    def hincrease_by(self, name, key, amount=1):
        """Increase the value of a key in a hash by the given amount.

        :return: None if the connection failed;
                 value after the increment if the initial value is an integer;
                 amount if key does not exist (set initial value to 0).

        :raise: redis.exceptions.ResponseError if value is not an integer.
        """
        return self._db.execute_command('HINCRBY', name, key, amount)

    @redis_except_handler
    def hincrease_by_float(self, name, key, amount=1.0):
        """Increase the value of a key in a hash by the given amount.

        :return: None if the connection failed;
                 value after the increment if the initial value can be
                 converted to a float;
                 amount if key does not exist (set initial value to 0).

        :raise: redis.exceptions.ResponseError if value is not a float.
        """
        return self._db.execute_command('HINCRBYFLOAT', name, key, amount)
