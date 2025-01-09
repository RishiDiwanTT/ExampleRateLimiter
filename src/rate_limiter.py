from abc import abstractmethod
from collections import defaultdict
import threading
import time
from typing import cast
import redis
import redis.client
import random


class RateLimiter:
    limit: int = 100

    @abstractmethod
    def inc(self, identifier):
        raise NotImplementedError()

    @abstractmethod
    def count(self, identifier):
        raise NotImplementedError()


class RedisRateLimiter(RateLimiter):
    """Use the leaky bucket algorithm taking advantage of the redis cache expiry
    We depend on Redis to be the synchronous clock for all rate limits across processes"""

    def __init__(self, port, limit=100):
        self.limit = limit  # Maximum number of requests per unit time
        self.expire_in_seconds = 1  # How many seconds
        self._redis_port = port
        self.redis = redis.Redis("localhost", port=self._redis_port)

    def _get_key(self, identifier: str):
        """The key saved in redis, it always starts with the identifier, then timestamp, then a random number for uniqueness"""
        return f"{identifier}_{time.time()}_{random.randint(0, 10000)}"

    def inc(self, indentifier):
        """Increment simply adds a new key-value to redis that should expire in the configured time"""
        self.redis.set(
            self._get_key(indentifier), time.time(), ex=self.expire_in_seconds
        )

    def count(self, indentifier):
        """Pull all the keys for a given identifier and count the number"""
        keys: list[str] = cast(list[str], self.redis.keys(f"{indentifier}_*"))
        return len(keys)


class InMemoryRateLimiter(RateLimiter):
    """An in memory leaky bucket rate limiter, single process, unscalable"""
    def __init__(self, limit=100):
        self._buckets = defaultdict(list)
        self._locks = defaultdict(threading.Lock)
        self.limit = limit  # Maximum number of requests per unit time
        self.expire_in_seconds = 1

    def inc(self, identifier):
        """Lock the given identifier and push the current timestamp"""
        with self._locks[identifier]:
            self._buckets[identifier].append(time.time())

    def count(self, identifier):
        """Lock the given identifier, expire any old values and return the count"""
        with self._locks[identifier]:
            requests = self._buckets[identifier]
            while True:
                if len(requests) == 0:
                    break
                if requests[0] + self.expire_in_seconds < time.time():
                    requests.pop(0)
                else:
                    break

            return len(requests)


class RateLimitExceededException(Exception):
    pass


def rate_limit(identifier: str, service: RateLimiter):
    """Rate limit any request based on a predefined identifier
    The identifer can be anything from a client session key to a user database ID depending on the goal of the service"""
    if service.count(identifier) + 1 > service.limit:
        raise RateLimitExceededException()
    service.inc(identifier)
