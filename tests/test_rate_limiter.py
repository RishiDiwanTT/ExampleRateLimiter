import time
import pytest
from rate_limiter import InMemoryRateLimiter, RateLimitExceededException, RedisRateLimiter, rate_limit


class TestInMemoryRateLimiter:

    def test_inc_count(self):
        rate_limiter = InMemoryRateLimiter()
        rate_limiter.inc("test_identifier")
        assert rate_limiter.count("test_identifier") == 1

    def test_multiple_inc_count(self):
        rate_limiter = InMemoryRateLimiter()
        rate_limiter.inc("test_identifier")
        rate_limiter.inc("test_identifier")
        rate_limiter.inc("test_identifier")
        rate_limiter.inc("test_identifier_1")
        rate_limiter.inc("test_identifier_1")
        assert rate_limiter.count("test_identifier") == 3
        assert rate_limiter.count("test_identifier_1") == 2

    def test_expire_keys(self):
        rate_limiter = InMemoryRateLimiter(limit=1)
        rate_limiter.inc("test_identifier")
        rate_limiter.inc("test_identifier")
        assert rate_limiter.count("test_identifier") == 2
        # sleep for the one second till expiry
        time.sleep(1)
        assert rate_limiter.count("test_identifier") == 0


class TestRateLimit:

    def test_rate_limit(self):
        rate_limiter = InMemoryRateLimiter(limit=2)
        rate_limit("test_identifier", rate_limiter)
        rate_limit("test_identifier", rate_limiter)
        with pytest.raises(RateLimitExceededException):
            rate_limit("test_identifier", rate_limiter)

    def test_rate_limit_multiple(self):
        rate_limiter = InMemoryRateLimiter(limit=2)
        rate_limit("test_identifier", rate_limiter)
        rate_limit("test_identifier", rate_limiter)
        # should not raise an error
        rate_limit("test_identifier_1", rate_limiter)
        rate_limit("test_identifier_1", rate_limiter)
        # raises an error
        with pytest.raises(RateLimitExceededException):
            rate_limit("test_identifier_1", rate_limiter)

@pytest.fixture
def redis_limiter():
    rate_limiter = RedisRateLimiter(port=6378, limit=2)
    try:
        yield rate_limiter
    except:
        pass
    finally:
        rate_limiter.redis.flushall()  # clear redis for other tests

class TestRedisRateLimiter:
    # This test requires a running Redis server on port 6378
    # The docker-compose should suffice here

    def test_inc_count(self, redis_limiter: RedisRateLimiter):
        redis_limiter.inc("test_identifier")
        redis_limiter.inc("test_identifier")
        assert redis_limiter.count("test_identifier") == 2
        redis_limiter.inc("test_identifier_1")
        assert redis_limiter.count("test_identifier_1") == 1

    def test_expire_keys(self, redis_limiter: RedisRateLimiter):
        redis_limiter.inc("test_identifier")
        redis_limiter.inc("test_identifier")
        assert redis_limiter.count("test_identifier") == 2
        redis_limiter.inc("test_identifier_1")
        assert redis_limiter.count("test_identifier_1") == 1
        time.sleep(1)
        assert redis_limiter.count("test_identifier") == 0
        assert redis_limiter.count("test_identifier_1") == 0

    def test_rate_limit(self, redis_limiter: RedisRateLimiter):
        rate_limit("test_identifier", redis_limiter)
        rate_limit("test_identifier", redis_limiter)
        with pytest.raises(RateLimitExceededException):
            rate_limit("test_identifier", redis_limiter)

        rate_limit("test_identifier_1", redis_limiter)
        rate_limit("test_identifier_1", redis_limiter)
        with pytest.raises(RateLimitExceededException):
            rate_limit("test_identifier_1", redis_limiter)