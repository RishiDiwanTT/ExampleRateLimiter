## Rate limit implmentation

2 implementations are available

### In Memory Limiter
This is not scalable and is limited to a single process, since it is in memory
It is low latency, however due to the locking mechanism it will slow down performance

### Redis Limiter
This can be scaled across processes
We depend on the Redis server to maintain sanity of data and expire the keys appropriately
This is a better solution for a more scalable solution like any webserver

## Testing
To test the Redis Limiter, run the associated docker-compose independently
Then simply `pytest`
