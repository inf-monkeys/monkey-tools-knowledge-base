import redis
from core.config import config_data
from rediscluster import (
    RedisCluster,
    ClusterConnectionPool,
)
from redis.sentinel import Sentinel

redis_config = config_data.get("redis", {})

mode = redis_config.get("mode", "standalone")

if mode == "standalone":
    redis_url = redis_config.get("url")
    redis_client = redis.from_url(redis_url)

elif mode == "cluster":
    startup_nodes = redis_config.get("nodes", [])
    password = redis_config.get("password")
    pool = ClusterConnectionPool(startup_nodes=startup_nodes, password=password)
    redis_client = RedisCluster(connection_pool=pool)

elif mode == "sentinel":
    sentinel_nodes = redis_config.get("sentinels", [])
    sentinel_name = redis_config.get("sentinelName")
    password = redis_config.get("password")
    sentinel_nodes = [(item['host'], int(item['port'])) for item in sentinel_nodes]
    sentinel = Sentinel(sentinel_nodes, password=password)
    redis_client = sentinel.master_for(sentinel_name)

