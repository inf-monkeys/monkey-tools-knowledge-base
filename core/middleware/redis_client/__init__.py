import redis
from core.config import config_data

redis_config = config_data.get('redis', {})

redis_url = redis_config.get('url')
redis_client = redis.from_url(redis_url)
