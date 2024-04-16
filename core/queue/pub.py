import json
from core.middleware.redis_client import redis_client


def submit_task(queue_name, task_data):
    task_json = json.dumps(task_data)
    redis_client.rpush(queue_name, task_json)
