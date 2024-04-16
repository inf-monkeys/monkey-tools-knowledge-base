import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

from core.queue.queue_name import QUEUE_NAME_PROCESS_FILE
from core.queue.sub import consume_task_forever

if __name__ == "__main__":
    consume_task_forever(QUEUE_NAME_PROCESS_FILE)
