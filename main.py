import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

from src.config import config_data
from multiprocessing import Process
from src.server import app
from src.queue import consume_task_forever, PROCESS_FILE_QUEUE_NAME

if __name__ == '__main__':
    consume_task_process = Process(target=consume_task_forever, args=(PROCESS_FILE_QUEUE_NAME,))
    consume_task_process.start()

    app.run(host='0.0.0.0', port=config_data.get('server', {}).get('port', 8899))
