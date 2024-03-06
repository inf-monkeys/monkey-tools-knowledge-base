from src.config import config_data
from multiprocessing import Process
from src.server import app
from src.queue import consume_task_forever, PROCESS_FILE_QUEUE_NAME

if __name__ == '__main__':
    consume_task_process = Process(target=consume_task_forever, args=(PROCESS_FILE_QUEUE_NAME,))
    consume_task_process.start()

    app.run(host='0.0.0.0', port=config_data.get('server', {}).get('port', 8899))
