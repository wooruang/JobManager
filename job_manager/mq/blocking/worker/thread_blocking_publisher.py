import logging
import threading
import queue
import time

from job_manager.mq.blocking.worker.blocking_queue_publisher import BlockingQueuePublisher
from job_manager.singleton import SingletonInstance

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class ThreadBlockingPublisher:

    def __init__(self, queue_list, rabbitmq_host=None):
        self.queue_list = queue_list
        self.rabbitmq_host = rabbitmq_host
        self._publisher = BlockingQueuePublisher(rabbitmq_host)
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._run)
        self._is_running = True
        self._thread.start()

    def __del__(self):
        self.close()

    def close(self):
        self._is_running = False
        self._thread.join(1)

    def _run(self):
        publisher = BlockingQueuePublisher(self.rabbitmq_host)
        for q in self.queue_list:
            publisher.add_queue(queue=q, durable=True)
        while self._is_running:
            try:
                data = self._queue.get()
                publisher.reconnect_queue()
                self._publisher.publish(**data)
            except Exception as e:
                LOGGER.error(e)
                pass

    def publish(self, data, queue_name, exchange='', reply_to=None, corr_id=None):
        self._queue.put(
            {'data': data, 'queue_name': queue_name, 'exchange': exchange, 'reply_to': reply_to, 'corr_id': corr_id}
        )
