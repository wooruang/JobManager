import logging
import time
import signal
import json
import uuid

from job_manager.job import BaseJob
from job_manager.logtools import initialize_logger
from job_manager.mq.blocking.worker.blocking_worker_queue import BlockingWorkerQueue

LOGGER = logging.getLogger(__name__)


class JobTaskWorker:
    HEARTBEAT_LOGGER_PERIOD = 60 * 10
    RECONNECT_COUNT = 50

    def __init__(self, job_manager_cls, queue_name='DefaultJob'):
        self.uuid = str(uuid.uuid4())

        self.job_manager_cls = job_manager_cls

        # MessageQ(RabbitMQ).
        self.worker = BlockingWorkerQueue(self.worker_consume)
        self.worker.add_queue(queue_name, durable=True)
        self.worker.set_basic_consume(queue_name)

        self.set_signal()

        self.is_run = True

    def set_signal(self):
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

    def worker_consume(self, body):
        try:
            json_dict = json.loads(body)

            manager = self.job_manager_cls()
            manager.execute(json_dict)
        except Exception as e:
            LOGGER.error('Error %s', e)

    def run(self):
        start_time = time.time()
        while self.is_run:
            diff_time = time.time() - start_time
            if diff_time > self.HEARTBEAT_LOGGER_PERIOD:
                start_time = time.time()
                LOGGER.info('%s worker HeartBeat. (%s)', self.__class__.__name__, self.uuid)
            LOGGER.info('%s worker ready. (%s)', self.__class__.__name__, self.uuid)
            self.worker.start_consuming()
            time.sleep(1)

    def close(self, signum, frame):
        self.is_run = False
        self.close_message_queue()

    def close_message_queue(self):
        self.worker.close()
