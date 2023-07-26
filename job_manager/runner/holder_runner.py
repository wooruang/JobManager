import datetime
import logging
import time
import signal
import json
import uuid

from job_manager.mq.blocking import BlockingQueue
from job_manager import timetools, logtools

LOGGER = logging.getLogger(__name__)


class JobTaskHolder:
    HEARTBEAT_LOGGER_PERIOD = 60 * 10
    RECONNECT_COUNT = 50

    def __init__(self):
        self.uuid = str(uuid.uuid4())

        # MessageQ(RabbitMQ).
        self.worker = BlockingQueue(self.holder_consume)
        self.worker.add_queue('job_worker', durable=True)
        self.worker.add_queue('job_holder', durable=True)
        self.worker.set_basic_consume('job_holder')

        self.holder_dict = {}

        self.set_signal()

        self.is_run = True

    def set_signal(self):
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

    def holder_consume(self, body):
        try:
            json_dict = json.loads(body)

            self.hold_job(json_dict)
        except Exception as e:
            LOGGER.error('Error %s', e)

    def run(self):
        start_time = time.time()
        while self.is_run:
            diff_time = time.time() - start_time
            if diff_time > self.HEARTBEAT_LOGGER_PERIOD:
                start_time = time.time()
                LOGGER.info('%s holder HeartBeat. (%s)', self.__class__.__name__, self.uuid)

            self.worker.process_data_events()

            release_jobs = self.release_job()

            for r in release_jobs:
                d = json.dumps(r[1])
                self.worker.publish(d, 'job_worker')
            time.sleep(0.1)

    def close(self, signum, frame):
        self.is_run = False
        self.close_message_queue()

    def close_message_queue(self):
        self.worker.close()

    def hold_job(self, job_dict):
        hold_key = str(uuid.uuid4())
        release_time = timetools.str_to_datetime(job_dict['request_context']['release_time'])

        self.holder_dict[hold_key] = (release_time, job_dict)

    def release_job(self):
        now = datetime.datetime.now()

        released_keys = []
        for key in self.holder_dict:
            if self.holder_dict[key][0] <= now:
                released_keys.append(key)

        released_jobs = []
        for k in released_keys:
            released_jobs.append(self.holder_dict.pop(k))
        return released_jobs
