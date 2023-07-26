import logging
import signal
import time
import traceback
import uuid

from utils.db import DB

DB.instance()

from ai_jobs.models import find_ready_and_update_to_generating_for_job, update_to_error_for_job, Job, \
    is_time_to_generate_job, update_to_ready_or_done_for_job, update_to_ready_for_job
from job_manager.logtools import initialize_logger

LOGGER = logging.getLogger(__name__)


class JobTaskGenerator:
    HEARTBEAT_LOGGER_PERIOD = 60 * 10

    def __init__(self, job_manager_cls):
        self._run_flag = True
        self.uuid = str(uuid.uuid4())

        self.job_manager_cls = job_manager_cls
        self.manager = job_manager_cls()

        self.set_signal()

    def run(self):
        LOGGER.info('Start %s worker!', self.__class__.__name__)
        start_time = time.time()
        while self._run_flag:
            diff_time = time.time() - start_time
            if diff_time > self.HEARTBEAT_LOGGER_PERIOD:
                start_time = time.time()
                LOGGER.info('%s worker HeartBeat. (%s)', self.__class__.__name__, self.uuid)
            try:
                data = self.find_data_for_analysis()
                if data is None:
                    time.sleep(0.1)
                    continue

                if not self._is_time_to_generate(data):
                    self._restore_ready_status(data)
                    time.sleep(0.1)
                    continue

                self.generate(data)

                self._update_to_ready_or_done_for_job(data)

            except KeyboardInterrupt as e:
                LOGGER.info('KeyboardInterrupt : %s', e)
                self._run_flag = False
            except Exception as e:
                LOGGER.critical('Unknown exception. - %s', e, exc_info=True)
                self._run_flag = False

    def find_data_for_analysis(self):
        pass

    def _is_time_to_generate(self, job):
        pass

    def generate(self, job):
        try:
            self._analyze_impl(job)
        except Exception as e:
            LOGGER.exception('Fail analysis!')
            self._update_error_status(job, traceback.format_exc())

    def _analyze_impl(self, job: Job):
        pass

    @staticmethod
    def _restore_ready_status(job):
        pass


    @staticmethod
    def _update_error_status(idx, reason):
        pass


    @staticmethod
    def _update_to_ready_or_done_for_job(data):
        pass

    def set_signal(self):
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

    def close(self, signum, frame):
        self._run_flag = False
