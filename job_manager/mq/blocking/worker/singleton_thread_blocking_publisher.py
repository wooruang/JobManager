import logging

from job_manager.mq.blocking.worker.thread_blocking_publisher import ThreadBlockingPublisher
from job_manager.singleton import SingletonInstance

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class SingletonThreadBlockingPublisher(ThreadBlockingPublisher, SingletonInstance):
    def __init__(self, queue_list, rabbitmq_host=None):
        super().__init__(queue_list, rabbitmq_host)
