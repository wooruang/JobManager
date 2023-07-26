import logging

from job_manager.mq.blocking.rpc.blocking_rpc_client_queue import BlockingRpcClientQueue
from job_manager.singleton import SingletonInstance

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class SingletonBlockingRpcClientQueue(BlockingRpcClientQueue, SingletonInstance):
    def __init__(self):
        super().__init__(None)
