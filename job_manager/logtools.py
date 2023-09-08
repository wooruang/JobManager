import os
import logging
import sys
from functools import wraps

AODT_LOGGING_LEVEL = 'INFO' if os.getenv('AODT_LOGGING_LEVEL') is None else os.getenv('AODT_LOGGING_LEVEL')


def initialize_default_logger():
    LOG_FORMAT = ('%(levelname) -8s %(asctime)s %(name) -10s %(funcName) '
                  '-20s %(lineno) -5d: %(message)s')
    logging.basicConfig(stream=sys.stdout, format=LOG_FORMAT, level=logging.getLevelName(AODT_LOGGING_LEVEL))


def initialize_logger(tag='aodt'):
    initialize_default_logger()


def job_logging(logger):
    def wrapper(func):
        @wraps(func)
        def decorator(*args, **kwargs):
            logger.info(f'Start {func}')
            result = func(*args, **kwargs)
            return result

        return decorator

    return wrapper
