import os


class BaseQueue:
    DEFAULT_RABBITMQ_HOST = 'localhost'

    def __init__(self, rabbitmq_host=None):
        self.queue_name_dict = {}
        self.exchange_name = ''

        self.rabbitmq_host = rabbitmq_host
        if rabbitmq_host is None:
            self.rabbitmq_host = os.getenv('JOB_RABBITMQ_HOST') \
                if os.getenv('JOB_RABBITMQ_HOST') is not None else self.DEFAULT_RABBITMQ_HOST

        self.connection = None
        self.channel = None

    def close(self):
        self._close_impl()
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()

    def _close_impl(self):
        pass
