import os
import pika

from job_manager.mq.base_queue import BaseQueue


class BaseBlockingQueue(BaseQueue):
    def __init__(self, rabbitmq_host=None):
        super().__init__(rabbitmq_host)
        self.connect_queue()
        self.connect_channel()
        self._connect_queue_impl()

    def connect_queue(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host,
                                                                            heartbeat=0,
                                                                            tcp_options={'TCP_KEEPIDLE': 60}))

    def connect_channel(self):
        self.channel = self.connection.channel()

    def reconnect_queue(self):
        if self.channel.is_closed:
            if self.connection.is_closed:
                self.connect_queue()
            self.connect_channel()

    def add_queue(self,
                  queue,
                  passive=False,
                  durable=False,
                  exclusive=False,
                  auto_delete=False,
                  arguments=None):
        if queue in self.queue_name_dict:
            return
        self.channel.queue_declare(queue,
                                   passive,
                                   durable,
                                   exclusive,
                                   auto_delete,
                                   arguments)
        self.queue_name_dict[queue] = {'queue': queue, 'passive': passive, 'durable': durable,
                                       'exclusive': exclusive,
                                       'auto_delete': auto_delete, 'arguments': arguments}

    def _connect_queue_impl(self):
        pass


class BaseBlockingConsumeQueue(BaseBlockingQueue):
    def __init__(self, consume_cb, rabbitmq_host=None):
        super().__init__(rabbitmq_host)
        self._consume_cb = consume_cb

    def start_consuming(self):
        self.channel.start_consuming()


class BaseBlockingPublishQueue(BaseBlockingQueue):
    def __init__(self, rabbitmq_host=None):
        super().__init__(rabbitmq_host)
