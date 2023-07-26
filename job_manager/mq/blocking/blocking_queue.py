import logging

import pika

from job_manager.mq.blocking.base_blocking_queue import BaseBlockingQueue

LOGGER = logging.getLogger(__name__)


class BlockingQueue(BaseBlockingQueue):
    def __init__(self, consume_cb, rabbitmq_host=None):
        super().__init__(rabbitmq_host)
        self._consume_cb = consume_cb

    def start_consuming(self):
        self.channel.start_consuming()

    def process_data_events(self, time_limit=0):
        self.connection.process_data_events(time_limit=time_limit)

    def set_basic_consume(self, queue_name, callback_cb=None):
        self.channel.basic_qos(prefetch_count=1)
        if callback_cb is None:
            self.channel.basic_consume(queue_name, self._callback)
        else:
            self.channel.basic_consume(queue_name, callback_cb)

    def _callback(self, ch, method, properties, body):
        if callable(self._consume_cb):
            try:
                self._consume_cb(body)
            except Exception:
                LOGGER.exception('Consume Exception!', exc_info=True)
                return
        else:
            LOGGER.error('Consume callback is not callable! (%s)', self._consume_cb)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def publish(self, data, queue_name, exchange=''):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=queue_name,
            body=data,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
