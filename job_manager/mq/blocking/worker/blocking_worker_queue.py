import logging

from job_manager.mq.blocking.base_blocking_queue import BaseBlockingConsumeQueue

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class BlockingWorkerQueue(BaseBlockingConsumeQueue):
    def __init__(self, consume_cb, rabbitmq_host=None):
        super().__init__(consume_cb, rabbitmq_host)

    # def _connect_queue_impl(self):
    #     self.channel.queue_declare(queue=self.queue_name, durable=True)
    #
    #     self.channel.basic_qos(prefetch_count=1)
    #     self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback)

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
