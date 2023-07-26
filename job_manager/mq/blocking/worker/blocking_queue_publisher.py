import pika

from job_manager.mq.blocking.base_blocking_queue import BaseBlockingPublishQueue


class BlockingQueuePublisher(BaseBlockingPublishQueue):

    def __init__(self, rabbitmq_host=None):
        super().__init__(rabbitmq_host)

    def publish(self, data, queue_name, exchange='', reply_to=None, corr_id=None):
        props = pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
        if reply_to is not None:
            props.reply_to = reply_to
        if corr_id is not None:
            props.correlation_id = corr_id

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=queue_name,
            body=data,
            properties=props
        )
