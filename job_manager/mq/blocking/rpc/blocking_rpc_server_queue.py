import pika

from job_manager.mq.blocking.base_blocking_queue import BaseBlockingConsumeQueue


class BlockingRpcServerQueue(BaseBlockingConsumeQueue):
    def __init__(self, consume_cb, routing_key, rabbitmq_host=None):
        super().__init__(consume_cb, rabbitmq_host)
        self.routing_key = routing_key
        # self._bind_queue()

    # def _connect_queue_impl(self):
    #     self.channel.queue_declare(queue=self.queue_name)
    #     self.channel.exchange_declare(exchange=self.exchange_name)
    #
    # def _bind_queue(self):
    #     self.channel.queue_bind(exchange=self.exchange_name,
    #                             queue=self.queue_name,
    #                             routing_key=self.routing_key)
    #
    #     self.channel.basic_consume(self.queue_name, on_message_callback=self._on_message_callback)

    def _on_message_callback(self, ch, method, properties, body):
        response = self._consume_cb(body)

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=response)
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    def _consume(data):
        print(len(data))
        import json
        import base64
        d = json.loads(data.decode())
        # a = d['data'].encode('ascii')
        print(d)
        da = base64.b64decode(d['data'])
        import cv2
        import numpy as np
        img_np = np.frombuffer(da, dtype=np.uint8)
        img = cv2.imdecode(img_np, cv2.IMREAD_COLOR)  # cv2.IMREAD_COLOR in OpenCV 3.1

        print(img.shape)
        cv2.imshow('tt', img)
        cv2.waitKey(0)
        # cv2.imwrite('t.png', img)
        return '2'


    q_n = 'test_blocking_queue2'
    e_n = 'test_blocking_exchange'
    r_k = 'test2'
    s = BlockingRpcServerQueue(q_n, e_n, _consume, r_k)
    s.start_consuming()
