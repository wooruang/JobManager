import time
import uuid

import pika

from job_manager.mq.blocking.base_blocking_queue import BaseBlockingPublishQueue


class BlockingRpcClientQueue(BaseBlockingPublishQueue):
    def __init__(self, rabbitmq_host=None):
        # !IMPORTANT! callback_queue Must be initialized before 'super().__init__'.
        self.callback_queue = None

        super().__init__(rabbitmq_host)

        self._response_data = None
        self.corr_id = None

    def _connect_queue_impl(self):
        self._connect_callback_queue()
        # self.channel.exchange_declare(exchange=self.exchange_name)

    def _connect_callback_queue(self):
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True)

    def _on_response(self, ch, method, properties, body):
        self._is_get_response = True
        if self.corr_id == properties.correlation_id:
            self._response_data = body

    def _init_response(self, request_uuid=None):
        self._is_get_response = False
        self._response_data = None
        self.corr_id = str(uuid.uuid4()) if request_uuid is None else request_uuid

    def check_queue_and_reconnect(self):
        if self.channel.is_closed or self.connection.is_closed:
            self.reconnect_queue()
            self._connect_queue_impl()

    def request(self, routing_key, data, timeout=5):
        self.check_queue_and_reconnect()
        self._init_response()

        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key=routing_key,
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id,
                                   ),
                                   body=data)

        # start_time = time.time()
        # while True:
        #     compare_time = time.time()
        #     diff_time = compare_time - start_time
        #     if diff_time > timeout:
        #         break
        #     self.connection.process_data_events()
        self.connection.process_data_events(time_limit=timeout)

        return self._response_data

    def response_only(self, request_uuid, timeout=5):
        self._init_response(request_uuid)
        self.connection.process_data_events(time_limit=timeout)
        return self._response_data


if __name__ == '__main__':
    e_n = 'test_blocking_exchange'
    r_k = 'test2'
    c = BlockingRpcClientQueue('', e_n)

    import cv2
    import base64
    with open('/Users/hansaemlee/Documents/temp/maskrcnn.png', 'rb') as f:
        d = f.read()
    # img = cv2.imread('/Users/hansaemlee/Documents/temp/maskrcnn.png')
    # print(img.dtype)
    # d = img.tobytes()
    # 3518400
    #  533292
    #  711056
    # print(d) # 533292 3518400
    data = {'data': base64.b64encode(d).decode('ascii')}
    print(len(data['data']))

    import json
    json_data = json.dumps(data)

    res = c.request(r_k, json_data)
    print(res)
