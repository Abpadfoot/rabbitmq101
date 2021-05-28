import pika
import time
import logging
LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')


class Sender:
    def __init__(self, host_name):
        self.host_name = host_name
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(self.host_name))
        LOGGER.info('Sender Object Created and Connection established to Rabbit MQ Server')
        self.channel_component = self._connection.channel()
        self.host_name = host_name
        LOGGER.info('Channel created for communication')

    def declare_exchange(self, exchange_name, exchange_type):
        self.channel_component.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        LOGGER.info('Exchange created , name = {}, type = {}'.format(exchange_name, exchange_type))

    def declare_queue(self, queue_name):
        self.channel_component.queue_declare(queue=queue_name)
        LOGGER.info('Queue declared'.format(queue_name))

    def bind_queue_to_exchange(self, queue_name, exchange_name, key):
        self.channel_component.queue_bind(queue=queue_name, exchange=exchange_name, routing_key = key)
        LOGGER.info('Queue - {} and Exchange - {} Binded '.format(queue_name, exchange_name))

    def bind_exchange_to_exchange(self, source, destination, key):
        self.channel_component.exchange_bind(source, destination, key)
        LOGGER.info('Source - {} and Destination Exchange - {} Binded '.format(source, destination))

    def publish_message(self, exchange_name, message_body, key):
        self.channel_component.basic_publish(
            exchange=exchange_name,  # amq.topic as exchange
            routing_key=key,  # Routing key used by producer
            body=bytes(message_body, encoding='utf8')
        )

        
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    sender_object = Sender('localhost')
    # sender_object.define_channel()
    exchange_name = 'ex.fanout2'
    exchange_type = 'fanout'
    sender_object.declare_exchange(exchange_name, exchange_type)
    queue_list = ['queue_python', 'queue_java', 'queue_cpp']
    for queue_name in queue_list:
        sender_object.declare_queue(queue_name)
        binding_key = 'errors'
        sender_object.bind_queue_to_exchange(queue_name, exchange_name, binding_key)
    routing_key_list = ['python.errors', 'java.cpp.errors', 'errors.log', 'all_errors']
    for routing_key in routing_key_list:
        for i in range(10):
            message_body = 'Hello World {} , errors= {}'.format(i, routing_key)
            sender_object.publish_message(exchange_name, message_body, routing_key)
            time.sleep(1)


