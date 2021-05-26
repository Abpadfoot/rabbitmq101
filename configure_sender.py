import pika, os, logging


class rabbitMq(object):

    def __init__(self ):
        self.url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
        self.params = pika.URLParameters(self.url)
        self.params.socket_timeout = 5
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()

    def create_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name)

    def publish_message(self, payload):
        self.channel.basic_publish(exchange='ex.fanout', routing_key='', body = str(payload))
        print("message has been published")
        self.connection.close()


if __name__ == '__main__':
    server = rabbitMq()
    server.publish_message(payload={'Name': 'Abhra Gupta', 'Profile': 'Data Engineering'})
