import pika, os, logging, time


class RabbitmqServer:
    def __init__(self, host):
        self.url = os.environ.get('CLOUDAMQP_URL', host)
        self.params = pika.URLParameters(self.url)
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()  # start a channel

    @classmethod
    def pdf_process_function(cls, msg):
        print("PDF processing")
        print("[x] Received " + str(msg))
        time.sleep(5)
        print("PDF processing finished")
        return;

    def start_channel(self):
        pass

    def declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name)  # declare a  queue

    def callback(self, method, properties, next, body):
        self.pdf_process_function(body)

    def consume_message(self, queue_name):
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()
        self.channel.close()


if __name__ == '__main__':
    host = 'amqp://guest:guest@abhra-ubuntu:5672/%2f'
    consumer_object = RabbitmqServer(host)
    # consumer_object.declare_queue('myqueue1')
    consumer_object.consume_message('topic_queue_errors')

