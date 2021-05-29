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

    def callback(self, body):
        self.pdf_process_function(body)

    def consume_message(self, queue_name, ack=False):
        for method_frame, properties, body in self.channel.consume(queue_name):

            # Display the message parts
            print(method_frame)
            print(properties)
            print(body)
            self.callback(body=body)
            # Acknowledge the message
            self.channel.basic_ack(method_frame.delivery_tag)

            # Escape out of the loop after 10 messages
            if method_frame.delivery_tag == 10:
                break

        # Cancel the consumer and return any pending messages
        requeued_messages = self.channel.cancel()
        print('Requeued %i messages' % requeued_messages)

        # Close the channel and the connection
        self.channel.close()
        self.connection.close()

    def get_message(self, queue_name):
        print('Basic get method')
        message = self.channel.basic_get(queue=queue_name)
        return message

    def list_active_customers(self):
        return self.channel.consumer_tags


if __name__ == '__main__':
    host = 'amqp://guest:guest@abhra-ubuntu:5672/%2f'
    consumer_object = RabbitmqServer(host)
    # consumer_object.declare_queue('myqueue1')
    # consumer_object.consume_message('queue_cpp')
    # consumer_object.consume_message('queue_errors', ack=False)
    # message = consumer_object.get_message('queue_timestamp')
    # print(message[2])
    # print(consumer_object.list_active_customers())