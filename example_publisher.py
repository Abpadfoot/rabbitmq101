import pika, os, logging
logging.basicConfig()
# Load  the  client  library  and  set  up  some  configuration  parameters.
# The DEFAULT_SOCKET_TIMEOUT is set to
# 0.25s, we would recommend raising this to about 5s to avoid connection timeouts,
# params.socket_timeout = 5. Parse
# CLODUAMQP_URL (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
params = pika.URLParameters(url)
params.socket_timeout = 5
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='pdfprocess')

# send a message
channel.basic_publish(exchange='', routing_key='pdfprocess', body='User Information')
print("[x] Message sent to consumer")
connection.close()