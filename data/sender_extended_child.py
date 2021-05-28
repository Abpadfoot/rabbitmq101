from sender_extended import Sender

alternate_exchange_ob = Sender('localhost')
source_name = 'ex.alternate1'
source_exchange_type = 'direct'
destination_name = 'ex.alternate2'
destination_exchange_type = 'fanout'
alternate_exchange_ob.declare_exchange(source_name, source_exchange_type)
alternate_exchange_ob.declare_exchange(destination_name, destination_exchange_type)
alternate_exchange_ob.declare_queue('queue28.1')
alternate_exchange_ob.declare_queue('queue28.2')
alternate_exchange_ob.declare_queue('unroutedqueue.28')


alternate_exchange_ob.bind_exchange_to_exchange(source_name, destination_name, key='other')
alternate_exchange_ob.bind_queue_to_exchange(queue_name='queue28.1', exchange_name=source_name, key='video')
alternate_exchange_ob.bind_queue_to_exchange(queue_name='queue28.2', exchange_name=source_name, key='image')
alternate_exchange_ob.bind_queue_to_exchange(queue_name='unroutedqueue.28', exchange_name=source_name, key='other')

alternate_exchange_ob.publish_message(exchange_name=source_name, message_body='This is a video message', key='video')
alternate_exchange_ob.publish_message(exchange_name=source_name, message_body='This is an image message', key='image')
alternate_exchange_ob.publish_message(exchange_name=source_name, message_body='This is a text message', key='other')


