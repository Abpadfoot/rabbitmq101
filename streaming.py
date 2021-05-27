from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
# from pyspark.streaming.mqtt import MQTTUtils


sc = SparkContext()
ssc = StreamingContext(sc, 10)
sc.setLogLevel("OFF")
# lines = ssc.textFileStream('/home/bluepi/PycharmProjects/rabbitmq101/data/')
mqttStream = MQTTUtils.createStream(
    ssc,
    "localhost:1883",  # Note both port number and protocol
    "queue_errors",
    username='guest',
    password='guest'
)
# lines = ssc.socketTextStream('localhost', 9999)
words = mqttStream.flatMap(lambda x: x.split(" "))
wordCounts = words.map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y)
print(wordCounts.pprint())
# print(wordCounts.pprint())

ssc.start()
ssc.awaitTermination()
