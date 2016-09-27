# coding=utf-8
from collections import namedtuple

from kafka import KafkaConsumer
from kafka import TopicPartition
try:
    consumer = KafkaConsumer(('nodeHlsTest','RtmpFluenceTest'), bootstrap_servers=['192.168.1.145:9092','192.168.1.150:9092','192.168.1.138:9092'])
    message = next(consumer)
    # topics=['nodeHlsTest']
    # for topic in topics:
    #     partitions = consumer.partitions_for_topic(topic)
    #     print(consumer.topics())
        # l = []
        # for tp in partitions:
        #     l.append(TopicPartition('nodeHlsTest', tp))
        # consumer.assign(l)
        # for tp in partitions:
    #         consumer.seek_to_end(TopicPartition('nodeHlsTest', tp))
    #
    #     # consumer.seek_to_end()
    #     # consumer.assign([TopicPartition('nodeHlsTest', 0)])
    #     # consumer.seek_to_beginning(TopicPartition('nodeHlsTest', 1))
    #
    #
    #
    #     message=next(consumer)
    #     print ("%s:%d:%d" % (message.topic, message.partition, message.offset))
    print ("%s:%d:%d" % (message.topic, message.partition, message.offset))
    consumer.close()


except Exception as e:
    print(e)

#
# 从zk获取topic/partition 的fromOffset（获取方法链接）已经解决

# 利用SimpleConsumer获取每个partiton的lastOffset（untilOffset）
# 判断每个partition lastOffset与fromOffset的关系
# 当lastOffset < fromOffset时，将fromOffset赋值为0
# 通过以上步骤完成fromOffset的值矫正。


#
# # manually assign the partition list for the consumer
# from kafka import TopicPartition
# consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
# consumer.assign([TopicPartition('foobar', 2)])
# msg = next(consumer)
#
# # Deserialize msgpack-encoded values
# consumer = KafkaConsumer(value_deserializer=msgpack.loads)
# consumer.subscribe(['msgpackfoo'])
# for msg in consumer:
#     assert isinstance(msg.value, dict)
