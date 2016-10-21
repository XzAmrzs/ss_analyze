#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/9/27 0027 下午 4:44
from kafka import KafkaConsumer,TopicPartition

brokers = 'test1:9092'
topics = ['nodeHlsTest']

# consumer = KafkaConsumer(bootstrap_servers=brokers)
# consumer.subscribe(topics)

# msg=iter(consumer).next()
# print(msg)

# for msg in consumer:
#     print (msg)
# consumer.close()
from pyspark.streaming.kafka import TopicAndPartition

consumer = KafkaConsumer(bootstrap_servers=brokers)
current_offsets = {}

for topic in topics:
    partitions = consumer.partitions_for_topic(topic)
    l = []
    for tp in partitions:
        l.append(TopicPartition(topic, tp))

    consumer.assign(l)

    for tp in partitions:
        consumer.seek_to_end(TopicPartition(topic, tp))

        msg = consumer.next()

        topicAndPartition = TopicAndPartition(msg.topic, msg.partition)
        current_offsets[topicAndPartition] = msg.offset

consumer.close()

print(current_offsets)