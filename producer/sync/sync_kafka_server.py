# coding=utf-8

import threading, time

from kafka import SimpleClient
from kafka.producer import SimpleProducer


# from kafka.partitioner import HashedPartitioner
# from kafka.partitioner import RoundRobinPartitioner


class SyncProducer(threading.Thread):
    deamon = True

    def run(self):
        client = SimpleClient("192.168.1.96:9092")

        producer = SimpleProducer(client)
        # producer = KeyedProducer(client,partitioner=HashedPartitioner)

        while True:
            producer.send_messages('test', 'test')
            producer.send_messages('test', 'test')
            time.sleep(1)
