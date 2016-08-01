# coding=utf-8
# kafka同步生产模型，必须收到确认，否则程序就会停止发送（客户端至少发送一次确认）
# kafka异步生产模型，放入到发送队列中，队列满了的话就会丢掉，或者将整个队列发送

# 软件环境，已经搭建好的kafka集群，linux服器一台，python2.7，kafka-python软件包
# 同步和异步在程序上的区别仅仅在于SimpleProducer()中async的参数是否为True

# import threading, time
import time
from kafka import SimpleClient
from kafka import SimpleProducer


def run():
    client = SimpleClient("localhost:9092")
    producer = SimpleProducer(client, async=True)
    # producer = KeyedProducer(client, partitioner=HashedPartitioner,async=True)

    while True:
        print('test')
        producer.send_messages('test', 'test')

        producer.send_messages('test', 'test')

        time.sleep(1)

if __name__ == '__main__':
    run()