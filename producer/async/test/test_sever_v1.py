# coding=utf-8
from __future__ import print_function

import time
import sys

from kafka import SimpleClient
from kafka import SimpleProducer
# from kafka.partitioner import HashedPartitioner
# from kafka.partitioner import RoundRobinPartitioner

from MQ.NodeHlsAPI import nodeHlsAPI as MQAPI
from config import conf
import tools

PARTITION_NUM = conf.PARTITION_NUM
logPath = conf.logPath
timeYmd = conf.timeYmd
n = 0



def deal_with(data_list, producer, kfk_topic, partition):
    """
    :param data_list: list
    :param producer: SimpleProducer
    :param kfk_topic: str
    :return:
    """
    for data in data_list:
        body = data.get('body', 'Error:no body keyword')
        if body.endswith('}') and body.startswith('{'):
            global n
            n += 1
            if n < 10000000:
                producer.send_messages(kfk_topic, bytes(body.replace('\\', '')))
            else:
                print('发送数据已经达到上限')
        else:
            offset = data.get('offset', 'Error:no offset keyword')
            tools.logout(logPath, 'hls', timeYmd,
                         'Error data: partition: ' + str(partition) + ' offset: ' + str(offset) + '\n' + str(data), 3)


def response(producer, kfk_topic):
    """
    相关MQ的API调用应当再次封装一层，然后通读所有的partition，封装效果：方法调用不必传入参数
    :param producer:
    :param kfk_topic:
    :return:
    """
    # 解析处理数据这部分，应该单独拿出来，继承MQAPI，然后做一个nodehlsAPI
    # kafka应当只负责从各个MQ分区(或者循环接收也丢弃)接受格式化数据并发布到队列当中
    while True:
        for partition in range(PARTITION_NUM + 1):
            try:
                offsets = MQAPI.getOffset(partition)
                startOffset, offset, lastOffset = offsets['startOffset'], offsets['offset'], offsets['lastOffset']

                # 调整远程游标的读取位置
                if startOffset < offset < lastOffset:
                    pass
                else:
                    offset = startOffset
                    MQAPI.setOffset(partition, offset)

            except Exception as e:
                # 此处发生异常很大一部分只能是网络原因,跳出循环尝试读取下一个分区
                tools.logout(logPath, 'hls', timeYmd,
                             'Error: ' + str(partition) + 'EX:' + str(e), 1)
                print(str(e))
                break

            while True:
                try:
                    data = MQAPI.pullData(partition, offset)
                    nextOffset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
                    data_list = data.get("list", "Error: no list keykowd in this frame")

                    # 处理数据
                    deal_with(data_list, producer, kfk_topic, partition)

                    # 更新MQ远程和当前游标的状态
                    MQAPI.setOffset(partition, nextOffset)
                    offset = nextOffset
                    if offset >= lastOffset:
                        # 如果更新过的当前游标大于等于最后一个游标,立即跳出循环去读取下一个分区的记录
                        break
                except Exception as e:
                    tools.logout(logPath, 'hls', timeYmd,
                                 'Error: ' + str(partition) + ' ' + str(offset) + ' EX:' + str(e), 1)
                finally:
                    time.sleep(0.1)


def run(brokers, topic):
    client = SimpleClient(brokers)
    producer = SimpleProducer(client, async=True)
    response(producer, topic)


if __name__ == '__main__':
    # brokers, topic = sys.argv[1:]
    brokers = ["192.168.1.72:9092", "192.168.1.150:9092"]
    brokers = ','.join(brokers)
    topic = 'nodeHls'
    run(brokers, topic)
    time.sleep(5000)
