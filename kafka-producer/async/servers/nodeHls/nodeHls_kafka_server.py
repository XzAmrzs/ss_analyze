# coding=utf-8
from __future__ import print_function

import threading
import time

from kafka import KafkaProducer

from ..MQ.NodeHlsAPI import nodeHlsAPI as MQAPI
from ..config import nodeHls_conf as conf
from ..utils import tools

PARTITION_NUM = conf.PARTITION_NUM
LOG_PATH = conf.log_producer_Path
TIMESTAMP = tools.timeFormat('%Y%m%d', int(time.time()))


class NodeHlsProducer(threading.Thread):
    def __init__(self, start_partition, stop_partition):
        super(NodeHlsProducer, self).__init__()
        self.daemon = True
        # 自定义变量
        self.kfk_brokers = conf.KAFKA_BROKERS
        self.kfk_topic = conf.KAFKA_TOPIC

        self.producer = KafkaProducer(bootstrap_servers=[self.kfk_brokers])

        if start_partition < 0 and stop_partition < PARTITION_NUM:
            raise ValueError("start_partition or stop_partition error ")
        self.partition_range = (start_partition, stop_partition + 1)

    def run(self):
        self.response()

    def response(self):
        """
        相关MQ的API调用应当再次封装一层，然后通读所有的partition，封装效果：方法调用不必传入参数
        :return:
        """
        while True:
            for partition in range(*self.partition_range):
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
                    tools.logout(LOG_PATH, self.kfk_topic, TIMESTAMP,
                                 'Error: ' + str(partition) + 'EX:' + str(e), 1)
                    print(str(e))
                    break

                while True:
                    try:
                        data = MQAPI.pullData(partition, offset)
                        nextOffset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
                        data_list = data.get("list", "Error: no list keykowd in this frame")

                        # 处理数据
                        self.deal_with(data_list, partition)
                        # 更新MQ远程和当前游标的状态
                        MQAPI.setOffset(partition, nextOffset)
                        offset = nextOffset
                        if offset >= lastOffset:
                            # 如果更新过的当前游标大于等于最后一个游标,立即跳出循环去读取下一个分区的记录
                            break
                    except Exception as e:
                        tools.logout(LOG_PATH, self.kfk_topic, TIMESTAMP,
                                     'Error: ' + str(partition) + ' ' + str(offset) + ' EX:' + str(e), 1)
                    finally:
                        time.sleep(0.1)

    def deal_with(self, data_list, partition):
        """
        :param data_list: list
        :param partition:
        :return: None
        """
        for data in data_list:
            body = data.get('body', 'Error:no body keyword')
            if body.endswith('}') and body.startswith('{'):
                self.producer.send(self.kfk_topic, key=bytes(self.kfk_topic), value=bytes(body.replace('\\', '')))
            else:
                offset = data.get('offset', 'Error:no offset keyword')
                tools.logout(LOG_PATH, self.kfk_topic, TIMESTAMP,
                             'Error data: partition: ' + str(partition) + ' offset: ' + str(offset) + '\n' + str(data),
                             3)


if __name__ == '__main__':
    NodeHlsProducer(0, 1).start()
    time.sleep(5000)

