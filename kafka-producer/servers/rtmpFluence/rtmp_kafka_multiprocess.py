# coding=utf-8
from __future__ import print_function

import json
import multiprocessing
import threading
import time


from kafka import KafkaProducer

from ..mq.RtmpFluenceAPI import mqAPI as MQAPI
from ..config import rtmpFluence_conf as conf
from ..utils import tools

PARTITION_NUM = conf.PARTITION_NUM
LOG_PATH = conf.log_producer_Path
TIMESTAMP = tools.timeFormat('%Y%m%d', int(time.time()))

kfk_brokers = conf.KAFKA_BROKERS
kfk_topic = conf.KAFKA_TOPIC
producer = KafkaProducer(bootstrap_servers=kfk_brokers)


class RtmpProducer(threading.Thread):
    def __init__(self, start_partition, stop_partition):
        super(RtmpProducer, self).__init__()
        self.daemon=True
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
                    time.sleep(2)
                    offsets = MQAPI.getOffset(partition)
                    startOffset, offset, lastOffset = offsets['startOffset'], offsets['offset'], offsets['lastOffset']

                    # 调整远程游标的读取位置
                    if offset < startOffset:
                        offset = startOffset
                        MQAPI.setOffset(partition, offset)
                    if offset >= lastOffset:
                        continue
                    counts = lastOffset - offset
                    # print(kfk_topic+' Starting partition ' + str(partition) + ' 数据条数: ' + str(counts))
                    start = time.clock()
                    while offset < lastOffset:
                        try:
                            data = MQAPI.pullData(partition, offset)
                            nextOffset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
                            data_list = data.get("list", "Error: no list keykowd in this frame")

                            # 处理数据

                            self.deal_with(data_list)

                            # 更新MQ远程和当前游标的状态
                            offset = nextOffset
                        except Exception as e:
                            tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                                         'Error 1: ' + str(partition) + ' ' + str(offset) + ' EX:' + str(e), 1)
                    MQAPI.setOffset(partition, offset)
                    end = time.clock()
                    interval = end - start
                    if interval > 300:
                        tools.logout(LOG_PATH, 'MQ Error', TIMESTAMP,
                                     'Error Pull MQ Data Counts:' + str(counts) + ' time out :' + str(interval), 1)
                        # print('Stop partition ' + str(partition) + ' offset=' + str(offset) + ' lastOffset=' + str(
                        #     lastOffset))

                except Exception as e:
                    tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                                 'Error 2:' + str(partition) + 'EX:' + str(e), 1)

    def deal_with(self, data_list):
        """
        :param data_list: list
        :param partition:
        :return: None
        """
        for data in data_list:
            body = data.get('body', 'Error:no body keyword').replace('\\', '')

            try:
                body_dict = json.loads(body)
                producer.send(kfk_topic, key=bytes(kfk_topic),
                              value=bytes(json.dumps(body_dict, ensure_ascii=False)))

            except Exception as e:
                print(e)
                pass
                # offset = data.get('offset', 'Error:no offset keyword')
                # tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                #              str(e) + ' Error data: partition: ' + str(partition) + ' offset: ' + str(
                #                  offset) + '\n' + str(data), 3)


if __name__ == '__main__':
    RtmpProducer(0, 1).start()
    time.sleep(5000)
