# coding=utf-8
from __future__ import print_function

import time, json
from kafka import KafkaProducer
from servers.mq.NodeHlsAPI import mqAPI as MQAPI
from servers.config import nodeHls_conf as conf
from servers.utils import tools
from servers.utils.ipip import IP

PARTITION_NUM = conf.PARTITION_NUM
LOG_PATH = conf.log_producer_Path
TIMESTAMP = tools.timeFormat('%Y%m%d', int(time.time()))

import os
import sys
import time

reload(sys)
sys.setdefaultencoding("utf-8")
a = os.path.abspath("mydata4vipweek2.dat")
IP.load(a)


def deal_with(data_list, producer, kfk_topic, partition):
    """
    :param data_list: list
    :param producer: SimpleProducer
    :param kfk_topic: str
    :return: None
    """
    for data in data_list:
        body = data.get('body', 'Error:no body keyword').replace('\\', '')

        try:
            body_dict = json.loads(body)

            # 进行ip查找地区的数据预处理
            remote_addr = body_dict.get('remote_addr', 'error_remote_addr')
            location = IP.find(remote_addr).split('\t')[1]
            del body_dict['remote_addr']
            body_dict['location'] = location
            message = json.dumps(body_dict, ensure_ascii=False)
            producer.send(kfk_topic, key=bytes(kfk_topic), value=bytes(message))

        except Exception as e:
            offset = data.get('offset', 'Error:no offset keyword')
            tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                         str(e) + ' Error data: partition: ' + str(partition) + ' offset: ' + str(
                             offset) + '\n' + str(data), 3)


def response(producer, kfk_topic, partition_range):
    """
    相关MQ的API调用应当再次封装一层，然后通读所有的partition，封装效果：方法调用不必传入参数
    :param producer:
    :param kfk_topic:
    :return:
    """
    # 解析处理数据这部分，应该单独拿出来，继承MQAPI，然后做一个nodehlsAPI
    # kafka应当只负责从各个MQ分区(或者循环接收也丢弃)接受格式化数据并发布到队列当中
    while True:
        for partition in range(*partition_range):
            try:
                time.sleep(3)
                offsets = MQAPI.getOffset(partition)
                startOffset, offset, lastOffset = offsets['startOffset'], offsets['offset'], offsets['lastOffset']

                # 调整远程游标的读取位置
                if offset < startOffset:
                    offset = startOffset
                    MQAPI.setOffset(partition, offset)
                if offset >= lastOffset:
                    continue
                counts = lastOffset - offset
                print('Starting partition ' + str(partition) + ' 数据条数: ' + str(counts))
                start = time.clock()
                while offset < lastOffset:
                    try:
                        data = MQAPI.pullData(partition, offset)
                        nextOffset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
                        data_list = data.get("list", "Error: no list keykowd in this frame")

                        # 处理数据

                        deal_with(data_list, producer, kfk_topic, partition)

                        # 更新MQ远程和当前游标的状态
                        offset = nextOffset
                    except Exception as e:
                        tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                                     'Error 1: ' + str(partition) + ' ' + str(offset) + ' EX:' + str(e), 1)
                MQAPI.setOffset(partition, offset)
                end = time.clock()
                interval = end - start
                if interval > 300:
                    tools.logout(LOG_PATH, 'MQ_Error', TIMESTAMP,
                                 'Error Pull MQ Data Counts:' + str(counts) + ' time out :' + str(interval), 1)
                    # print('Stop partition ' + str(partition) + ' offset=' + str(offset) + ' lastOffset=' + str(
                    #     lastOffset))

            except Exception as e:
                tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
                             'Error 2:' + str(partition) + 'EX:' + str(e), 1)


# 子进程要执行的代码
def run_proc(partition_range):
    brokers = conf.KAFKA_BROKERS
    topic = conf.KAFKA_TOPIC
    producer = KafkaProducer(bootstrap_servers=brokers)
    response(producer, topic, partition_range)


if __name__ == '__main__':

    from multiprocessing import Process
    
    threads = {
        Process(target=run_proc, args=((0, 2),)),
        #Process(target=run_proc, args=((2, 4),)),
        #Process(target=run_proc, args=((4, 6),)),
        #Process(target=run_proc, args=((6, 8),)),
        #Process(target=run_proc, args=((8, 10),))
    }

    for t in threads:
        t.start()

    for t in threads:
        t.join()
