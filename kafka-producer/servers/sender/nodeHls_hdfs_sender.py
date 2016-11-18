# coding=utf-8
from __future__ import print_function

import json
import os
from threading import Thread
import time


from servers.mq.NodeHlsAPI import mqAPI as MQAPI
from servers.config import nodeHls_conf as conf
from servers.utils import tools

PARTITION_NUM = conf.PARTITION_NUM
LOG_PATH = conf.log_producer_Path
TIMESTAMP = tools.timeFormat('%Y%m%d', int(time.time()))
mq_topic = conf.MQ_TOPIC
dataRootPath = conf.dataRootPath

class NodeHlsProducer(Thread):
    def __init__(self, start_partition, stop_partition):
        super(NodeHlsProducer, self).__init__()
        self.daemon = True
        # 自定义变量
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
                    if offset < startOffset:
                        offset = startOffset
                        MQAPI.setOffset(partition, offset)
                    if offset >= lastOffset:
                        continue
                    counts = lastOffset - offset
                    # print(mq_topic + ' Starting partition ' + str(partition) + ' 数据条数: ' + str(counts))
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
                            tools.logout(LOG_PATH, mq_topic, TIMESTAMP,
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
                    tools.logout(LOG_PATH, mq_topic, TIMESTAMP,
                                 'Error 2:' + str(partition) + 'EX:' + str(e), 1)

    def deal_with(self, data_list):
        """
        :param data_list: list
        :param partition:
        :return: None
        """
        for data in data_list:
            body = data.get('body', 'Error:no body keyword').replace('\\', '')
            offset = data.get('offset','Error: no offset keyword')

            try:
                body_dict = json.loads(body)

                bodyTimeYmd = tools.timeFormat("%Y%m%d", int(body_dict.get("unix_time", int(time.time()))))
                path = "/".join([dataRootPath, mq_topic])
                fileName = bodyTimeYmd
                self.saveToLocal(path, fileName, body)

            except Exception as e:
                print(e)

    def saveToLocal(self, filePath, fileName, data):

        # 0 if hdfs else hdfs = pyhdfs.HdfsClient("master")
        0 if os.path.exists(filePath) else os.makedirs(filePath)
        with open(filePath + '/' + fileName, 'a+') as fi:
            fi.writelines(data + '\n')
        return filePath

if __name__ == '__main__':
    pass
