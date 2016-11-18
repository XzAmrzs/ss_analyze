# coding=utf-8
from __future__ import print_function

import json
from multiprocessing import Process, JoinableQueue
from threading import Thread
import time

from kafka import KafkaProducer

from servers.config import nodeHls_conf as conf
from servers.utils import tools

LOG_PATH = conf.log_producer_Path

kfk_brokers = conf.KAFKA_BROKERS
kfk_topic = conf.KAFKA_TOPIC
producer = KafkaProducer(bootstrap_servers=kfk_brokers)


def run():
    with open('/zookeeper_data/topic_data/nodeHls', 'r') as f:
        deal_with(f)


def deal_with(data_list):
    """
    :param data_list: list
    :param partition:
    :return: None
    """
    for data in data_list:
        try:
            producer.send(kfk_topic, key=bytes(kfk_topic),
                          value=bytes(json.dumps(data, ensure_ascii=False)))
        except Exception as e:
            print(e)
            # offset = data.get('offset', 'Error:no offset keyword')
            # tools.logout(LOG_PATH, kfk_topic, TIMESTAMP,
            #              str(e) + ' Error data: partition: ' + str(partition) + ' offset: ' + str(
            #                  offset) + '\n' + str(data), 3)


if __name__ == '__main__':
    run()
