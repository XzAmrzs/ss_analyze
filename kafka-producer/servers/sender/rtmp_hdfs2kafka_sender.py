# coding=utf-8
from __future__ import print_function

import json

from kafka import KafkaProducer

from servers.config import rtmpFluence_conf as conf

LOG_PATH = conf.log_producer_Path

kfk_brokers = conf.KAFKA_BROKERS
kfk_topic = conf.KAFKA_TOPIC
producer = KafkaProducer(bootstrap_servers=kfk_brokers)
root_path = conf.dataRootPath


def run():
    with open(root_path + kfk_topic+'/20161121', 'r') as f:
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

if __name__ == '__main__':
    run()
