#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/4 0004 上午 10:13

from __future__ import print_function

import json
from pymongo import MongoClient

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from utils import tools
from conf import conf

logPath = conf.LOG_PATH
timeYmd = conf.TIME_YMD
kafka_brokers = conf.KAFKA_BROKERS
kafka_topic = conf.KAFKA_TOPIC
app_name = conf.APP_NAME
checkpoint_dir = conf.CHECKPOINT_DIR
database_name = conf.DATABASE_NAME
collection_name = conf.COLLECTION_NAME
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT


def json2dict(s):
    """
    :param s: str
    :return: dict
    """
    try:
        dit = json.loads(s[1], encoding='utf-8')
        return dit
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + s[1] + ' ' + str(e), 1)
        return {}


def get_user_and_flux(body_dict):
    """
    :param body_dict: dict
    :return: type:(str,int) -> ('user',flux)
    """
    user = body_dict.get('user', "no user keyword")
    flux = body_dict.get('body_bytes_sent', 0)
    return user, flux


# updata flux
def update_func(new_values, last_sum):
    """
    :param new_values: int
    :param last_sum: int
    :return: int
    """
    return sum(new_values) + (last_sum or 0)


def send_partition(iter):
    """
    :param iter: list
    :return: None
    """
    client = MongoClient(database_driver_host, database_driver_port)
    db = client.get_database(database_name)
    nodeHls_col = db.get_collection(collection_name)
    for record in iter:
        nodeHls_col.update_one({"user": record[0]}, {"$set": {"flux": record[1]}}, True)
    client.close()


def run():
    sc = SparkContext(appName=app_name)
    ssc = StreamingContext(sc, 15)
    ssc.checkpoint(checkpoint_dir)

    brokers = kafka_brokers
    topic = kafka_topic

    # 直接读取kafka的topic, 避免和zookeeper交流
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # json格式转字典
    body_dict = kvs.map(json2dict)

    # 获取用户的流量信息
    user_flux = body_dict.map(get_user_and_flux)

    running_counts = user_flux.updateStateByKey(update_func)

    running_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    run()
