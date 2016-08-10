#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/4 0004 上午 10:13

import json
from pymongo import MongoClient
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition,OffsetRange

from utils import tools
from conf import conf

database_name = conf.DATABASE_NAME
collection_name = conf.COLLECTION_NAME
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

logPath = conf.LOG_PATH
timeYmd = conf.TIME_YMD

kafka_brokers = conf.KAFKA_BROKERS
kafka_topic = conf.KAFKA_TOPIC
zk_servers = conf.ZK_SERVERS

app_name = conf.APP_NAME
checkpoint_dir = conf.CHECKPOINT_DIR

offsetRanges = []


def store_offset_ranges(rdd):
    """保存当前rdd的游标范围信息"""
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def set_zk_offset(rdd):
    """设置zookeeper的游标"""
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    for offsetRange in offsetRanges:
        nodePath = '/'.join(
            ['/consumers', 'spark-group', 'offsets', str(offsetRange.topic), str(offsetRange.partition)])
        # 如果存在这个分区，更新分区，否则，新建这个分区并且把游标移动到这次读完的位置(或者这次开始读的位置,再判断)
        if zk.exists(nodePath):
            zk.set(nodePath, offsetRange.untilOffset)
        else:
            zk.create(nodePath, offsetRange.untilOffset, makepath=True)
    zk.stop()


def get_last_offsets(zkServers, groupID, topic):
    """
    返回zk中此消费者消费当前topic中的每个partition的offset
    :param zkServers:
    :param groupID:
    :param topic:
    :return: [(partition1,offset1), ..., (partitionN,offsetN)]
    """
    zk = KazooClient(zkServers, read_only=True)
    zk.start()

    retvals = {}

    try:
        nodePath = '/'.join(['/consumers', groupID, 'offsets', topic])
        if zk.exists(nodePath):
            for partition in zk.get_children(nodePath):
                offset, stat = zk.get('/'.join([nodePath, partition]))
                topicAndPartition = TopicAndPartition(topic, int(partition))
                retvals[topicAndPartition] = long(offset)
    except KazooException as e:
        print(e)
    finally:
        zk.stop()
    return retvals


def json2dict(s):
    """
    :param s: str
    :return: dict
    """
    try:
        dit = json.loads(s[1], encoding='utf-8')
        return dit
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + s[1] + ' ' + str(e), 3)
        return {}


def get_user_and_flux(body_dict):
    """
    :param body_dict: dict
    :return: type:(str,int) -> ('user',flux)
    """
    user, flux = body_dict.get('user', "no user keyword"), body_dict.get('body_bytes_sent', 0)
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

    官方文档提示采用lazy模式的连接池来进行数据的入库操作 参考 : http://shiyanjun.cn/archives/1097.html
    关键点: 实现数据库的(序列化)和(反序列化)接口，或者也可以避免,理想效果是只是传入sql语句即可??
    """
    try:
        client = MongoClient(database_driver_host, database_driver_port)
        db = client.get_database(database_name)
        nodeHls_col = db.get_collection(collection_name)
        for record in iter:
            nodeHls_col.update_one({"user": record[0]}, {"$set": {"flux": record[1]}}, True)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def create_context(brokers, topic):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName=app_name)
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint(checkpoint_dir)

    # 分别从Kafka中获得某个Topic当前每个partition的offset
    # topicOffsets = get_topic_offsets(brokers, topic)

    # 再从Zookeeper中获得某个consumer消费当前Topic中每个partition的offset
    lastOffsets = get_last_offsets(zk_servers, "spark-group", topic)
    print(lastOffsets)

    # 在初始化 kafka stream 的时候
    # 查看 zookeeper 中是否保存有 offset
    # 有就从该 offset 进行读取，
    # 没有就从最新/旧进行读取。

    # 设置kafka的brokers
    kafkaParams = {"metadata.broker.list": brokers}
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams, fromOffsets=lastOffsets)

    # 在消费 kafka 数据的同时, 将每个 partition 的 offset 保存到 zookeeper 中进行备份
    kvs.transform(store_offset_ranges).foreachRDD(set_zk_offset)

    # json格式转dict格式
    body_dict = kvs.map(json2dict)

    # 获取用户的流量信息
    user_flux = body_dict.map(get_user_and_flux)

    # 累加用户流量
    running_counts = user_flux.updateStateByKey(update_func)

    # 数据输出保存(这里要注意Dstream->RDD->单个元素,要遍历三层才能获得单个元素)
    running_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))

    return ssc


if __name__ == '__main__':
    ssc = StreamingContext.getOrCreate(checkpoint_dir, lambda: create_context(kafka_brokers, kafka_topic))
    ssc.start()
    ssc.awaitTermination()
