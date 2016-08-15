#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/4 0004 上午 10:13

import json
import time
import re
from pymongo import MongoClient
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

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
            zk.set(nodePath, bytes(offsetRange.untilOffset))
        else:
            zk.create(nodePath, bytes(offsetRange.untilOffset), makepath=True)
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
        dit = json.loads(s, encoding='utf-8')
        return dit
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + ' ' + str(e), 3)
        return {}


def get_user_app_stream_flux(body_dict):
    """
    :param body_dict: dict
    :return: type:(str,int,int) ->
            ('1234',"1470898486","GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1",448)
    """
    # 获取APP和stream
    request_url = body_dict.get('request_url', 'error_request_url')
    # 这里不能用简单的'/'来分割,会碰到请求的url有多个'/'的情况
    # GET //hzsrzj/hzsrzj-stream-1470988831-1470991945170-3042.ts HTTP/1.1
    # 所以解析的时候直接全转成空格，然后按照任意长度的空格来分割
    request_url_list = request_url.replace('/',' ')
    app_stream = re.split(' *', request_url_list)
    app = app_stream[1]
    try:
        if '.m3u8' in request_url:
            # 'GET /szqhqh/stream.m3u8 HTTP/1.1'
            stream = app_stream[2].split('.')[0]
        else:
            # 'GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1'
            stream = app_stream[2].split('-')[1]
    except Exception as e: 
            tools.logout(logPath, app_name, timeYmd, 'Error: '+request_url+ str(app_stream) + str(e), 1)
            stream = 'error'
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    user = body_dict.get('user', "no user keyword")
    flux = body_dict.get('body_bytes_sent', 0)
    unix_time = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    user_app_stream_flux = (user,  unix_time, app, stream), flux
    return user_app_stream_flux


def get_user_app_flux(user_app_stream_flux):
    (user, unix_time, app, stream), flux = user_app_stream_flux
    user_app_flux = (user, unix_time, app), flux
    return user_app_flux


def get_user_flux(user_app_flux):
    (user, unix_time, app), flux = user_app_flux
    user_flux = (user, unix_time), flux
    return user_flux

# updata flux
def update_func(new_values, last_sum):
    """
    :param new_values: int
    :param last_sum: int
    :return: int
    """
    return sum(new_values) + (last_sum or 0)


def store_user_flux(iter,col_name):
    """
    :param iter: list
    :return: None

    官方文档提示采用lazy模式的连接池来进行数据的入库操作 参考 : http://shiyanjun.cn/archives/1097.html
    关键点: 实现数据库的(序列化)和(反序列化)接口，或者也可以避免,理想效果是只是传入sql语句即可??
    """
    try:
        client = MongoClient(database_driver_host, database_driver_port)
        db = client.get_database(database_name)
        dao_store_and_update(db, col_name, iter)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, col_name, iter):
    col=db.get_collection(col_name)
    if col_name == 'user_app_stream_flux':
        for record in iter:
            user, timestamp, app, stream = record[0]
            flux = record[1]
            col.update_one({"user": user, "timestamp": timestamp, "app": app, "stream": stream},{"$set": {"flux": flux}},True)
    if col_name == 'user_app_flux':
        for record in iter:
            user, timestamp, app = record[0]
            flux = record[1]
            col.update_one({"user": user, "timestamp": timestamp, "app": app},{"$set": {"flux": flux}}, True)
    if col_name == 'user_flux':
        for record in iter:
            user, timestamp = record[0]
            flux = record[1]
            col.update_one({"user": user, "timestamp": timestamp},{"$set": {"flux": flux}}, True)
            
def valid_func(s): 
    return ('.m3u8' in s) or ('.ts' in s)

def create_context(brokers, topic):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName=app_name)
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint(checkpoint_dir)

    # lastOffsets = get_last_offsets(zk_servers, "spark-group", topic)
    # kafkaParams = {"metadata.broker.list": brokers}
    host = 'localhost'
    port = '9999'
    kvs = ssc.socketTextStream(host, int(port))
    # kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)
    # kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams, fromOffsets=lastOffsets)
    # 在消费 kafka 数据的同时, 将每个 partition 的 offset 保存到 zookeeper 中进行备份
    # 这句话在生产环境里面要放到最下面，即入库了以后再移动zookeeper下标的值
    # kvs.transform(store_offset_ranges).foreachRDD(set_zk_offset)

    # json格式转dict格式
    kvs = kvs.filter(valid_func)
    body_dict = kvs.map(json2dict)

    # 筛选出本次处理的相关的用户信息
    user_app_stream_flux = body_dict.map(get_user_app_stream_flux)
    user_app_flux = user_app_stream_flux.map(get_user_app_flux)
    user_flux = user_app_flux.map(get_user_flux)

    # 累加用户流量的操作
    user_flux_counts = user_flux.updateStateByKey(update_func)
    user_app_flux_counts = user_app_flux.updateStateByKey(update_func)
    user_app_stream_flux_counts = user_app_stream_flux.updateStateByKey(update_func)

    # 数据输出保存(这里要注意Dstream->RDD->单个元素,要遍历三层才能获得单个元素)
    user_flux_counts.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_user_flux(x, col_name='user_flux')))
    user_app_flux_counts.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_user_flux(x, col_name='user_app_flux')))
    user_app_stream_flux_counts.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_user_flux(x, col_name='user_app_stream_flux')))
    return ssc


if __name__ == '__main__':
    ssc = StreamingContext.getOrCreate(checkpoint_dir, lambda: create_context(kafka_brokers, kafka_topic))
    ssc.start()
    ssc.awaitTermination()
