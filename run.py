#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 上午 9:08

import json
import time
import re
from pymongo import MongoClient
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

# 添加项目依赖环境
import sys

sys.path.append('/home/xzp/HLS_analyze/config/')

import conf
import logging


def timeFormat(format, intTime):
    returnTime = time.strftime(format, time.localtime(intTime))
    return returnTime


def logout(logRootPath, logName, timeYMD, message, level):
    # 创建一个logger
    logger = logging.getLogger(logName)
    logger.setLevel(logging.DEBUG)
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler(logRootPath + logName + '-' + timeYMD)
    fh.setLevel(logging.DEBUG)
    # 再创建一个handler，用于输出到控制台
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # ch.setFormatter(formatter)
    # 给logger添加handler
    logger.addHandler(fh)
    # logger.addHandler(ch)
    # 记录一条日志
    if level is 1:
        logger.info(message)
    elif level is 2:
        logger.warning(message)
    elif level is 3:
        logger.error(message)
    return 1


database_name = conf.DATABASE_NAME
collection_name = conf.COLLECTION_NAME
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

logPath = conf.LOG_PATH
timeYmd = timeFormat('%Y%m%d', int(time.time()))

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
    zk = KazooClient(hosts=zk_servers)
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


def valid_func(s):
    return ('.m3u8' in s) or ('.ts' in s)


def json2dict(s):
    """
    :param s: str
    :return: dict
    """
    try:
        dit = json.loads(s, encoding='utf-8')
        return dit
    except Exception as e:
        logout(logPath, app_name, timeYmd, 'Error Load Json: ' + s[1] + ' ' + str(e), 3)
        return {}


def get_user_app_stream_flux(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    # 获取APP和stream
    request_url = body_dict.get('request_url', 'error_request_url')
    # 这里不能用简单的'/'来分割,会碰到请求的url有多个'/'的情况
    # GET http://hzsrzj/hzsrzj-stream-1470988831-1470991945170-3042.ts HTTP/1.1
    # 所以解析的时候直接全转成空格，然后按照任意长度的空格来分割
    request_url_list = request_url.replace('/', ' ')
    app_stream = re.split(' *', request_url_list)
    app = app_stream[1]
    try:
        if '.m3u8' in request_url:
            # 'GET /szqhqh/stream.m3u8 HTTP/1.1'
            stream = app_stream[2].split('.')[0]
        else:
            if 'http' in request_url:
                app = app_stream[-4]
                stream = app_stream[-3].split('-')[1]
            else:
                # 'GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1'
                stream = app_stream[2].split('-')[1]
    except Exception as e:
        logout(logPath, app_name, timeYmd, 'Error: ' + request_url + str(app_stream) + str(e), 1)
        stream = 'error'
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    user = body_dict.get('user', "no user keyword")
    flux = body_dict.get('body_bytes_sent', 0)
    unix_time = timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    user_app_stream_flux = (user, unix_time, app, stream), flux
    return user_app_stream_flux


def store_user_flux(iter, col_names):
    """
    :param iter: abc
    :param col_names: list
    :return: None

    官方文档提示采用lazy模式的连接池来进行数据的入库操作 参考 : http://shiyanjun.cn/archives/1097.html
    关键点: 实现数据库的(序列化)和(反序列化)接口，或者也可以避免,理想效果是只传入sql语句即可??
    """

    try:
        client = MongoClient(database_driver_host, database_driver_port)
        db = client.get_database(database_name)
        dao_store_and_update(db, col_names, iter)
        client.close()
    except Exception as e:
        logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, col_names, iter):
    for record in iter:
        user, timestamp, app, stream = record[0]
        flux = record[1]
        data = {"user": user, "timestamp": timestamp}
        for index, col_name in enumerate(col_names):
            col = db.get_collection(col_name)
            if index == 1:
                data['app'] = app
            if index == 2:
                data['stream'] = stream
            update_dit = {'$inc': {'flux': flux}}
            col.update(data, update_dit, True)
            del data


def create_context(brokers, topic):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName=app_name)

    sc.addPyFile('./config/conf.py')

    ssc = StreamingContext(sc, 10)
    lastOffsets = get_last_offsets(zk_servers, "spark-group", topic)
    kafkaParams = {"metadata.broker.list": brokers}

    # kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams, fromOffsets=lastOffsets)
    kvs.checkpoint(60)

    # json格式转dict格式
    body_dict = kvs.map(lambda x: x[1]).filter(valid_func).map(json2dict)
    user_app_stream_flux_counts = body_dict.map(get_user_app_stream_flux).reduceByKey(lambda x, y: x + y)

    # 数据输出保存(这里要注意Dstream->RDD->单个元素,要遍历三层才能获得单个元素)
    user_app_stream_flux_counts.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_user_flux(x, col_names=('user_app', 'user_app_flux', 'user_app_stream_flux'))))

    # 在消费完kafka数据, 将每个 partition 的 offset记录更新到zookeeper
    kvs.transform(store_offset_ranges).foreachRDD(set_zk_offset)

    return ssc


if __name__ == '__main__':
    ssc = StreamingContext.getOrCreate(checkpoint_dir, lambda: create_context(kafka_brokers, kafka_topic))
    ssc.start()
    ssc.awaitTermination()
