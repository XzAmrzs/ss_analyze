#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 上午 9:08

import json
import time
import re

from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

# 添加项目依赖环境
import sys

sys.path.append('/home/xzp/ss_analyze/config/')
sys.path.append('/home/xzp/ss_analyze/utils/')

import conf
import tools

database_name = conf.DATABASE_NAME
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

logPath = conf.LOG_PATH
timeYmd = tools.timeFormat('%Y%m%d', int(time.time()))

kafka_brokers = conf.KAFKA_BROKERS
kafka_topics = conf.KAFKA_TOPICS
zk_servers = conf.ZK_SERVERS

app_name = conf.APP_NAME
# checkpoint_dir = conf.CHECKPOINT_DIR

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


def get_last_offsets(zkServers, groupID, topics):
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
        for topic in topics:
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


def get_current_offsets(brokers, topics):
    try:

        consumer = KafkaConsumer(brokers, 1)
        # partitions = consumer.partitions_for_topic('nodeHlsTest')

        l = []
        # for tp in partitions:
        #     l.append(TopicPartition('nodeHlsTest', tp))
        #
        # consumer.assign(l)
        # for tp in partitions:
        #     consumer.seek_to_end(TopicPartition('nodeHlsTest', tp))

        # consumer.seek_to_end()
        # consumer.assign([TopicPartition('nodeHlsTest', 0)])
        # consumer.seek_to_beginning(TopicPartition('nodeHlsTest', 1))

        for message in consumer:
            #     message value and key are raw bytes -- decode if necessary!
            #     e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d" % (message.topic, message.partition, message.offset))
            # print ("%s:%d:%d" % (message.topic, message.partition, message.offset))
        consumer.close()

    except Exception as e:
        print(e)


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
        tools.logout(logPath, app_name, timeYmd, 'Error Load Json: ' + s[1] + ' ' + str(e), 3)
        return {}


def hlsParser(body_dict):
    """
    获取hls用户数据的函数
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
    try:
        app = app_stream[1]
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
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + request_url + str(app_stream) + str(e), 1)
        app = 'error'
        stream = 'error'
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    user = body_dict.get('user', "no user keyword")
    flux = body_dict.get('body_bytes_sent', 0)
    timestamp = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    data = (user, timestamp, app, stream), flux
    return data


def rtmpParser(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    user = body_dict.get('User', "no user keyword")
    timestamp = tools.timeFormat('%Y%m%d', float(body_dict.get('EndTime', 0)))
    app = body_dict.get('App', 'no App keyword')
    stream = body_dict.get('Stream', 'no Stream keyword')
    cmd = body_dict.get('Cmd', '')
    flux = 0
    try:
        if cmd == 'play':
            flux = body_dict.get('SendByteSum', 0)
        else:
            flux = body_dict.get('RecvByteSum', 0)
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: rtmpParser' + str(e), 1)
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    data = (user, timestamp, app, stream, cmd), flux
    return data


def store_data(iter, flag, col_names):
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
        dao_store_and_update(db, flag, col_names, iter)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, flag, col_names, iter):
    for record in iter:
        if flag == 'nodeHls':
            user, timestamp, app, stream = record[0]
            data = {"timestamp": timestamp}
        else:
            user, timestamp, app, stream, cmd = record[0]
            data = {"timestamp": timestamp, "cmd": cmd}
        flux = record[1]

        for index, col_name in enumerate(col_names):
            col = db.get_collection(col_name)

            if index != 1 and user == 'no user keyword':
                # 如果是上行数据,那么只更新user_flux表
                continue
            if index == 1:
                data['user'] = user
            if index == 2:
                data['app'] = app
            if index == 3:
                data['stream'] = stream
            update_dit = {'$inc': {'flux': flux}}
            col.update(data, update_dit, True)


if __name__ == '__main__':
    sc = SparkContext(appName=app_name)

    sc.addPyFile('./config/conf.py')
    sc.addPyFile('./utils/tools.py')

    ssc = StreamingContext(sc, 15)

    lastOffsets = get_last_offsets(zk_servers, "spark-group", kafka_topics)
    # currentoffsets = get_current_offsets(kafka_brokers, kafka_topics)

    # if lastOffsets < currentoffsets:
    #     pass

    # 利用SimpleConsumer获取每个partiton的lastOffset（untilOffset）
    # 判断每个partition lastOffset与fromOffset的关系
    # 当lastOffset < fromOffset时，将fromOffset赋值为0
    # 通过以上步骤完成fromOffset的值矫正。

    kafkaParams = {"metadata.broker.list": kafka_brokers}

    # streams = KafkaUtils.createDirectStream(ssc, ['nodeHlsTest'], kafkaParams)
    streams = KafkaUtils.createDirectStream(ssc, ['nodeHlsTest'], kafkaParams, fromOffsets=lastOffsets)

    # ################### nodeHls数据处理 #############################
    nodeHls_body_dict = streams.filter(lambda x: x[0] == 'nodeHlsTest').map(lambda x: x[1]).filter(valid_func).map(json2dict)
    # user_time_app_stream_flux
    nodeHls_utasf_counts = nodeHls_body_dict.map(hlsParser).reduceByKey(lambda x, y: x + y)
    nodeHls_utasf_counts.foreachRDD(
        lambda rdd: rdd.foreachPartition(lambda x: store_data(x, 'nodeHls', col_names=(
            'hls_timestamp_flux', 'hls_user_flux', 'hls_user_app_flux', 'hls_user_app_stream_flux'))))

    # ################### rtmp数据处理 #############################
    # json格式转dict格式
    # rtmp_body_dict = streams.filter(lambda x: x[0] == 'RtmpFluence').map(lambda x: x[1]).map(json2dict)
    # # user_time_app_stream_cmd_flux
    # rtmp_utascf_counts = rtmp_body_dict.map(rtmpParser).reduceByKey(lambda x, y: x + y)
    # 数据输出保存(这里要注意Dstream->RDD->单个元素,要遍历三层才能获得单个元素)
    # rtmp_utascf_counts.foreachRDD(
    #     lambda rdd: rdd.foreachPartition(
    #         lambda x: store_data(x, 'RtmpFluence',col_names=(
    #             'rtmp_user_flux', 'rtmp_user_app_flux', 'rtmp_user_app_stream_flux'))))

    # 在消费完kafka数据, 将每个 partition 的 offset记录更新到zookeeper
    streams.transform(store_offset_ranges).foreachRDD(set_zk_offset)

    ssc.start()
    ssc.awaitTermination()
