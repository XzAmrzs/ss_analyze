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
import os

sys.path.append('/home/xzp/ss_analyze/config/')
sys.path.append('/home/xzp/ss_analyze/utils/')

import conf
import tools

database_name = conf.DATABASE_NAME
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT
database_cols_up = conf.DATABASE_TABLES_HLS_R
database_cols_down = conf.DATABASE_TABLES_HLS_S

logPath = conf.LOG_PATH
timeYmd = tools.timeFormat('%Y%m%d', int(time.time()))

kafka_brokers = conf.KAFKA_BROKERS
kafka_topics = conf.KAFKA_TOPICS
zk_servers = conf.ZK_SERVERS

app_name = conf.APP_NAME
# checkpoint_dir = conf.CHECKPOINT_DIR

offsetRanges = []


def json2dict(s):
    """
    :param s: str
    :return: dict
    """
    try:
        dit = json.loads(s[1], encoding='utf-8')
        return dit
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error Load Json: ' + s[1] + ' ' + str(e), 3)
        return {}


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

    last_offsets = {}

    try:
        for topic in topics:
            nodePath = '/'.join(['/consumers', groupID, 'offsets', topic])
            if zk.exists(nodePath):
                for partition in zk.get_children(nodePath):
                    offset, stat = zk.get('/'.join([nodePath, partition]))
                    topicAndPartition = TopicAndPartition(topic, int(partition))
                    last_offsets[topicAndPartition] = long(offset)
    except KazooException as e:
        print(e)
    finally:
        zk.stop()
    return last_offsets


# 筛选出nodehls主题的message
def nodeHls_filter(s):
    return s[0] == 'nodeHlsTest'


# 筛选出nodehlshls主题中的有效数据
def nodeHls_valid_filter(s):
    a = ('.m3u8' in s.get('request_url', 'error_request_url')) or ('.ts' in s.get('request_url', 'error_request_url'))
    b = s.get('http_stat', 202) != 202
    return a and b


# 筛选出下行数据
def nodeHls_recv_filter(s):
    return s.get('svr_type', 0) != 1


# 筛选出上行数据
def nodeHls_send_filter(s):
    return s.get('svr_type', 0) == 1


def hlsParser(body_dict):
    """
    获取hls用户数据的函数
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    # 获取APP和stream
    request_url = body_dict.get('request_url', 'error_request_url')
    location = body_dict.get('location', 'error_location')
    server_addr = body_dict.get('server_addr', 'error_server_addr')
    svr_type = body_dict.get('svr_type', 0)

    # 这里不能用简单的'/'来分割,会碰到请求的url有多个'/'的情况
    # GET http://hzsrzj/hzsrzj-stream-1470988831-1470991945170-3042.ts HTTP/1.1
    # 所以解析的时候直接全转成空格，然后按照任意长度的空格来分割
    request_url_list = request_url.replace('/', ' ')
    app_stream = re.split(' *', request_url_list)
    request_time = float(body_dict.get('request_time', '0.000'))
    try:
        app = app_stream[1]
        if '.m3u8' in request_url:
            # 'GET /szqhqh/stream.m3u8 HTTP/1.1'
            stream = app_stream[2].split('.')[0]
            # m3u8判断是否流畅
            is_fluency = 1 if request_time < 1 else 0
            # hls类型，True表示为m3u8,False表示为ts
            hls_type = True
        else:
            if 'http' in request_url:
                app = app_stream[-4]
                item = app_stream[-3].split('-')
            else:
                # 'GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1'
                item = app_stream[2].split('-')
            stream = item[1]
            time_length = item[4].split('.')[0]
            # ts判断是否流畅 切片时间大于3倍的请求时间说明是流畅的
            is_fluency = 1 if time_length > 3 * request_time else 0
            hls_type = False
    except Exception as e:
        print (e)
        app = 'error'
        # stream = 'error'
        is_fluency = 'error'
        hls_type = 'error'
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    # user = body_dict.get('user', "no user keyword")
    # flux = body_dict.get('body_bytes_sent', 0)
    timestamp = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    # 如果状态位置取不出来，说明该数据无效，仍是失败数据,直接去取400
    # is_not_less_than
    is_nlt_400 = 1 if body_dict.get('http_stat', 400) >= 400 else 0
    # is_more_than
    is_mt_1 = 1 if request_time > 1 else 0
    is_mt_3 = 1 if request_time > 3 else 0

    data = {'svr_type': svr_type, 'hls_type': hls_type, 'timestamp': timestamp, 'app': app, 'location': location,
            'server_addr': server_addr,
            'is_fluency': is_fluency, 'is_nlt_400': is_nlt_400, 'is_mt_1': is_mt_1, 'is_mt_3': is_mt_3,
            'request_time': request_time, 'count': 1}
    # data = (svr_type, hls_type, timestamp, app, location, server_addr), (is_fluency, is_nlt_400, is_mt_1, is_mt_3, request_time, 1)
    return hls_app_location_pair(data)
    # return data


def hls_app_location_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'], data['app'], data['location'],data['server_addr']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


def hls_s_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'], data['server_addr']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


def reduce_function(x, y):
    pair = []
    for xx, yy in zip(x, y):
        pair.append(xx + yy)
    return tuple(pair)


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
        if flag == 'hls_result':
            svr_type, hls_type, timestamp, app, location, server_addr = record[0]
            data = {'timestamp': timestamp, 'hls_type': hls_type}
        else:
            svr_type, hls_type, timestamp, app, location,server_addr = record[0]
            data = {'timestamp': timestamp, 'hls_type': hls_type}

        sum_fluency, sum_nlt_400, sum_mt_1, sum_mt_3, sum_request_time, sum_counts = record[1]

        update_dit = {
            '$inc': dict(sum_fluency=sum_fluency, sum_nlt_400=sum_nlt_400, sum_mt_1=sum_mt_1, sum_mt_3=sum_mt_3,
                         sum_request_time=sum_request_time, sum_counts=sum_counts)
        }
        # =1说明是上行数据，否则是下行数据
        if svr_type == 1:
            tables_list = col_names['up']
        else:
            tables_list = col_names['down']

        for index, col_name in enumerate(tables_list):
            col = db.get_collection(col_name)
            if index == 1:
                data['app'] = app
            if index == 2:
                data['location'] = location
            if index == 3:
                del data['app'], data['location']
                data['server_addr'] = server_addr
            col.update(data, update_dit, True)


if __name__ == '__main__':
    sc = SparkContext(appName=app_name)

    sc.addPyFile('./config/conf.py')
    sc.addPyFile('./utils/tools.py')

    ssc = StreamingContext(sc, 15)

    fromOffsets = get_last_offsets(zk_servers, "spark-group", kafka_topics)

    kafkaParams = {"metadata.broker.list": kafka_brokers}

    # streams = KafkaUtils.createDirectStream(ssc, ['nodeHlsTest'], kafkaParams)
    streams = KafkaUtils.createDirectStream(ssc, kafka_topics, kafkaParams, fromOffsets=fromOffsets)
    # 将每个 partition 的 offset记录更新到zookeeper
    streams.transform(store_offset_ranges).foreachRDD(set_zk_offset)
    # ################### nodeHls数据处理 #############################
    nodeHls_body_dict = streams.filter(nodeHls_filter).map(json2dict).filter(nodeHls_valid_filter)
    # timestamp, app, hls_type, is_fluency, is_nlt_400, is_mt_1, is_mt_3, request_time
    # hls_app_location_pair
    hls_result = nodeHls_body_dict.map(hlsParser).reduceByKey(reduce_function)
    # 按照key值的不同，分成8张表
    # hls_alp_result = hls_alp.map(lambda x: x[0]).reduceByKey(reduce_function)
    # hls_sp_result = hls_alp.map(lambda x: x[1]).reduceByKey(reduce_function)

    hls_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'hls_result', col_names={'down': database_cols_down, 'up': database_cols_up})))

    # hls_alp_result.foreachRDD(
    #     lambda rdd: rdd.foreachPartition(
    #         lambda x: store_data(x, 'hls_alp', col_names={'down': database_cols_down, 'up': database_cols_up})))
    #
    # hls_sp_result.foreachRDD(
    #     lambda rdd: rdd.foreachPartition(
    #         lambda x: store_data(x, 'hls_sp', col_names={'down': database_cols_down, 'up': database_cols_up})))

    ssc.start()
    ssc.awaitTermination()
