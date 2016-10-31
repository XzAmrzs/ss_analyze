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

#########################
database_name_hls = conf.DATABASE_NAME_HLS
database_name_rtmp = conf.DATABASE_NAME_RTMP
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

cols_up_hls = conf.TABLES_HLS_R
cols_up_s_hls = conf.TABLES_HLS_R_S
cols_down_hls = conf.TABLES_HLS_S
cols_down_s_hls = conf.TABLES_HLS_S_S

cols_up_rtmp = conf.TABLES_RTMP_R
cols_up_u_rtmp = conf.TABLES_RTMP_R_USER
cols_up_s_rtmp = conf.TABLES_RTMP_R_S
cols_down_rtmp = conf.TABLES_RTMP_S
cols_down_u_rtmp = conf.TABLES_RTMP_S_USER
cols_down_s_rtmp = conf.TABLES_RTMP_S_S
cols_forward_rtmp = conf.TABLES_RTMP_F
cols_forward_u_rtmp = conf.TABLES_RTMP_F_USER
cols_forward_s_rtmp = conf.TABLES_RTMP_F_S

logPath = conf.LOG_PATH
timeYmd = tools.timeFormat('%Y%m%d', int(time.time()))

kafka_brokers = conf.KAFKA_BROKERS
kafka_topics = conf.KAFKA_TOPICS
node_key = conf.NODE_FILTER
rtmp_key = conf.RTMP_FILTER
zk_servers = conf.ZK_SERVERS

app_name = conf.APP_NAME

####################################################


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
    return s[0] == node_key


def rtmp_filter(s):
    return s[0] == rtmp_key


# 筛选出nodehlshls主题中的有效数据
def nodeHls_valid_filter(s):
    a = ('.m3u8' in s.get('request_url', 'error_request_url')) or ('.ts' in s.get('request_url', 'error_request_url'))
    return a


def rtmpParser(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    body_dict['timestamp'] = tools.timeFormat('%Y%m%d', float(body_dict.get('EndTime', 0)))
    body_dict["totalCounts"] = 1
    body_dict["validCounts"] = 1 if body_dict.get("PublishTime") >= 30 * 30000 else 0
    body_dict["playFlencyTime"] = body_dict['PlayTime'] * body_dict['PlayFluency']
    body_dict["PlayFluencyZeroCounts"] = 1 if body_dict.get("PlayFluency") == 0 else 0

    return rtmp_app_pair(body_dict), rtmp_user_pair(body_dict), rtmp_s_pair(body_dict)


def rtmp_app_pair(data):
    return (data['Cmd'], data['timestamp'], data['App']), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


def rtmp_user_pair(data):
    return (data['Cmd'], data['timestamp'], data.get('User', '-1')), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


def rtmp_s_pair(data):
    return (data['Cmd'], data['timestamp'], data['SvrIp']), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


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
    valid_http_stat = body_dict.get('http_stat', 400) < 400 and body_dict.get('http_stat', 202) != 202

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
            is_fluency = 1 if request_time < 1 and valid_http_stat else 0
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
            is_fluency = 1 if time_length > 3 * request_time and valid_http_stat else 0
            hls_type = False
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: hlsParser ' + str(e) + str(body_dict), 1)
        app = 'error'
        # stream = 'error'
        is_fluency = 'error'
        hls_type = 'error'
    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    # user = body_dict.get('user', "no user keyword")
    flux = body_dict.get('body_bytes_sent', 0) if valid_http_stat else 0
    timestamp = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    timestamp_hour = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    # 如果状态位置取不出来，说明该数据无效，仍是失败数据,直接去取400
    # is_not_less_than
    is_nlt_400 = 1 if body_dict.get('http_stat', 400) >= 400 else 0
    # is_more_than
    is_mt_1 = 1 if request_time > 1 else 0
    is_mt_3 = 1 if request_time > 3 else 0

    data = {'svr_type': svr_type, 'hls_type': hls_type, 'timestamp': timestamp, 'app': app, 'location': location,
            'server_addr': server_addr,
            'is_fluency': is_fluency, 'is_nlt_400': is_nlt_400, 'is_mt_1': is_mt_1, 'is_mt_3': is_mt_3,
            'request_time': request_time, 'count': 1, 'flux': flux}
    return hls_app_location_pair(data), hls_s_pair(data)  # , hls_flux_pair(data)
    # return data


def hls_app_location_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'], data['app'], data['location']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


def hls_s_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'], data['server_addr']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


def hls_flux_pair(data):
    return (data['svr_type'], data['timestamp']), data['flux']


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
        if flag.startswith('hls'):
            db = client.get_database(database_name_hls)
            dao_store_and_update(db, flag, col_names, iter)
        if flag.startswith('rtmp'):
            db = client.get_database(database_name_rtmp)
            dao_store_and_update(db, flag, col_names, iter)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, flag, col_names, iter):
    for record in iter:
        if flag == 'hls_alp':
            svr_type, hls_type, timestamp, app, location = record[0]

            data = {'timestamp': timestamp}

            sum_fluency, sum_nlt_400, sum_mt_1, sum_mt_3, sum_request_time, sum_counts = record[1]

            if hls_type:
                update_dit = {
                    '$inc': dict(sum_fluency_m3=sum_fluency, sum_nlt_400_m3=sum_nlt_400, sum_mt_1_m3=sum_mt_1,
                                 sum_mt_3_m3=sum_mt_3,
                                 sum_request_time_m3=sum_request_time, sum_counts_m3=sum_counts,
                                 sum_fluency_ts=0, sum_nlt_400_ts=0, sum_mt_1_ts=0,
                                 sum_mt_3_ts=0,
                                 sum_request_time_ts=0, sum_counts_ts=0
                                 )
                }
            else:
                update_dit = {
                    '$inc': dict(sum_fluency_m3=0, sum_nlt_400_m3=0, sum_mt_1_m3=0,
                                 sum_mt_3_m3=0,
                                 sum_request_time_m3=0, sum_counts_m3=0,
                                 sum_fluency_ts=sum_fluency, sum_nlt_400_ts=sum_nlt_400, sum_mt_1_ts=sum_mt_1,
                                 sum_mt_3_ts=sum_mt_3,
                                 sum_request_time_ts=sum_request_time, sum_counts_ts=sum_counts)
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

                col.update(data, update_dit, True)

        elif flag == 'hls_sp':
            svr_type, hls_type, timestamp, server_addr = record[0]

            data = {'timestamp': timestamp, 'server_addr': server_addr}

            sum_fluency, sum_nlt_400, sum_mt_1, sum_mt_3, sum_request_time, sum_counts = record[1]

            if hls_type:
                update_dit = {
                    '$inc': dict(sum_fluency_m3=sum_fluency, sum_nlt_400_m3=sum_nlt_400, sum_mt_1_m3=sum_mt_1,
                                 sum_mt_3_m3=sum_mt_3,
                                 sum_request_time_m3=sum_request_time, sum_counts_m3=sum_counts,
                                 sum_fluency_ts=0, sum_nlt_400_ts=0, sum_mt_1_ts=0,
                                 sum_mt_3_ts=0,
                                 sum_request_time_ts=0, sum_counts_ts=0
                                 )
                }
            else:
                update_dit = {
                    '$inc': dict(sum_fluency_m3=0, sum_nlt_400_m3=0, sum_mt_1_m3=0,
                                 sum_mt_3_m3=0,
                                 sum_request_time_m3=0, sum_counts_m3=0,
                                 sum_fluency_ts=sum_fluency, sum_nlt_400_ts=sum_nlt_400, sum_mt_1_ts=sum_mt_1,
                                 sum_mt_3_ts=sum_mt_3,
                                 sum_request_time_ts=sum_request_time, sum_counts_ts=sum_counts)
                }

            if svr_type == 1:
                tables_list = col_names['up']
            else:
                tables_list = col_names['down']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                col.update(data, update_dit, True)

        elif flag == 'hls_flux':
            svr_type, timestamp = record[0]
            data = {'timestamp': timestamp}
            if svr_type != 1:
                sum_flux = record[1]
                col = db.get_collection('hls_timestamp_flux')
                update_dit = {
                    '$inc': dict(flux=sum_flux)
                }
                col.update(data, update_dit, True)
        elif flag == 'rtmp_a':
            cmd, timestamp, app = record[0]

            data = {'timestamp': timestamp}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                if index == 1:
                    data['app'] = app

                col.update(data, update_dit, True)

        elif flag == 'rtmp_u':
            cmd, timestamp, User = record[0]

            data = {'timestamp': timestamp, 'User': User}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                col.update(data, update_dit, True)
        elif flag == 'rtmp_s':
            cmd, timestamp, server = record[0]
            data = {'timestamp': timestamp, 'SvrIp': server}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                col.update(data, update_dit, True)


if __name__ == '__main__':
    sc = SparkContext(appName=app_name)

    sc.addPyFile('./config/conf.py')
    sc.addPyFile('./utils/tools.py')

    ssc = StreamingContext(sc, 15)

    fromOffsets = get_last_offsets(zk_servers, "spark-group", kafka_topics)

    kafkaParams = {"metadata.broker.list": kafka_brokers}

    streams = KafkaUtils.createDirectStream(ssc, kafka_topics, kafkaParams)
    # streams = KafkaUtils.createDirectStream(ssc, kafka_topics, kafkaParams, fromOffsets=fromOffsets)

    # 将每个 partition 的 offset记录更新到zookeeper
    streams.transform(store_offset_ranges).foreachRDD(set_zk_offset)
    # ################### nodeHls数据处理 #############################
    nodeHls_body_dict = streams.filter(nodeHls_filter).map(json2dict).filter(nodeHls_valid_filter)
    # timestamp, app, hls_type, is_fluency, is_nlt_400, is_mt_1, is_mt_3, request_time
    hls_result = nodeHls_body_dict.map(hlsParser)

    # hls_app_location_pair
    hls_alp_result = hls_result.map(lambda x: x[0]).reduceByKey(reduce_function)
    hls_alp_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'hls_alp', col_names={'down': cols_down_hls, 'up': cols_up_hls})))

    hls_sp_result = hls_result.map(lambda x: x[1]).reduceByKey(reduce_function)
    hls_sp_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'hls_sp', col_names={'down': cols_down_s_hls, 'up': cols_up_s_hls})))

    # ############################# RTMP数据处理 ##############################################
    rtmp_body_dict = streams.filter(rtmp_filter).map(json2dict)
    rtmp_result = rtmp_body_dict.map(rtmpParser)

    rtmp_a_result = rtmp_result.map(lambda x: x[0]).reduceByKey(reduce_function)
    rtmp_a_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'rtmp_a',
                                 col_names={'down': cols_down_rtmp, 'up': cols_up_rtmp, 'forward': cols_forward_rtmp})))

    rtmp_u_result = rtmp_result.map(lambda x: x[1]).reduceByKey(reduce_function)
    rtmp_u_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'rtmp_u',
                                 col_names={'down': cols_down_u_rtmp, 'up': cols_up_u_rtmp,
                                            'forward': cols_forward_u_rtmp})))

    rtmp_s_result = rtmp_result.map(lambda x: x[2]).reduceByKey(reduce_function)
    rtmp_s_result.foreachRDD(
        lambda rdd: rdd.foreachPartition(
            lambda x: store_data(x, 'rtmp_s',
                                 col_names={'down': cols_down_s_rtmp, 'up': cols_up_s_rtmp,
                                            'forward': cols_forward_s_rtmp})))

    ssc.start()
    ssc.awaitTermination()
