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
database_name_rtmp = conf.DATABASE_NAME_RTMP
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

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

def rtmp_filter(s):
    return s[0] == rtmp_key


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
        if flag.startswith('rtmp'):
            db = client.get_database(database_name_rtmp)
            dao_store_and_update(db, flag, col_names, iter)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, flag, col_names, iter):
    for record in iter:
        if flag == 'rtmp_a':
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

    kafkaParams = {"metadata.broker.list": kafka_brokers}

    streams = KafkaUtils.createDirectStream(ssc, kafka_topics, kafkaParams)

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
