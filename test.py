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

sys.path.append('/home/xzp/ss_analyze/config/')
sys.path.append('/home/xzp/ss_analyze/utils/')

import conf
import tools

#########################
database_name_hls = conf.DATABASE_NAME_HLS
database_name_rtmp = conf.DATABASE_NAME_RTMP
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

cols_up_hls_a = conf.TABLES_HLS_UP_A
cols_up_hls_s = conf.TABLES_HLS_UP_S
cols_up_hls_asf = conf.TABLES_HLS_UP_ASF
cols_up_hls_ashf = conf.TABLES_HLS_UP_ASHF
cols_up_hls_uf = conf.TABLES_HLS_UP_UF
cols_up_hls_ufh = conf.TABLES_HLS_UP_UFH
cols_up_hls_hc = conf.TABLES_HLS_UP_HC

cols_down_hls_a = conf.TABLES_HLS_DOWN_A
cols_down_hls_s = conf.TABLES_HLS_DOWN_S
cols_down_hls_asf = conf.TABLES_HLS_DOWN_ASF
cols_down_hls_ashf = conf.TABLES_HLS_DOWN_ASHF
cols_down_hls_uf = conf.TABLES_HLS_DOWN_UF
cols_down_hls_ufh = conf.TABLES_HLS_DOWN_UFH
cols_down_hls_hc = conf.TABLES_HLS_DOWN_HC

cols_rtmp_up_a = conf.TABLES_RTMP_UP_A
cols_rtmp_up_s = conf.TABLES_RTMP_UP_S
cols_rtmp_up_asf = conf.TABLES_RTMP_UP_ASF
cols_rtmp_up_ashf = conf.TABLES_RTMP_UP_ASHF
cols_rtmp_up_uf = conf.TABLES_RTMP_UP_UF
cols_rtmp_up_ufh = conf.TABLES_RTMP_UP_UFH

cols_rtmp_down_a = conf.TABLES_RTMP_DOWN_A
cols_rtmp_down_s = conf.TABLES_RTMP_DOWN_S
cols_rtmp_down_asf = conf.TABLES_RTMP_DOWN_ASF
cols_rtmp_down_ashf = conf.TABLES_RTMP_DOWN_ASHF
cols_rtmp_down_uf = conf.TABLES_RTMP_DOWN_UF
cols_rtmp_down_ufh = conf.TABLES_RTMP_DOWN_UFH

cols_rtmp_forward_a = conf.TABLES_RTMP_FORWARD_A
cols_rtmp_forward_s = conf.TABLES_RTMP_FORWARD_S
cols_rtmp_forward_asf = conf.TABLES_RTMP_FORWARD_ASF
cols_rtmp_forward_ashf = conf.TABLES_RTMP_FORWARD_ASHF
cols_rtmp_forward_uf = conf.TABLES_RTMP_FORWARD_UF
cols_rtmp_forward_ufh = conf.TABLES_RTMP_FORWARD_UFH

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


# ###############################################nodeHls业务分析########################################################

def nodeHls_filter(s):
    return s[0] == node_key


# 筛选出nodehlshls主题中的有效数据
def nodeHls_valid_filter(s):
    a = ('.m3u8' in s.get('request_url', 'error_request_url')) or ('.ts' in s.get('request_url', 'error_request_url'))
    return a


def hlsParser(body_dict):
    """
    获取hls用户数据的函数
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    # 获取APP和stream
    request_url = body_dict.get('request_url', 'error_request_url')
    server_addr = body_dict.get('server_addr', 'error_server_addr')
    remote_addr = body_dict.get('remote_addr', 'error_remote_addr')
    # =1说明是上行数据，否则是下行数据,统一为0
    svr_type = body_dict.get('svr_type', 0)
    # 如果http_stat取不到就取0
    http_stat = body_dict.get('http_stat', 0)
    valid_http_stat = http_stat < 400 and http_stat != 202
    request_time = int(float(body_dict.get('request_time', 0)) * 1000)

    try:
        # 这里不能用简单的'/'来分割,会碰到请求的url有多个'/'的情况
        # 所以解析的时候直接全转成空格，然后按照任意长度的空格来分割
        app_stream_list = re.split(' *', request_url.replace('/', ' '))

        if '.m3u8' in request_url:
            # hls类型，True 表示为m3u8,False表示为ts
            hls_type = True
            app, stream = get_app_stream(app_stream_list, hls_type)
            # m3u8判断是否流畅
            fluency_counts = 1 if request_time < 1 and valid_http_stat else 0
        else:
            hls_type = False
            app, stream, time_length = get_app_stream(app_stream_list, hls_type)
            # ts判断是否流畅 切片时间大于3倍的请求时间说明是流畅的
            fluency_counts = 1 if time_length > 3 * request_time and valid_http_stat else 0
    except Exception as e:
        print(e)
        app = 'error'
        stream = 'error'
        hls_type = 'error'
        fluency_counts = 0

    # 因为有的是上行数据，没有user这个字段，所以统一归为'no user keyword'字段
    try:
        user = int(body_dict.get('user', "no_user_keyword"))
    except Exception as e:
        # print(e)
        user = body_dict.get('user', "no_user_keyword")

    flux = body_dict.get('body_bytes_sent', 0) if valid_http_stat else 0
    timestamp = tools.timeFormat('%Y%m%d', float(body_dict.get('unix_time', 0)))
    timestamp_hour = tools.timeFormat('%Y%m%d%H', float(body_dict.get('unix_time', 0)))
    valid_counts = 1 if valid_http_stat else 0

    # 如果状态位置取不出来，说明该数据无效，仍是失败数据,直接去取400
    # is_not_less_than
    is_nlt_400 = 1 if http_stat >= 400 else 0
    # is_more_than
    is_mt_1 = 1 if request_time > 1 else 0
    is_mt_3 = 1 if request_time > 3 else 0

    data = {'svr_type': svr_type, 'hls_type': hls_type, 'timestamp': timestamp, 'timestamp_hour': timestamp_hour,
            'app': app, 'stream': stream, 'user': user, 'server_addr': server_addr, 'remote_addr': remote_addr,
            'http_stat': http_stat, 'is_nlt_400': is_nlt_400, 'is_mt_1': is_mt_1,
            'is_mt_3': is_mt_3, 'request_time': request_time, 'count': 1, 'flux': flux, 'valid_counts': valid_counts,
            'fluency_counts': fluency_counts}

    return hls_app_stream_user_server_httpcode(data)


def get_app_stream(app_stream_list, hls_type):
    # GET http://hzsrzj/hzsrzj-stream-1470988831-1470991945170-3042.ts HTTP/1.1
    # 'GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1'

    app = app_stream_list[1]
    if hls_type:
        stream = app_stream_list[2].split('.')[0].split('__')[0]
        return app, stream
    else:
        if 'http:' in app_stream_list:
            app = app_stream_list[2]

        item = app_stream_list[-3].split('-')
        stream = item[1]
        time_length = item[-1].split('.')[0]
        return app, stream, time_length


def hls_app_stream_user_server_httpcode(data):
    return (data['svr_type'], data['hls_type'], data['timestamp_hour'], data['app'], data['stream'], data['user'],
            data['server_addr'], data['http_stat']), \
           (data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['flux'], data['request_time'],
            data['fluency_counts'], data['valid_counts'], data['count'])


# ##############################################结束###################################################

def rtmp_filter(s):
    return s[0] == rtmp_key


def rtmpParser(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """

    body_dict['Timestamp'] = tools.timeFormat('%Y%m%d', float(body_dict.get('EndTime', 0)))
    body_dict['TimestampHour'] = tools.timeFormat('%Y%m%d%H', float(body_dict.get('EndTime', 0)))
    # play是下行
    body_dict['Flux'] = body_dict['SendByteSum'] if body_dict.get('Cmd') == 'play' else body_dict['RecvByteSum']
    body_dict["Count"] = 1
    body_dict["ValidCounts"] = 1 if body_dict.get("PublishTime") >= 30 * 30000 else 0
    body_dict["PlayFlencyTime"] = body_dict['PlayTime'] * body_dict['PlayFluency']
    body_dict["PlayFluencyZeroCounts"] = 1 if body_dict.get("PlayFluency") == 0 else 0

    return rtmp_app_stream_user_server(body_dict)


def rtmp_app_stream_user_server(data):
    return (data['Cmd'], data['TimestampHour'], data['App'], data['Stream'], data.get('User', -1), data['SvrIp']), \
           (data['ValidCounts'], data["Count"], data['PlayFluency'], data['PlayFluencyIn60s'],
            data['PublishTime'], data['PlayFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
            data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'], data['PlayFluencyZeroCounts'],
            data['Max60sFluency'], data['Min60sFluency'], data['Flux']
            )


# ############################################结束#########################################################


def reduce_function(x, y):
    pair = []
    for xx, yy in zip(x, y):
        pair.append(xx + yy)
    return tuple(pair)


def store_data(iter, flag):
    """
    :param iter: abc
    :param col_names: list
    :return: None

    官方文档提示采用lazy模式的连接池来进行数据的入库操作 参考 : http://shiyanjun.cn/archives/1097.html
    关键点: 实现数据库的(序列化)和(反序列化)接口，或者也可以避免,理想效果是只传入sql语句即可??
    """

    try:
        client = MongoClient(database_driver_host, database_driver_port)
        if flag == 'hls':
            db = client.get_database(database_name_hls)
            dao_store_and_update(db, flag, iter)
        elif flag == 'rtmp':
            db = client.get_database(database_name_rtmp)
            dao_store_and_update(db, flag, iter)
        client.close()
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, flag, iter):
    for record in iter:
        if flag == 'hls':
            svr_type, hls_type, timestamp_hour, app, stream, user, server_addr, http_stat = record[0]

            nlt_400_counts, mt_1_counts, mt_3_counts, flux, request_time, fluency_counts, valid_counts, counts = record[
                1]

            if hls_type:
                update_dit = {
                    '$inc': dict(fluency_counts_m3=fluency_counts, nlt_400_counts_m3=nlt_400_counts,
                                 mt_1_counts_m3=mt_1_counts,
                                 mt_3_counts_m3=mt_3_counts, request_time_m3=request_time, counts_m3=counts,
                                 fluency_counts_ts=0, nlt_400_counts_ts=0, mt_1_counts_ts=0,
                                 mt_3_counts_ts=0, request_time_ts=0, counts_ts=0,
                                 flux=flux
                                 )
                }
            else:
                update_dit = {
                    '$inc': dict(fluency_counts_m3=0, nlt_400_counts_m3=0, mt_1_counts_m3=0,
                                 mt_3_counts_m3=0, request_time_m3=0, counts_m3=0,
                                 fluency_counts_ts=fluency_counts, nlt_400_counts_ts=nlt_400_counts,
                                 mt_1_counts_ts=mt_1_counts,
                                 mt_3_counts_ts=mt_3_counts, request_time_ts=request_time, counts_ts=counts,
                                 flux=flux
                                 )
                }

            # 如果是上行数据
            if svr_type == 1:
                tables_list = ('hls_up', 'hls_up_a', 'hls_up_s')
            else:
                tables_list = ('hls_down', 'hls_down_a', 'hls_down_s')

            data = {'timestamp': int(timestamp_hour[:8])}
            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                if index == 1:
                    data['app'] = app
                if index == 2:
                    del data['app']
                    data['server_addr'] = server_addr
                try:
                    col.update_one(data, update_dit, True)
                except Exception as e:
                    col.update_one(data, update_dit)

            # 如果是下行
            if svr_type == 0:
                update_dit = {
                    '$inc': dict(flux=flux, valid_counts=valid_counts, request_time=request_time, counts=counts,
                                 fluency_counts=fluency_counts)}

                data = {'timestamp': int(timestamp_hour[:8])}
                tables_list = ('hls_down_user', 'hls_down_app_stream', 'hls_down_httpcode')
                for index, col_name in enumerate(tables_list):

                    col = db.get_collection(col_name)
                    if index == 0:
                        data['user'] = user
                    if index == 1:
                        data['app'] = app
                        data['stream'] = stream
                    if index == 2:
                        data['httpCode'] = http_stat
                        del data['user']
                        del data['app']
                        del data['stream']
                    try:
                        col.update_one(data, update_dit, True)
                    except Exception as e:
                        col.update_one(data, update_dit)

                data = {'timestamp': int(timestamp_hour)}
                tables_list = ('hls_down_user_hour', 'hls_down_app_stream_hour')
                for index, col_name in enumerate(tables_list):
                    col = db.get_collection(col_name)
                    if index == 0:
                        data['user'] = user
                    if index == 1:
                        data['app'] = app
                        data['stream'] = stream
                    try:
                        col.update_one(data, update_dit, True)
                    except Exception as e:
                        col.update_one(data, update_dit)

                data = {'updateTime': int(timestamp_hour[:8])}
                update_dit = {
                    '$inc': dict(sizeSum=flux, count=valid_counts, reqTime=request_time,
                                 fluency=fluency_counts)}
                tables_list = ('HlsDayData', 'HlsUserData', 'HlsStreamData',)
                for index, col_name in enumerate(tables_list):

                    col = db.get_collection(col_name)
                    if index == 1:
                        data['user'] = user
                    if index == 2:
                        data['app'] = app
                        data['stream'] = stream
                    try:
                        col.update_one(data, update_dit, True)
                    except Exception as e:
                        col.update_one(data, update_dit)

                        # if flag == 'rtmp':
                        #     cmd, timestamp_hour, app, stream, user, svr_ip = record[0]
                        #
                        #     valid_counts, total_counts, play_fluency, play_fluency_in60s, publish_time, play_flency_time, empty_numbers, empty_time, max_empty_time, first_buffer_time, empty_numbers_in60s, empty_time_in60s, play_fluency_zero_counts, max60s_fluency, min60s_fluency, flux = record[1]
                        #
                        #     update_dit = {
                        #         '$inc': dict(valid_counts=valid_counts, total_counts=total_counts, play_fluency=play_fluency,
                        #                      play_fluency_in60s=play_fluency_in60s, publish_time=publish_time,
                        #                      play_flency_time=play_flency_time,
                        #                      empty_numbers=empty_numbers, empty_time=empty_time, max_empty_time=max_empty_time,
                        #                      first_buffer_time=first_buffer_time, empty_numbers_in60s=empty_numbers_in60s,
                        #                      empty_time_in60s=empty_time_in60s, play_fluency_zero_counts=play_fluency_zero_counts,
                        #                      max60s_fluency=max60s_fluency, min60s_fluency=min60s_fluency, flux=flux)
                        #     }
                        #
                        #     data = {'timestamp': int(timestamp_hour)}
                        #
                        #     if cmd == 'play':
                        #         tables_list = ('rtmp_down','rtmp_down_app','rtmp_down_server')
                        #     elif cmd == 'forward':
                        #         tables_list = ('rtmp_forward','rtmp_forward_app','rtmp_forward_server')
                        #     else:
                        #         tables_list = ('rtmp_up','rtmp_up_app','rtmp_up_server')
                        #
                        #     for index, col_name in enumerate(tables_list):
                        #         col = db.get_collection(col_name)
                        #         if index == 1:
                        #             data['app'] = app
                        #         if index == 2:
                        #             del data['app']
                        #             data['svr_ip'] = svr_ip
                        #
                        #         col.update(data, update_dit, True)
                        #
                        #     if cmd == 'play':
                        #         update_dit = {
                        #             '$inc': dict(flux=flux, valid_counts=valid_counts, total_counts=total_counts)
                        #         }
                        #
                        #         data = {'timestamp': int(timestamp_hour[:8])}
                        #         tables_list = ('rtmp_down_user', 'rtmp_down_app_stream')
                        #         for index, col_name in enumerate(tables_list):
                        #             col = db.get_collection(col_name)
                        #             if index == 0:
                        #                 data['user'] = user
                        #             if index == 1:
                        #                 data['app'] = app
                        #                 data['stream'] = stream
                        #             col.update(data, update_dit, True)
                        #
                        #         data = {'timestamp': int(timestamp_hour)}
                        #         tables_list = ('rtmp_down_user_hour', 'rtmp_down_app_stream_hour')
                        #         for index, col_name in enumerate(tables_list):
                        #             col = db.get_collection(col_name)
                        #             if index == 0:
                        #                 data['user'] = user
                        #             if index == 1:
                        #                 data['app'] = app
                        #                 data['stream'] = stream
                        #             col.update(data, update_dit, True)


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
    # ################### nodeHls数据处理 ####################################################
    nodeHls_body_dict = streams.filter(nodeHls_filter).map(json2dict).filter(nodeHls_valid_filter)
    hls_result = nodeHls_body_dict.map(hlsParser)

    # hls_app_stream_user_httpcode
    hls_asuh = hls_result.reduceByKey(reduce_function)
    hls_asuh.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_data(x, 'hls')))
    # ############################# RTMP数据处理 ##############################################
    rtmp_body_dict = streams.filter(rtmp_filter).map(json2dict)
    rtmp_result = rtmp_body_dict.map(rtmpParser)

    # rtmp_app_stream_user_server
    rtmp_asus = rtmp_result.reduceByKey(reduce_function)
    rtmp_asus.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_data(x, 'rtmp')))

    ssc.start()
    ssc.awaitTermination()
