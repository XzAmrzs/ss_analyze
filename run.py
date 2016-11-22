#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 上午 9:08

import json
import time
import re

import redis

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
redisIp = conf.REDIS_HOST
redisPort = conf.REDIS_PORT
redisDB = conf.REDIS_DB
redisPwd = conf.REDIS_PWD


logPath = conf.LOG_PATH
timeYmd = tools.timeFormat('%Y%m%d', int(time.time()))

kafka_brokers = conf.KAFKA_BROKERS
kafka_topics = conf.KAFKA_TOPICS
node_key = conf.NODE_FILTER
rtmp_key = conf.RTMP_FILTER
zk_servers = conf.ZK_SERVERS

app_name = conf.APP_NAME

####################################################


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
            fluency_counts = 1 if request_time < 1000 and valid_http_stat else 0
        else:
            hls_type = False
            app, stream, slice_time = get_app_stream(app_stream_list, hls_type)
            # ts判断是否流畅 切片时间大于3倍的请求时间说明是流畅的
            fluency_counts = 1 if slice_time > 3 * request_time and valid_http_stat else 0
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
    is_mt_1 = 1 if request_time > 1000 else 0
    is_mt_3 = 1 if request_time > 3000 else 0

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


# ##############################################Rtmp业务分析###################################################

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
    body_dict["ValidCounts"] = 1 if body_dict.get("PublishTime") >= 30000 else 0
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


def store_data(flag, iter):
    """
    :param iter: abc
    :param col_names: list
    :return: None

    官方文档提示采用lazy模式的连接池来进行数据的入库操作 参考 : http://shiyanjun.cn/archives/1097.html
    关键点: 实现数据库的(序列化)和(反序列化)接口，或者也可以避免,理想效果是只传入sql语句即可??
    """

    try:
        pool = redis.ConnectionPool(host=redisIp, port=redisPort, db=redisDB,password=redisPwd)
        r_db = redis.StrictRedis(connection_pool=pool)
        dao_store_and_update(r_db, flag, iter)
    except Exception as e:
        tools.logout(logPath, app_name, timeYmd, 'Error: ' + str(e), 1)


def dao_store_and_update(db, flag, iter):
    for record in iter:
        if flag == 'hls':
            (svr_type, hls_type, timestamp_hour, app, stream, user, server_addr, http_stat), \
            (
            nlt_400_counts, mt_1_counts, mt_3_counts, flux, request_time, fluency_counts, valid_counts, counts) = record

            data_dict = dict(svr_type=svr_type, hls_type=hls_type, timestamp_hour=timestamp_hour, app=app,
                             stream=stream,
                             user=user, server_addr=server_addr, http_stat=http_stat, nlt_400_counts=nlt_400_counts,
                             mt_1_counts=mt_1_counts, mt_3_counts=mt_3_counts, flux=flux, request_time=request_time,
                             fluency_counts=fluency_counts, valid_counts=valid_counts, counts=counts)

            db.lpush("hls", json.dumps(data_dict))

        if flag == 'rtmp':
            cmd, timestamp_hour, app, stream, user, svr_ip = record[0]

            valid_counts, total_counts, play_fluency, play_fluency_in60s, publish_time, play_fluency_time, \
            empty_numbers, empty_time, max_empty_time, first_buffer_time, empty_numbers_in60s, empty_time_in60s, \
            play_fluency_zero_counts, max60s_fluency, min60s_fluency, flux = record[1]

            data_dict = dict(cmd=cmd, timestamp_hour=timestamp_hour, app=app, stream=stream, user=user, svr_ip=svr_ip,
                             valid_counts=valid_counts, total_counts=total_counts, play_fluency=play_fluency,
                             play_fluency_in60s=play_fluency_in60s, publish_time=publish_time,
                             play_fluency_time=play_fluency_time, empty_numbers=empty_numbers, empty_time=empty_time,
                             max_empty_time=max_empty_time, first_buffer_time=first_buffer_time,
                             empty_numbers_in60s=empty_numbers_in60s, empty_time_in60s=empty_time_in60s,
                             play_fluency_zero_counts=play_fluency_zero_counts, max60s_fluency=max60s_fluency,
                             min60s_fluency=min60s_fluency, flux=flux)

            db.lpush("rtmp", json.dumps(data_dict))

if __name__ == '__main__':
    sc = SparkContext(appName=app_name)
    # 添加依赖文件
    sc.addPyFile('./config/conf.py')
    sc.addPyFile('./utils/tools.py')
    # 设置程序处理间隔
    ssc = StreamingContext(sc, 300)

    # 启动配置
    kafkaParams = {"metadata.broker.list": kafka_brokers}
    streams = KafkaUtils.createDirectStream(ssc, kafka_topics, kafkaParams)

    # ################### nodeHls数据处理 ####################################################
    nodeHls_body_dict = streams.filter(nodeHls_filter).map(json2dict).filter(nodeHls_valid_filter)
    hls_result = nodeHls_body_dict.map(hlsParser)

    # hls_app_stream_user_httpcode
    hls_asuh = hls_result.reduceByKey(reduce_function)

    hls_asuh.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_data('hls', x)))
    # hls_asuh.foreachRDD(lambda x: store_data('hls', x))
    # ############################# RTMP数据处理 ##############################################
    rtmp_body_dict = streams.filter(rtmp_filter).map(json2dict)
    rtmp_result = rtmp_body_dict.map(rtmpParser)

    # rtmp_app_stream_user_server
    rtmp_asus = rtmp_result.reduceByKey(reduce_function)
    rtmp_asus.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: store_data('rtmp', x)))
    # rtmp_asus.foreachRDD(lambda x: r.insertRedis('rtmp', x))
    # ############################## 结束 #####################################################

    ssc.start()
    ssc.awaitTermination()
