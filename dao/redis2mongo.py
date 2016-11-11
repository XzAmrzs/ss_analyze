# coding=utf-8
from multiprocessing.dummy import Pool as ThreadPool
import redis
import json

import time
from pymongo import MongoClient
from config import conf

database_name_hls = conf.DATABASE_NAME_HLS
database_name_rtmp = conf.DATABASE_NAME_RTMP
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

redisIp = conf.REDIS_HOST
redisPort = conf.REDIS_PORT
redisDB = conf.REDIS_DB
redisPwd = conf.REDIS_PWD

r = redis.Redis(redisIp, redisPort, redisDB, redisPwd)

client = MongoClient(database_driver_host)
db_hls = client.get_database(database_name_hls)
db_rtmp = client.get_database(database_name_rtmp)

def run_rtmp(a):
    time.sleep(0.01)
    data_dict = json.loads(a)

    cmd, timestamp_hour, app, stream, user, svr_ip = \
        data_dict['cmd'], data_dict['timestamp_hour'], data_dict['app'], data_dict['stream'], \
        data_dict.get('user', -1), data_dict['svr_ip']

    valid_counts, total_counts, play_fluency, play_fluency_in60s, publish_time, play_flency_time, empty_numbers, \
    empty_time, max_empty_time, first_buffer_time, empty_numbers_in60s, empty_time_in60s, play_fluency_zero_counts, \
    max60s_fluency, min60s_fluency, flux = \
        data_dict['valid_counts'], data_dict["total_counts"], data_dict['play_fluency'], data_dict[
            'play_fluency_in60s'], \
        data_dict['publish_time'], data_dict['play_flency_time'], data_dict['empty_numbers'], data_dict['empty_time'], \
        data_dict['max_empty_time'], data_dict['first_buffer_time'], data_dict['empty_numbers_in60s'], \
        data_dict['empty_time_in60s'], data_dict['play_fluency_zero_counts'], data_dict['max60s_fluency'], \
        data_dict['min60s_fluency'], data_dict['flux']

    # easy to init a dict!!!
    update_dit = {
        '$inc': dict(valid_counts=valid_counts, total_counts=total_counts, play_fluency=play_fluency,
                     play_fluency_in60s=play_fluency_in60s, publish_time=publish_time,
                     play_flency_time=play_flency_time,
                     empty_numbers=empty_numbers, empty_time=empty_time, max_empty_time=max_empty_time,
                     first_buffer_time=first_buffer_time, empty_numbers_in60s=empty_numbers_in60s,
                     empty_time_in60s=empty_time_in60s, play_fluency_zero_counts=play_fluency_zero_counts,
                     max60s_fluency=max60s_fluency, min60s_fluency=min60s_fluency, flux=flux)
    }

    data = {'timestamp': int(timestamp_hour)}

    if cmd == 'play':
        tables_list = ('rtmp_down', 'rtmp_down_app', 'rtmp_down_server')
    elif cmd == 'forward':
        tables_list = ('rtmp_forward', 'rtmp_forward_app', 'rtmp_forward_server')
    else:
        tables_list = ('rtmp_up', 'rtmp_up_app', 'rtmp_up_server')

    for index, col_name in enumerate(tables_list):
        col = db_rtmp.get_collection(col_name)
        if index == 1:
            data['app'] = app
        if index == 2:
            del data['app']
            data['svr_ip'] = svr_ip

        try:
            col.update_one(data, update_dit, True)
        except Exception as e:
            col.update_one(data, update_dit)

    if cmd == 'play':
        update_dit = {
            '$inc': dict(flux=flux, valid_counts=valid_counts, total_counts=total_counts)
        }

        data = {'timestamp': int(timestamp_hour[:8])}
        tables_list = ('rtmp_down_user', 'rtmp_down_app_stream')
        for index, col_name in enumerate(tables_list):
            col = db_rtmp.get_collection(col_name)
            if index == 0:
                data['user'] = user
            if index == 1:
                data['app'] = app
                data['stream'] = stream
            try:
                col.update_one(data, update_dit, True)
            except Exception as e:
                col.update_one(data, update_dit)


        data = {'timestamp': int(timestamp_hour)}
        tables_list = ('rtmp_down_user_hour', 'rtmp_down_app_stream_hour')
        for index, col_name in enumerate(tables_list):
            col = db_rtmp.get_collection(col_name)
            if index == 0:
                data['user'] = user
            if index == 1:
                data['app'] = app
                data['stream'] = stream
            try:
                col.update_one(data, update_dit, True)
            except Exception as e:
                col.update_one(data, update_dit)



def run_hls(a):
    time.sleep(0.01)
    data_dict = json.loads(a)

    svr_type, hls_type, timestamp_hour, app, stream, user, server_addr, http_stat = \
        data_dict['svr_type'], data_dict['hls_type'], data_dict['timestamp_hour'], data_dict['app'], \
        data_dict['stream'], data_dict['user'], data_dict['server_addr'], data_dict['http_stat'],

    nlt_400_counts, mt_1_counts, mt_3_counts, flux, request_time, fluency_counts, valid_counts, counts = \
        data_dict['nlt_400_counts'], data_dict['mt_1_counts'], data_dict['mt_3_counts'], data_dict['flux'], \
        data_dict['request_time'], data_dict['fluency_counts'], data_dict['valid_counts'], data_dict['counts']

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
        col = db_hls.get_collection(col_name)
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

            col = db_hls.get_collection(col_name)
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
            col = db_hls.get_collection(col_name)
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

            col = db_hls.get_collection(col_name)
            if index == 1:
                data['user'] = user
            if index == 2:
                data['app'] = app
                data['stream'] = stream
            try:
                col.update_one(data, update_dit, True)
            except Exception as e:
                col.update_one(data, update_dit)


def run():
    pool = ThreadPool(4)
    while True:
        counts_rtmp = r.llen('rtmp')
        if counts_rtmp:
            task_list_rtmp = [r.lpop('rtmp') for x in xrange(counts_rtmp)]
            pool.map(run_rtmp, task_list_rtmp)

        counts_hls = r.llen('hls')
        if counts_hls:
            task_list_hls = [r.lpop('hls') for x in xrange(counts_hls)]
            pool.map(run_hls, task_list_hls)


if __name__ == '__main__':
    run()
