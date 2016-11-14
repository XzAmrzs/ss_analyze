# coding=utf-8
from signal import signal, SIGINT

import redis
import json

from pymongo import MongoClient
from config import conf

database_name_hls = conf.DATABASE_NAME_HLS
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

redisIp = conf.REDIS_HOST
redisPort = conf.REDIS_PORT
redisDB = conf.REDIS_DB
redisPwd = conf.REDIS_PWD


def run_proc():
    r = redis.Redis(redisIp, redisPort, redisDB, redisPwd)
    client = MongoClient(database_driver_host)
    db_hls = client.get_database(database_name_hls)
    while True:
        a = r.lpop('hls')
        if a:
            run_hls(a, db_hls, client)


def run_hls(a, db, client):
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

    def signal_action(sig, stack_frame):
        client.close()
        exit(1)

    signal(SIGINT, signal_action)


if __name__ == '__main__':
    from multiprocessing import Process

    threads = {
        Process(target=run_proc),
        Process(target=run_proc),
    }

    for t in threads:
        t.start()

    for t in threads:
        t.join()
