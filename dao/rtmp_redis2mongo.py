# coding=utf-8
from signal import SIGINT
from signal import signal

import redis
import json

import time
from pymongo import MongoClient
from config import conf

database_name_rtmp = conf.DATABASE_NAME_RTMP
database_driver_host = conf.DATABASE_DRIVER_HOST
database_driver_port = conf.DATABASE_DRIVER_PORT

redisIp = conf.REDIS_HOST
redisPort = conf.REDIS_PORT
redisDB = conf.REDIS_DB
redisPwd = conf.REDIS_PWD


def run_rtmp(a, db, client):
    time.sleep(0.01)
    data_dict = json.loads(a)

    cmd, timestamp_hour, app, stream, user, svr_ip = \
        data_dict['cmd'], data_dict['timestamp_hour'], data_dict['app'], data_dict['stream'], \
        data_dict.get('user', -1), data_dict['svr_ip']

    valid_counts, total_counts, play_fluency, play_fluency_in60s, publish_time, play_fluency_time, empty_numbers, \
    empty_time, max_empty_time, first_buffer_time, empty_numbers_in60s, empty_time_in60s, play_fluency_zero_counts, \
    max60s_fluency, min60s_fluency, flux = \
        data_dict['valid_counts'], data_dict["total_counts"], data_dict.get('play_fluency'), data_dict[
            'play_fluency_in60s'], \
        data_dict['publish_time'], data_dict.get('play_fluency_time', 0), data_dict['empty_numbers'], data_dict[
            'empty_time'], \
        data_dict['max_empty_time'], data_dict['first_buffer_time'], data_dict['empty_numbers_in60s'], \
        data_dict['empty_time_in60s'], data_dict['play_fluency_zero_counts'], data_dict['max60s_fluency'], \
        data_dict['min60s_fluency'], data_dict['flux']

    # easy to init a dict!!!
    update_dit = {
        '$inc': dict(valid_counts=valid_counts, total_counts=total_counts, play_fluency=play_fluency,
                     play_fluency_in60s=play_fluency_in60s, publish_time=publish_time,
                     play_fluency_time=play_fluency_time,
                     empty_numbers=empty_numbers, empty_time=empty_time, max_empty_time=max_empty_time,
                     first_buffer_time=first_buffer_time, empty_numbers_in60s=empty_numbers_in60s,
                     empty_time_in60s=empty_time_in60s, play_fluency_zero_counts=play_fluency_zero_counts,
                     max60s_fluency=max60s_fluency, min60s_fluency=min60s_fluency, flux=flux)
    }

    data = {'timestamp': int(timestamp_hour[:8])}

    if cmd == 'play':
        tables_list = ('rtmp_down', 'rtmp_down_app', 'rtmp_down_server')
    elif cmd == 'forward':
        tables_list = ('rtmp_forward', 'rtmp_forward_app', 'rtmp_forward_server')
    else:
        tables_list = ('rtmp_up', 'rtmp_up_app', 'rtmp_up_server')

    for index, col_name in enumerate(tables_list):
        col = db.get_collection(col_name)
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

        data = {'timestamp': int(timestamp_hour)}
        tables_list = ('rtmp_down_user_hour', 'rtmp_down_app_stream_hour')
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

    def signal_action(sig, stack_frame):
        client.close()
        exit(1)

    signal(SIGINT, signal_action)


def run_proc():
    r = redis.Redis(redisIp, redisPort, redisDB, redisPwd)
    client = MongoClient(database_driver_host)
    db_rtmp = client.get_database(database_name_rtmp)
    while True:
        a = r.lpop('rtmp')
        if a:
            run_rtmp(a, db_rtmp, client)


if __name__ == '__main__':
    run_proc()
