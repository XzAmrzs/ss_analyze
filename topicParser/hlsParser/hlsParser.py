#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/31 0031 上午 9:36

# 筛选出nodehls主题的message
import re

from utils import tools


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