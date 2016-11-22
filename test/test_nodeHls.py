#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 下午 4:46

from __future__ import print_function
import re
from utils import tools


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
            app, stream, slice_time = get_app_stream(app_stream_list, hls_type)
            print(slice_time)
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

if __name__ == '__main__':
    # body_dict = {"time_local": "[31/Oct/2016:15:12:23 +0800]", "unix_time": "1477897943", "remote_addr": "223.104.4.59",
    #              "clientip": "-", "host": "120.210.195.68", "request_url": "GET /cjwtv/cjwtv__redirect_.m3u8 HTTP/1.1",
    #              "http_stat": 200, "request_length": 438, "body_bytes_sent": 250,
    #              "http_referer": "http://weixin.niuchaa.com/2202",
    #              "http_user_agent": "Mozilla/5.0 (Linux; Android 6.0; HUAWEI NXT-AL10 Build/HUAWEINXT-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile MQQBrowser/6.8 TBS/036869 Safari/537.36 MicroMessenger/6.3.28.900 NetType/cmnet Language/zh_CN",
    #              "server_addr": "120.210.195.68", "upstream_addr": "127.0.0.1:8080", "upstream_response_time": "0.001",
    #              "request_time": "0.001"}

    body_dict = {"time_local": "[31/Oct/2016:15:11:59 +0800]", "unix_time": "1477897919",
                 "remote_addr": "183.240.128.149", "clientip": "-", "host": "183.232.234.8",
                 "request_url": "GET /xsjrw/xsjrw-stream-1477897111-73315890-110-3000.ts HTTP/1.1", "http_stat": 200,
                 "request_length": 480, "body_bytes_sent": 78584, "http_referer": "http://www.51888zb.com/m/",
                 "http_user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69 QQ/6.5.8.437 V1_IPH_SQ_6.5.8_1_APP_A Pixel/1080 Core/UIWebView NetType/WIFI Mem/141",
                 "server_addr": "183.232.234.8", "upstream_addr": "-", "upstream_response_time": "-",
                 "request_time": "0.000"}

    # body_dict ={"user":"0","time_local":"[31/Oct/2016:15:11:48 +0800]", "unix_time":"1477897908", "remote_addr":"139.201.125.76", "svr_type":1, "host":"0.mlsscdnsource.aodianyun.com", "request_url":"GET /gmb/streamhxb__redirect__1102.m3u8 HTTP/1.1", "http_stat":200, "request_length":91, "body_bytes_sent":250, "http_referer":"-", "http_user_agent":"-", "server_addr":"120.76.29.115", "upstream_addr":"127.0.0.1:8080", "upstream_response_time":"0.002", "request_time":"0.002" }

    print(hlsParser(body_dict))


