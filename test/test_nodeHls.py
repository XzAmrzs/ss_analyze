#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 下午 4:46

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
    location = body_dict.get('location', 'error_remote_addr')
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
    #
    data = {'svr_type': svr_type, 'hls_type': hls_type, 'timestamp': timestamp, 'app': app, 'location': location,
            'server_addr': server_addr,
            'is_fluency': is_fluency, 'is_nlt_400': is_nlt_400, 'is_mt_1': is_mt_1, 'is_mt_3': is_mt_3,
            'request_time': request_time, 'count': 1}
    return data
    # return data


def hls_app_location_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'], data['app'], data['location']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


def hls_s_pair(data):
    return (data['svr_type'], data['hls_type'], data['timestamp'],data['server_addr']), \
           (data["is_fluency"], data['is_nlt_400'], data['is_mt_1'], data['is_mt_3'], data['request_time'],
            data['count'])


if __name__ == '__main__':
    body_dict = {"body_bytes_sent": 250, "http_user_agent": "AppleCoreMedia/1.0.0.13E238 (iPhone; U; CPU OS 9_3_1 like Mac OS X; zh_cn)", "request_length": 301, "request_time": "0.001", "http_referer": "-", "upstream_addr": "127.0.0.1:8080", "unix_time": "1475738206", "http_stat": 200, "host": "8887.hlsplay.aodianyun.com", "user": "8887", "upstream_response_time": "0.001", "time_local": "[06/Oct/2016:15:16:46 +0800]", "clientip": "-", "request_url": "GET /cjwtv/cjwtv.m3u8 HTTP/1.1", "server_addr": "218.92.216.81", "location": "\u91cd\u5e86"}

    print(unicode(hlsParser(body_dict)[0][4]))

    # IPX.load(os.path.abspath("mydata4vipday2.datx"))
    # print IPX.find("112.43.231.30")