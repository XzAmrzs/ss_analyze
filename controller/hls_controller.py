
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
