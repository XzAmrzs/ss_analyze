#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/30 0030 上午 11:21
from utils import tools

body_dict = {
    'VideoFrameRate': 0,
    'FirstBufferTime': 0,
    'PlayTimeIn60s': 60000,
    'PublishTime': 164089,  # 毫秒
    'EmptyNumbers': 0,
    'RemoteIp': u'222.37.4.15',
    'PlayTime': 164089,   # 毫秒
    'Max60sFluency': 100,
    'EndTime': 1477450951,
    'AudioBitRate': 0,
    'MaxEmptyTime': 0,
    'SendByteSum': 159474944,
    'EmptyTime': 0,
    'EmptyTimeIn60s': 0,
    'AudioCodecId': 0,
    'Min60sFluency': 99.88,
    'RemotePort': 55659,
    'VideoBitRate': 0,
    'SvrIp': u'118.194.59.254',
    'Cmd': u'play',
    'PlayFluencyIn60s': 100,
    'RecvByteSum': 4774,
    'VideoWidth': 0,
    'VideoCodecId': 0,
    'Stream': u'stream',
    'AudioSampleRate': 0,
    'App': u'xcs',
    'PlayFluency': 100,
    'EmptyNumbersIn60s': 0,
    'VideoKeyFrameInterval': 0,
    'StartTime': 1477450787,
    'VideoHeigth': 0
}


def rtmp_app_stream_user_server(data):
    return (data['Cmd'], data['TimestampHour'], data['App'], data['Stream'], data.get('User', -1), data['SvrIp']), \
           (data['ValidCounts'], data["Count"], data['PlayFluency'], data['PlayFluencyIn60s'],
            data['PublishTime'], data['PlayFluencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
            data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'], data['PlayFluencyZeroCounts'],
            data['Max60sFluency'], data['Min60sFluency'], data['Flux']
            )


def rtmpParser(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """

    body_dict['TimestampHour'] = tools.timeFormat('%Y%m%d%H', float(body_dict.get('EndTime', 0)))
    body_dict['Timestamp'] = tools.timeFormat('%Y%m%d', float(body_dict.get('EndTime', 0)))
    # play是下行
    body_dict['Flux'] = body_dict['SendByteSum'] if body_dict.get('Cmd') == 'play' else body_dict['RecvByteSum']
    body_dict["Count"] = 1
    body_dict["ValidCounts"] = 1 if body_dict.get("PublishTime") >= 30000 else 0
    body_dict["PlayFluencyTime"] = body_dict['PlayTime'] * body_dict['PlayFluency']
    body_dict["PlayFluencyZeroCounts"] = 1 if body_dict.get("PlayFluency") == 0 else 0

    return rtmp_app_stream_user_server(body_dict)


print rtmpParser(body_dict)
