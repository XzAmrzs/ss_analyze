#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/31 0031 上午 9:43
from utils import tools


def rtmp_filter(s):
    return s[0] == rtmp_key


def rtmpParser(body_dict):
    """
    :param body_dict: dict
    :return: type:((user,timestamp.app,stream), flux) -> (('1234',"1470898486","1529","stream"), 448))
    """
    body_dict['timestamp'] = tools.timeFormat('%Y%m%d', float(body_dict.get('EndTime', 0)))
    body_dict["totalCounts"] = 1
    body_dict["validCounts"] = 1 if body_dict.get("PublishTime") >= 30 * 30000 else 0
    body_dict["playFlencyTime"] = body_dict['PlayTime'] * body_dict['PlayFluency']
    body_dict["PlayFluencyZeroCounts"] = 1 if body_dict.get("PlayFluency") == 0 else 0

    return rtmp_app_pair(body_dict), rtmp_user_pair(body_dict), rtmp_s_pair(body_dict)


def rtmp_app_pair(data):
    return (data['Cmd'], data['timestamp'], data['App']), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


def rtmp_user_pair(data):
    return (data['Cmd'], data['timestamp'], data.get('User', '-1')), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


def rtmp_s_pair(data):
    return (data['Cmd'], data['timestamp'], data['SvrIp']), \
           (
               data["totalCounts"], data['validCounts'], data['PlayFluency'], data['PlayFluencyIn60s'],
               data['PublishTime'],
               data['playFlencyTime'], data['EmptyNumbers'], data['EmptyTime'], data['MaxEmptyTime'],
               data['FirstBufferTime'], data['EmptyNumbersIn60s'], data['EmptyTimeIn60s'],
               data['PlayFluencyZeroCounts'], data['Max60sFluency'], data['Min60sFluency'], data['RecvByteSum'],
               data['SendByteSum']
           )


def dao_store_and_update(db, flag, col_names, iter):
    for record in iter:
        if flag == 'rtmp_a':
            cmd, timestamp, app = record[0]

            data = {'timestamp': timestamp}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                if index == 1:
                    data['app'] = app

                col.update(data, update_dit, True)

        elif flag == 'rtmp_u':
            cmd, timestamp, User = record[0]

            data = {'timestamp': timestamp, 'User': User}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                col.update(data, update_dit, True)
        elif flag == 'rtmp_s':
            cmd, timestamp, server = record[0]
            data = {'timestamp': timestamp, 'SvrIp': server}

            totalCounts, validCounts, PlayFluency, PlayFluencyIn60s, PublishTime, playFlencyTime, EmptyNumbers, \
            EmptyTime, MaxEmptyTime, FirstBufferTime, EmptyNumbersIn60s, EmptyTimeIn60s, PlayFluencyZeroCounts, \
            Max60sFluency, Min60sFluency, RecvByteSum, SendByteSum = record[1]

            update_dit = {
                '$inc': dict(totalCounts=totalCounts, validCounts=validCounts, PlayFluency=PlayFluency,
                             PlayFluencyIn60s=PlayFluencyIn60s, PublishTime=PublishTime, playFlencyTime=playFlencyTime,
                             EmptyNumbers=EmptyNumbers, EmptyTime=EmptyTime, MaxEmptyTime=MaxEmptyTime,
                             FirstBufferTime=FirstBufferTime, EmptyNumbersIn60s=EmptyNumbersIn60s,
                             EmptyTimeIn60s=EmptyTimeIn60s, PlayFluencyZeroCounts=PlayFluencyZeroCounts,
                             Max60sFluency=Max60sFluency, Min60sFluency=Min60sFluency, RecvByteSum=RecvByteSum,
                             SendByteSum=SendByteSum)
            }

            if cmd == 'publish':
                tables_list = col_names['up']
            elif cmd == 'play':
                tables_list = col_names['down']
            else:
                tables_list = col_names['forward']

            for index, col_name in enumerate(tables_list):
                col = db.get_collection(col_name)
                col.update(data, update_dit, True)