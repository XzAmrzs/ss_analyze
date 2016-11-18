#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/13 0013 上午 9:21
# import time,datetime
from servers.mq.NodeHlsAPI import mqAPI
from servers.mq.RtmpFluenceAPI import mqAPI as rtmpAPI
from servers.utils.tools import timeFormat
import json


for i in range(0, 10):
    data = mqAPI.getOffset(i)
    # print(data)
    print(data['lastOffset']-data['offset'])
    # print (mqAPI.setOffset(i, data['lastOffset']))
    # print('nodeHls: 分区'+str(i)+'当前时间 '+timeFormat('%Y%m%d', int(json.loads(mqAPI.pullData(i, data['offset'])['list'][0]['body'])['unix_time'])))
    # print(mqAPI.pullData(i, data['offset']))


for i in range(0, 3):
    # print(rtmpAPI.setOffset(i, 69770000))
    data = rtmpAPI.getOffset(i)
    print(data['lastOffset'] - data['offset'])
    # print (rtmpAPI.setOffset(i, data['lastOffset']))

    # print('rtmp: 分区'+str(i)+'当前时间 '+timeFormat('%Y%m%d', int(json.loads(rtmpAPI.pullData(i, data['offset'])['list'][0]['body'])['EndTime'])))
    # print(data['lastOffset'] - data['offset'])
