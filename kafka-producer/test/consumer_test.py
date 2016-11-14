#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/13 0013 上午 9:21
# import time,datetime
from servers.mq.NodeHlsAPI import mqAPI
from servers.mq.RtmpFluenceAPI import mqAPI as rtmpAPI
from servers.utils.tools import timeFormat
import json
# 1385996783
# 1386292157
# 1386155767
# 1385985147
# 1386187766
# 1385949334
# 1385941431
# 1386097872
# 1385902873
# 1385931029


for i in range(0, 10):
    data = mqAPI.getOffset(i)
    print(data['lastOffset']-data['offset'])
    # print (mqAPI.setOffset(i, data['lastOffset']))

    # print('nodeHls: 分区'+str(i)+'当前时间 '+timeFormat('%Y%m%d', int(json.loads(mqAPI.pullData(i, data['offset'])['list'][0]['body'])['unix_time'])))
    # print(mqAPI.pullData(i, data['offset']))


for i in range(0, 3):
    data = rtmpAPI.getOffset(i)
    print(data['lastOffset'] - data['offset'])
    # print (rtmpAPI.setOffset(i, data['lastOffset']))

    # print('rtmp: 分区'+str(i)+'当前时间 '+timeFormat('%Y%m%d', int(json.loads(rtmpAPI.pullData(i, data['offset'])['list'][0]['body'])['EndTime'])))
    # print(data['lastOffset'] - data['offset'])
