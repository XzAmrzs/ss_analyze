#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/13 0013 上午 9:21
# import time,datetime
from servers.mq.NodeHlsAPI import mqAPI
from servers.mq.RtmpFluenceAPI import mqAPI as rtmpAPI

for i in range(0, 10):
    data = mqAPI.getOffset(i)
    print(data['lastOffset'] - data['offset'])
    # print (mqAPI.setOffset(i, data['lastOffset']))

for i in range(0, 3):
    data = rtmpAPI.getOffset(i)
    print(data['lastOffset'] - data['offset'])
    # print (rtmpAPI.setOffset(i, data['lastOffset']))
