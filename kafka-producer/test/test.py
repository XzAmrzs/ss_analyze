#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/10/13 0013 上午 9:21
# import time,datetime
from servers.mq.NodeHlsAPI import mqAPI
from servers.mq.RtmpFluenceAPI import mqAPI as rtmpAPI
# data = nodeHlsAPI.getOffset(0)
# lastOffset_a = data['lastOffset']
# lastOffset_b = lastOffset_a
# start = time.clock()
# i = 10
# while i:
#
#     while lastOffset_a == lastOffset_b:
#         data = nodeHlsAPI.getOffset(0)
#         lastOffset_b = data['lastOffset']
#         # print(lastOffset_a, lastOffset_b)
#
#     end = time.clock()
#     print(lastOffset_b-lastOffset_a)
#     print("%.03f seconds" % (end - start))
#     lastOffset_a = lastOffset_b
#     start = end
# #     i -= 1
for i in range(0, 10):
    data = mqAPI.getOffset(i)
    print(data['lastOffset']-data['offset'])
    # data2 = mqAPI.pullData(0, data['offset'])
    # print(data2)
    # print (mqAPI.setOffset(i, data['lastOffset']))
#
# for i in range(0, 3):
#     data = rtmpAPI.getOffset(i)
#     print(data['lastOffset']-data['offset'])
    # data2 = mqAPI.pullData(0, data['offset'])
    # print(data2)
    # print (rtmpAPI.setOffset(i, data['lastOffset']))
#

# print(data)
# print(data2)
#
# import os
# import signal
#
#
# # Define signal handler function
# def myHandler(signum, frame):
#     print('I received: ', signum)
#
#
# # register signal.SIGTSTP's handler
# signal.signal(signal., myHandler)
# signal.pause()
# print('End of Signal Demo')
#
#
# import signal
# import time
#
#
# def signal_handler(signum, frame):
#     print('Received signal: ', signum)
#     exit()
#
#
# signal.signal(signal.SIGHUP, signal_handler)  # 1
#
# while True:
#     print('waiting')
#     time.sleep(1)
