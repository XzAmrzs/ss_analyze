#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/25 0025 下午 4:46
# import time
# import re
# starttime = time.time()
# request_url='GET http://4285.lssplay.aodianyun.com/hqcaijing/hqcaijing-stream-1471616132-2138927400-3000.ts HTTP/1.1'
# request_url_list = request_url.replace('/', ' ')
# app_stream = re.split(' *', request_url_list)
# app = app_stream[1]
# if '.m3u8' in request_url:
#     # 'GET /szqhqh/stream.m3u8 HTTP/1.1'
#     stream = app_stream[2].split('.')[0]
#     print(app, stream)
# else:
#     if 'http' in request_url:
#         app = app_stream[-4]
#         stream = app_stream[-3].split('-')[1]
#         print(app, stream)
#     else:
#     # 'GET /1529/1529-stream-1459483582-1459486842823-3041.ts HTTP/1.1'
#     # ['GET', 'http:', '4285.lssplay.aodianyun.com', 'hqcaijing', 'hqcaijing-stream-1471616132-2138927400-3000.ts', 'HTTP', '1.1']
#         stream = app_stream[2].split('-')[1]
#         print(app, stream)
# stoptime = time.time()

