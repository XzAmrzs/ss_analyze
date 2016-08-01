# coding=utf-8
from __future__ import print_function

import json
import socket  # socket模块
import threading
import time

from MQAPI import getOffset, pullData

HOST = '0.0.0.0'
PORT = 9999
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 定义socket类型，网络通信，TCP
s.bind((HOST, PORT))  # 套接字绑定的IP与端口
s.listen(1)  # 开始TCP监听


def response(conn, addr):
    offsets = getOffset('http://api.mq.aodianyun.com/v1', 'nodeHls', 0, 'XZP')
    # offset = offsets.get('offset', 'Error: no startoffset keykowd in this frame')
    offset = offsets.get('startOffset', 'Error: no startoffset keykowd in this frame')
    lastOffset = offsets.get('lastOffset', 'Error: no lastOffset keykowd in this frame')

    # 一直发送数据直到游标为空(BUG:不能使用offset来判断是否为空，空的时候会发生超时错误)
    while offset <= lastOffset:
        try:
            data = pullData('http://api.mq.aodianyun.com/v1', 'nodeHls', 0, offset)

            filter_send_body(data, conn)
            data = json.loads(data)

            offset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
            print(offset)
            # setOffset('http://api.mq.aodianyun.com/v1', 'nodeHls', 0, offset, 'XZP')

            time.sleep(0.1)

        except Exception as e:
            conn.close()
            print(e)
            # break
            print("测试数据发送完毕")


def filter_send_body(data, conn):
    data_dict = json.loads(data, encoding='utf-8')
    pre_datas = data_dict['list']
    for data in pre_datas:
        body = data.get('body', 'Error:no body keyword')
        try:
            conn.send(body.replace('\\', '') + '\n')
        except socket.error as e:
            print("Socket error:" + str(e))
            conn.close()
            break
while 1:
    conn, addr = s.accept()  # 接受TCP连接，并返回新的套接字与IP地址
    print('Connected by', addr)  # 输出客户端的IP地址
    t = threading.Thread(target=response, args=(conn, addr))
    t.start()
