# coding=utf-8
from __future__ import print_function
from MQtest import getOffset, setOffset, pullData
import socket  # socket模块
import threading
import json
import time

HOST = '0.0.0.0'
PORT = 9999
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 定义socket类型，网络通信，TCP
s.bind((HOST, PORT))  # 套接字绑定的IP与端口
s.listen(1)  # 开始TCP监听


def response(conn, addr):
    offsets = getOffset('http://api.mq.aodianyun.com/v1', 'nodeHls_ttt', 0, 'XZP')
    offset = offsets.get('startOffset', 'Error: no startoffset keykowd in this frame')
    # 一直发送数据直到游标为空(BUG:不能使用offset来)
    while offset:
        try:
            data = pullData('http://api.mq.aodianyun.com/v1', 'nodeHls_ttt', 0, offset)
            filter_send_body(data, conn)
            data = json.loads(data)
            offset = data.get("nextOffset", "Error: no nextoffset keykowd in this frame")
            time.sleep(5)
        except Exception as e:
            conn.close()
            print(e)
            break

def filter_send_body(data, conn):
    data_dict = json.loads(data)
    pre_datas = data_dict['list']
    for data in pre_datas:
        body = data.get('body', 'Error:no body keyword')
        # print(body)
        try:
            conn.send(body + '\n')
        except Exception as e:
            conn.close()
            print(e)
            break

while 1:
    conn, addr = s.accept()  # 接受TCP连接，并返回新的套接字与IP地址
    print('Connected by', addr)  # 输出客户端的IP地址
    t = threading.Thread(target=response, args=(conn, addr))
    t.start()

conn.close()  # 关闭连接
