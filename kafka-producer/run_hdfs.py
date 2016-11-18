#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/31 0031 下午 2:18

# coding=utf-8

from servers.sender.nodeHls_hdfs_sender import NodeHlsProducer
from servers.sender.rtmp_hdfs_sender import RtmpProducer

def main():
    threads = {
        NodeHlsProducer(0, 1),
        NodeHlsProducer(2, 3),
        NodeHlsProducer(4, 5),
        NodeHlsProducer(6, 7),
        NodeHlsProducer(8, 9),
        RtmpProducer(0, 2)
    }

    for t in threads:
        t.start()

    for t in threads:
        t.join()

if __name__ == '__main__':
    # logging.basicConfig(
    #     format='%(asctime)s.%(msecs)s:%(thread)d:%(levelname)s:%(process)d:%(messages)s',
    #     level=logging.INFO
    # )
    main()