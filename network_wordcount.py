# coding=utf-8

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: flux_analyze.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    #
    sc = SparkContext(appName="HLS_Size_Analyze")

    # 创建 Streaming 的上下文
    ssc = StreamingContext(sc, 1)

    # 建立数据源,这里使用socket连接作为数据源
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
