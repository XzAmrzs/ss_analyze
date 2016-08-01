# coding=utf-8

"""
 user flux in UTF8 encoded, '\n' delimited text received from the
 network every second.

 Usage: flux_analyze.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming
    would connect to receive data.
"""
from __future__ import print_function

import sys
import json
from pymongo import MongoClient

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# updata flux
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def sendPartition(iter):
    client = MongoClient('localhost', 27017)
    db = client.hls
    posts = db.col
    for record in iter:
        posts.update({"user": record[0]}, {"$set": {"flux": record[1]}}, True)


def raw_print(s):
    # print(s)
    # print('*'*2)
    if s[0] == '{' and s[-1] == '}':
        return json.loads(s)
    else:
        return {}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: flux_analyze.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # sc = SparkContext('local[2]',appName="HLS_flux_Analyze")
    sc = SparkContext(appName="HLS_Flux_Analyze")
    ssc = StreamingContext(sc, 30)
    ssc.checkpoint("checkpoint")

    # Create a DStream that will connect to hostname:port
    host, port = sys.argv[1:]
    lines = ssc.socketTextStream(host, int(port))
    try:
        body_dict = lines.map(raw_print)
    except Exception as e:
        print("***Parse Error:" + str(e) + "***")
    user_flux = body_dict.map(
        lambda body_dict: (body_dict.get('user', "no user keyword"), body_dict.get('body_bytes_sent', 0)))

    running_counts = user_flux.updateStateByKey(updateFunc)
    running_counts.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

    ssc.start()
    ssc.awaitTermination()
    ssc.checkpoint("checkpoint")
