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

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# updata flux
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

def data_parse(data):
    data_dict = json.loads(data)
    pre_datas = data_dict['list']
    for data in pre_datas:
        body = data.get('body', 'Error:no body keyword')
        body_dict = json.loads(body)
        user = body_dict.get('user', 'Error:no user keyword')
        flux = body_dict.get('body_bytes_sent', 'Error:no body_bytes_sent keyword')
        yield user, flux


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: flux_analyze.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # sc = SparkContext('local[2]',appName="HLS_flux_Analyze")
    sc = SparkContext(appName="HLS_Flux_Analyze")
    ssc = StreamingContext(sc, 1)
    # 并不懂下一句什么意思
    ssc.checkpoint("checkpoint")

    # host, port = sys.argv[1:]
    # Create a DStream that will connect to hostname:port
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    # lines.pprint()
    # Split each line into word
    body_dict = lines.map(lambda s: json.loads(s))
    #body_dict.pprint()
    user_flux = body_dict.map(lambda body_dict: (body_dict['user'], body_dict['body_bytes_sent']))
    running_counts = user_flux.updateStateByKey(updateFunc)
    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
