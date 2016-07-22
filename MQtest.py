# coding=utf-8
# @MQ
# host http://192.168.1.58
# topic
# partition 
# key
from __future__ import print_function
import urllib2
import json


def getOffset(host, topic, partition, userKey):
    getUrl = '/'.join([host, 'offsets', topic, str(partition), userKey])
    print(getUrl)
    urlop = urllib2.urlopen(getUrl)
    offsets = json.loads(urlop.read())
    urlop.close()
    # print offsets
    return offsets


def pullData(host, topic, partition, cur):
    pullUrl = '/'.join([host, 'messages', topic, str(partition), str(cur)])
    print (pullUrl)
    pull = urllib2.urlopen(pullUrl, timeout=10)
    data = pull.read()
    # print data
    return data


def setOffset(host, topic, partition, cur, userKey):
    postUrl = '/'.join([host, 'offsets', topic, str(partition), userKey])
    print (postUrl)
    headers = {"Content-type": "application/json"}
    info = {"offset": cur}
    data = json.dumps(info)
    req = urllib2.Request(postUrl, data, headers)
    postData = urllib2.urlopen(req)
    postData.close()
    return 1


if __name__ == '__main__':
    offsets = getOffset('http://api.mq.aodianyun.com/v1', 'nodeHls_ttt', 0, 'XZP')
    data = pullData('http://api.mq.aodianyun.com/v1', 'nodeHls_ttt', 0, offsets['offset'])
    # setOffset('http://api.mq.aodianyun.com/v1', 'nodeHls_ttt', 0, 1623204, 'XZP')
    data_dict = json.loads(data)
    pre_datas = data_dict['list']
    for data in pre_datas:
        # print data
        body = data.get('body', 'Error:no body keyword')
        # body = data['body']
        body_dict = json.loads(body)
        # print body_dict

        # {"user": 4285, "time_local": "[15/Jan/2016:15:00:09 +0800]", "unix_time": "1452841209",
        #  "remote_addr": "59.51.90.178", "host": "4285.hlsplay.aodianyun.com",
        #  "request_url": "GET /1529/1529-stream-1452839610-145284119except socket.timeout:8584-6084.ts HTTP/1.1", "http_stat": 200,
        #  "request_length": 533, "body_bytes_sent": 92684, "http_referer": "-",
        #  "http_user_agent": "Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; HUAWEI C8813 Build/HuaweiC8813) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/6.3 Mobile Safari/537.36",
        #  "server_addr": "124.116.157.251", "upstream_addr": "-", "upstream_response_time": "-", "request_time": "0.000"}
        #
        # print body_dict.get('remote_addr','Error:no remote_addr keyword')
        # print body_dict.get('request_length', 'Error:no request.length keyword')
        size = body_dict.get('body_bytes_sent', 'Error:no body_bytes_sent keyword')
        print(size)
        # spark streaming的输入源要求是一个持续不断的输入流，现在有个输入源是restapi格式那种的http请求，
