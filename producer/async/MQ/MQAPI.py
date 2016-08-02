# coding=utf-8
# @MQ
# host http://192.168.1.58
# topic
# partition 
# key
from __future__ import print_function
import urllib2
import json


class MQAPI(object):
    def __init__(self, host, topic, userKey):
        self.host = host
        self.topic = topic
        self.userKey = userKey

    def getOffset(self, partition):
        """
        :param partition: int
        :param userKey: str
        :return: dict{'startOffset','lastOffset','offset'}
        """
        getUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
        print(getUrl)
        urlop = urllib2.urlopen(getUrl)
        offsets = json.loads(urlop.read())
        urlop.close()
        return offsets

    def pullData(self, partition, cur):
        pullUrl = '/'.join([self.host, 'messages', self.topic, str(partition), str(cur)])
        print(pullUrl)
        pull = urllib2.urlopen(pullUrl, timeout=10)
        data = json.loads(pull.read())
        return data

    def setOffset(self, partition, cur):
        postUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
        print(postUrl)
        headers = {"Content-type": "application/json"}
        info = {"offset": cur}
        data = json.dumps(info)
        req = urllib2.Request(postUrl, data, headers)
        postData = urllib2.urlopen(req)
        postData.close()
        return 1


if __name__ == '__main__':
    mq = MQAPI('http://api.mq.aodianyun.com/v1','nodeHls','XZP')
    offsets = mq.getOffset(0)
    print(offsets)
    print(offsets['offset'])
    # data = pullData('http://api.mq.aodianyun.com/v1', 'nodeHls', 0,  689467667)
    # print(data)
    # setOffset('http://api.mq.aodianyun.com/v1', 'nodeHls', 0, offsets['startOffset'], 'XZP')
    # setOffset('http://api.mq.aodianyun.com/v1', 'nodeHls', 0, 672032383, 'XZP')
    # data_dict = json.loads(data)
    # pre_datas = data_dict['list']
    #
    # for data in pre_datas:
    #     # print data
    #     body = data.get('body', 'Error:no body keyword')
    #     # body = data['body']
    #     # print(body)
    #     body_dict = json.loads(body, encoding='utf-8')
    #     print(body_dict)
    #
    #     # print body_dict.get('remote_addr','Error:no remote_addr keyword')
    #     # print body_dict.get('request_length', 'Error:no request.length keyword')
    #     size = body_dict.get('body_bytes_sent', 'Error:no body_bytes_sent keyword')
    #     ip = body_dict.get('remote_addr', 'Error:no remote_addr keyword')
    #     print(size)
    #     print(ip)
    # spark streaming的输入源要求是一个持续不断的输入流，现在有个输入源是restapi格式那种的http请求，
