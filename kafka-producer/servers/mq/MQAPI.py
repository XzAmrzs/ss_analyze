# coding=utf-8
# @MQ
# host http://192.168.1.58
# topic
# partition 
# key
from __future__ import print_function
import json
import requests


class MQAPI(object):
    def __init__(self, host, topic, userKey):
        self.host = host
        self.topic = topic
        self.userKey = userKey
        self.s = requests.session()

    def getOffset(self, partition):
        getUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
        urlop = self.s.get(getUrl)
        offsets = json.loads(urlop.text)
        return offsets

    def pullData(self, partition, cur):
        pullUrl = '/'.join([self.host, 'messages', self.topic, str(partition), str(cur)])
        pull = self.s.get(pullUrl, timeout=10)
        data = json.loads(pull.text)
        return data

    def setOffset(self, partition, cur):
        try:
            postUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
            headers = {"Content-type": "application/json"}
            info = {"offset": cur}
            data = json.dumps(info)
            req = self.s.post(postUrl, data, headers=headers)
            if req.status_code == 201:
                return 1
            else:
                return 0
        except Exception as e:
            print(e)
            return 0


if __name__ == '__main__':
    mq = MQAPI('http://api.mq.aodianyun.com/v1', 'nodeHls', 'XZPTest')
    offsets = mq.getOffset(0)
    print(offsets)
    data = mq.pullData(0, offsets['offset'])
    import pprint
    pprint.pprint(data)
    print(mq.setOffset(0, data['nextOffset']))
    print(mq.getOffset(0)['offset'])