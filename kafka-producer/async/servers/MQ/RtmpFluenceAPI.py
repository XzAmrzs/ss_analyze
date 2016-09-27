#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/30 0030 上午 10:25

import json
import urllib2
import time

class RtmpFluenceAPI(object):
    def __init__(self, host, topic, userKey):
        # super(NodeHlsAPI, self).__init__(host, topic, userKey)
        self.host = host
        self.topic = topic
        self.userKey = userKey

    def getOffset(self, partition, **kwargs):
        getUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
        print(getUrl)
        urlop = urllib2.urlopen(getUrl)
        offsets = json.loads(urlop.read())
        urlop.close()
        return offsets

    def pullData(self, partition, cur, **kwargs):
        pullUrl = '/'.join([self.host, 'messages', self.topic, str(partition), str(cur)])
        connCount = 0
        pull = None
        print(pullUrl)
        while connCount < 5:
            try:
                pull = urllib2.urlopen(pullUrl, timeout=10)
                break
            except:
                time.sleep(1)
                connCount += 1
        data = json.loads(pull.read())
        return data

    def setOffset(self, partition, cur, **kwargs):
        postUrl = '/'.join([self.host, 'offsets', self.topic, str(partition), self.userKey])
        print(postUrl)
        headers = {"Content-type": "application/json"}
        info = {"offset": cur}
        data = json.dumps(info)
        req = urllib2.Request(postUrl, data, headers)
        postData = urllib2.urlopen(req)
        postData.close()
        return 1


rtmpFluenceAPI = RtmpFluenceAPI('http://api.mq.aodianyun.com/v1', 'RtmpFluence', 'XZP')

if __name__ == '__main__':
    import pprint
    data = rtmpFluenceAPI.getOffset(0)
    data = rtmpFluenceAPI.pullData(0, data['startOffset'])
    pprint.pprint(data)