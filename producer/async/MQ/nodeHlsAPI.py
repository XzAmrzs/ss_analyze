# coding = utf-8
import json
import urllib2
import time
# from MQAPI import MQAPI


class NodeHlsAPI(object):
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


nodeHlsAPI = NodeHlsAPI('http://api.mq.aodianyun.com/v1', 'nodeHls', 'XZP')

if __name__ == '__main__':
    data = nodeHlsAPI.getOffset(0)
    data = nodeHlsAPI.pullData(0, data['startOffset'])
    print(data)
