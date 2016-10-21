# coding = utf-8
from __future__ import print_function

from MQAPI import MQAPI
from ..config import nodeHls_conf as conf
mq_topic = conf.MQ_TOPIC
mq_url = conf.MQ_URL
mq_userKey = conf.MQ_USERKEY


class NodeHlsAPI(MQAPI):
    pass

mqAPI = NodeHlsAPI(mq_url, mq_topic, mq_userKey)

if __name__ == '__main__':
    import pprint
    data = mqAPI.getOffset(0)
    data = mqAPI.pullData(0, data['startOffset'])
    pprint.pprint(data)
