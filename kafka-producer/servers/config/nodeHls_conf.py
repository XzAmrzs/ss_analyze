#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/4 0004 下午 2:44
MQ_TOPIC = 'nodeHls'
MQ_URL = 'http://api.mq.aodianyun.com/v1'
MQ_USERKEY = 'XZPtest'

PARTITION_NUM = 9
log_producer_Path = './log/'

# Kafka
# KAFKA_BROKERS = ['kafka-master:9092', 'kafka-slave01:9092', 'kafka-slave02:9092']
KAFKA_BROKERS = 'ops:9092,test2:9092,AD138:9092'
KAFKA_TOPIC = 'nodeHls'
