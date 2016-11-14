
## Spark调优:

1. 由于流计算对实时性要求很高，所以任何由于 JVM Full GC 引起的系统暂停都是不可接受的。除了在程序中合理使用内存，并且定期清理不需要的
   缓存数据外，CMS(Concurrent Mark and Sweep) GC 也是被 Spark 官方推荐的 GC 方式，它能有效的把由于 GC 引起的暂停维持在一个在很低
   的水平。我们可以在使用 spark-submit 命令时通过增加 --driver-java-options 选项来添加 CMS GC 相关的参数

2. 由于 updateStateByKey 操作需要开启 checkpoint 功能，但是频繁的 checkpoint 会造成程序处理时间增长，也会造成吞吐量下降。默认情
   况下，checkpoint 时间间隔会取 steaming 程序数据处理间隔或者 10 秒两者中较大的那个。官方推荐的间隔是 streaming 程序数据处理间
   隔的 5-10 倍。可以通过 dsteam.checkpoint(checkpointInterval) 来设置，参数需要用样本类 Duration 包装下，单位是毫秒。

3. 这里又碰到了一个问题，从consumer offsets到leader latest offsets中间延迟了很多消息，在下一次启动的时候，首个batch要处理大量的
   消息，会导致spark-submit设置的资源无法满足大量消息的处理而导致崩溃。因此在spark-submit启动的时候多加了一个配置:
   **--conf spark.streaming.kafka.maxRatePerPartition=10000**限制每秒钟从topic的每个partition最多消费的消息条数，这样就把首个
   batch的大量的消息拆分到多个batch中去了，为了更快的消化掉delay的消息，可以调大计算资源和把这个参数调大


## kafka:

Step 1: Start the server
```
bin/kafka-server-start.sh -daemon config/server.properties

/usr/local/kafka_2.11-0.10.0.0/bin/kafka-server-start.sh -daemon /usr/local/kafka_2.11-0.10.0.0/config/server.properties
```

Step 2: Create a topic(replication-factor一定要大于1，否则kafka只有一份数据，leader一旦崩溃程序就没有输入源了，分区数目视输入源而定)
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic nodeHls
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic RtmpFluence
```

Step 3: Describe a topic
```
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic nodeHls
```

step 3: list the topic
```
bin/kafka-topics.sh --list --zookeeper localhost:2181
```

step 4: send some message
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic nodeHlsTest
```

step 5: start a consumer
```
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic nodeHlsTest --from-beginning
```

step 6: delete a topic
要事先在 `serve.properties` 配置 `delete.topic.enable=true`
```
bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic nodeHls
# 如果仍然只是仅仅被标记了删除(zk中并没有被删除)，那么启动zkCli.sh,输入如下指令
rmr /brokers/topics/nodeHls
```

step 7: 查看kafka相应分区的最新下标
```
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nodeHls --time -1
```

## 项目初始化指令:
创建kafka主题:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic nodeHls
```

创建mongo索引:
```
mongod --fork --logpath /data/logs/mongodb/mongo.log --logappend

mongo kafka-master:27017

db.hls_up.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_up_s.ensureIndex({server_addr:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_up_a.ensureIndex({app:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_down.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_down_a.ensureIndex({app:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_down_s.ensureIndex({server_addr:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.hls_down_user.ensureIndex({user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.hls_down_user_hour.ensureIndex({user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.hls_down_app_stream.ensureIndex({app:1,stream:1,user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.hls_down_app_stream_hour.ensureIndex({app:1,stream:1,user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.hls_down_httpCode.ensureIndex({httpCode:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.HlsDayData.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.HlsUserData.ensureIndex({user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.HlsStreamData.ensureIndex({app:1,stream:1,timestamp:-1},{background:true,unique:true,dropDups:true})


db.rtmp_up.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_up_app.ensureIndex({app:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_up_server.ensureIndex({svr_ip:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_down.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_down_app.ensureIndex({app:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_down_server.ensureIndex({svr_ip:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_down_user.ensureIndex({user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.rtmp_down_app_stream.ensureIndex({app:1,stream:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.rtmp_down_user_hour.ensureIndex({user:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.rtmp_down_app_stream_hour.ensureIndex({app:1,stream:1,timestamp:-1},{background:true,unique:true,dropDups:true})
db.rtmp_forward.ensureIndex({timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_forward_app.ensureIndex({app:1,timestamp: -1},{background:true,unique:true,dropDups:true})
db.rtmp_forward_server.ensureIndex({svr_ip:1,timestamp: -1},{background:true,unique:true,dropDups:true})

db.hls_up.drop()
db.hls_down.drop()
db.hls_up_s.drop()
db.hls_down_s.drop()
db.hls_up_a.drop()
db.hls_down_a.drop()
db.hls_down_user.drop()
db.hls_down_user_hour.drop()
db.hls_down_app_stream.drop()
db.hls_down_app_stream_hour.drop()
db.hls_down_httpCode.drop()
db.HlsDayData.drop()
db.HlsUserData.drop()
db.HlsStreamData.drop()

db.rtmp_up.drop()
db.rtmp_up_app.drop()
db.rtmp_up_server.drop()
db.rtmp_down.drop()
db.rtmp_down_app.drop()
db.rtmp_down_server.drop()
db.rtmp_down_user.drop()
db.rtmp_down_app_stream.drop()
db.rtmp_down_user_hour.drop()
db.rtmp_down_app_stream_hour.drop()
db.rtmp_forward.drop()
db.rtmp_forward_app.drop()
db.rtmp_forward_server.drop()
```

## 启动与重启服务:
```
/usr/local/zookeeper-3.4.8/bin/zkServer.sh start
# wait 5s

/usr/local/kafka_2.10-0.8.2.2/bin/kafka-server-stop.sh

/usr/local/kafka_2.10-0.8.2.2/bin/kafka-server-start.sh -daemon /usr/local/kafka_2.10-0.8.2.2/config/server.properties
# wait 5s

# master主机宕机情况:
/usr/local/work_space/hadoop/sbin/stop-dfs.sh
# wait 5s
/usr/local/work_space/hadoop/sbin/start-dfs.sh
# wait 5s
/usr/local/work_space/spark/sbin/start-master.sh

nohup sh /home/xzp/ss_analyze/run.sh &

# slave01主机宕机情况:
# zk和kfaka同上
# spark
/usr/local/work_space/spark/sbin/start-slave.sh
/home/xzp/producer/async/run.sh &


kill命令
ps -ef|grep nodeHls_multiprocess |grep -v grep |awk '{print "kill -9",$2}' |sh
ps -ef|grep rtmp_multiprocess |grep -v grep |awk '{print "kill -9",$2}' |sh
ps -ef|grep redis2mongo |grep -v grep |awk '{print "kill -9",$2}' |sh
jps|grep -i kafka|grep -v grep |awk '{print "kill -9",$1}' |sh
jps|grep -i QuorumPeerMain|grep -v grep |awk '{print "kill -9",$1}' |sh
```

## 更新程序:
1. 暂停MQ进程(在所有机器上执行,)
```
ps -ef|grep -i nodeHls|grep -v grep |awk '{print "kill -2",$2}' |sh
ps -ef|grep -i rtmp|grep -v grep |awk '{print "kill -2",$2}' |sh
```
2. 打开kafka-master:4040页面，当streaming页面显示接入的数据为0的时候，将spark程序关闭
```
jps|grep -i sparksubmit|grep -v grep |awk '{print "kill -9",$1}' |sh
```
3. 更新并启动新程序
```
nohup sh ./run.sh &
```
4. 恢复MQ进程
```
jobs 
bg jobs_id
```
