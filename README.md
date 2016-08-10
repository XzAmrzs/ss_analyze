利用 Spark Streaming 构建一个高效健壮的流数据计算系统，我们还需要注意以下方面。

1. 需要合理的设置数据处理的间隔，即需要保证每一批数据的处理时间必须小于处理间隔，保证在处理下一批数据的时候，前一批已经处理完毕。显然
   这需要由您的 Spark 集群的计算能力还有 input 数据的量决定。

2. 需要尽可能的提升读取 input 数据的能力。在 Spark Streaming 与外部系统如 Kafka，Flume 等集成时，为了避免接收数据环节成为系统的瓶
   颈，我们可以启动多个 ReceiverInputDStream 对象实例。

4. 由于流计算对实时性要求很高，所以任何由于 JVM Full GC 引起的系统暂停都是不可接受的。除了在程序中合理使用内存，并且定期清理不需要的
   缓存数据外，CMS(Concurrent Mark and Sweep) GC 也是被 Spark 官方推荐的 GC 方式，它能有效的把由于 GC 引起的暂停维持在一个在很低
   的水平。我们可以在使用 spark-submit 命令时通过增加 --driver-java-options 选项来添加 CMS GC 相关的参数。

5. 在 Spark 官方提供关于集成 Kafka 和 Spark Streaming 的指导文档中，提到了两种方式，第一种是 Receiver Based Approach，即通过在
   Receiver 里实现 Kafka consumer 的功能来接收消息数据;第二种是 Direct Approach, 即不通过 Receiver，而是周期性的主动查询 Kafka
   消息分区中的最新 offset 值，进而去定义在每个 batch 中需要处理的消息的 offset 范围。本文采用的是第一种方式，因为目前第二种方式还
   处于试验阶段。

6. 如果采用 Receiver Based Approach 集成 Kafka 和 Spark Streaming，就需要考虑到由于 Driver 或者 Worker 节点宕机而造成的数据丢
   失的情况，在默认配置下，是有可能造成数据丢失的，除非我们开启 Write Ahead Log(WAL) 功能。在这种情况下，从 Kafka 接收到的消息数
   据会同步的被写入到 WAL 并保存到可靠的分布式文件系统上，如 HDFS。可以通过在 Spark 配置文件中 (conf/spark-defaults.conf) 把
   spark.streaming.receiver.writeAheadLog.enable 配置项设置成 true 开启这个功能。当然在开启 WAL 的情况下，会造成单个 Receiver
   吞吐量下降，这时候，我们可能需要并行的运行多个 Receiver 来改善这种情况。

7. 由于 updateStateByKey 操作需要开启 checkpoint 功能，但是频繁的 checkpoint 会造成程序处理时间增长，也会造成吞吐量下降。默认情
   况下，checkpoint 时间间隔会取 steaming 程序数据处理间隔或者 10 秒两者中较大的那个。官方推荐的间隔是 streaming 程序数据处理间
   隔的 5-10 倍。可以通过 dsteam.checkpoint(checkpointInterval) 来设置，参数需要用样本类 Duration 包装下，单位是毫秒。