# DATABASE
DATABASE_DRIVER_HOST = 'localhost'
DATABASE_DRIVER_PORT = 6379
DATABASE_NAME = 'hls'
COLLECTION_NAME = 'nodeHls'

# utils
LOG_PATH = '/data/spark_runtime_log/'

# Kafka
KAFKA_BROKERS = 'kafka-master:9092,kafka-slave01:9092,kafka-master:9092,kafka-slave01:9092'
KAFKA_TOPIC = 'nodeHls'

#zookeeper
ZK_SERVERS = 'node_1:2181'

# Spark
APP_NAME = 'HLS_Analyze'
CHECKPOINT_DIR = 'hdfs://$HOSTNAME:9000/data/checkpoint'


