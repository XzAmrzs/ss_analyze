# DATABASE
DATABASE_DRIVER_HOST = 'kafka-master'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME = 'hls'

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


