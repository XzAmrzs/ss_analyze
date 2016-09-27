# DATABASE
DATABASE_DRIVER_HOST = 'ops'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME = 'hls'

# utils
LOG_PATH = '/data/spark_runtime_log/'

# Kafka
KAFKA_BROKERS = 'ops:9092,test2:9092,AD138:9092'
KAFKA_TOPICS = ['nodeHlsTest']

#zookeeper
ZK_SERVERS = 'localhost:2181'

# Spark
APP_NAME = 'HLS_Analyze'
#CHECKPOINT_DIR = 'hdfs://$HOSTNAME:9000/data/checkpoint'


