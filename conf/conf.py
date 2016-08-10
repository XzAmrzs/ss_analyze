from utils import tools
import time

# DATABASE
DATABASE_DRIVER_HOST = 'localhost'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME = 'hls'
COLLECTION_NAME = 'nodeHls'

# utils
LOG_PATH = '/data/spark_runtime_log/'
TIME_YMD = tools.timeFormat('%Y%m%d', int(time.time()))

# Kafka
KAFKA_BROKERS = '192.168.1.72:9092,192.168.1.150:9092'
KAFKA_TOPIC = 'nodeHls'

#zookeeper
ZK_SERVERS = '192.168.1.72:2181'

# Spark
APP_NAME = 'HLS_Analyze'
CHECKPOINT_DIR = 'checkpoint'


