# DATABASE
# DATABASE_DRIVER_HOST = 'ops'
DATABASE_DRIVER_HOST = '10.161.135.123'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME = 'hls_xzp'

DATABASE_TABLES_HLS_R = (
    'hls_r',
    'hls_r_app',
    'hls_r_app_location',

)
DATABASE_TABLES_HLS_R_S = (
    'hls_r_server',
)


DATABASE_TABLES_HLS_S = (
    'hls_s',
    'hls_s_app',
    'hls_s_app_location',
)

DATABASE_TABLES_HLS_S_S = (
    'hls_s_server',
)
# utils
LOG_PATH = '/data/spark_runtime_log/'

# Kafka
# KAFKA_BROKERS = 'ops:9092,test2:9092,AD138:9092'
KAFKA_BROKERS = 'kafka-master:9092,kafka-slave01:9092,kafka-slave02:9092'
KAFKA_TOPICS = ['nodeHls']

# zookeeper
ZK_SERVERS = 'localhost:2181'

# Spark
APP_NAME = 'HLS_Analyze'
