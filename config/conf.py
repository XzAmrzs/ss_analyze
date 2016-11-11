# coding=utf-8
# DATABASE
# DATABASE_DRIVER_HOST = 'ops'
DATABASE_DRIVER_HOST = '10.161.135.123'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME_HLS = 'HlsAnalysis'
DATABASE_NAME_RTMP = 'RtmpAnalysis'

# REDIS_HOST = '192.168.1.145'
REDIS_HOST = 'r-bp181d4d5916c0b4.redis.rds.aliyuncs.com'
REDIS_PORT = 6379
REDIS_DB = 0
# REDIS_PWD = None
REDIS_PWD = '0ec5bec6e968fb8c1A'

TABLES_HLS_UP_A = (
    'hls_up',
    'hls_up_a',
)
TABLES_HLS_UP_S = (
    'hls_up_s',
)

TABLES_HLS_UP_ASF = (
    'hls_up_app_stream',
)
TABLES_HLS_UP_ASHF = (
    'hls_up_app_stream_hour',
)

TABLES_HLS_UP_UF = (
    'hls_up_user',
)

TABLES_HLS_UP_UFH = (
    'hls_up_user_hour',
)

TABLES_HLS_UP_HC = (
    'hls_up_httpCode',
)

TABLES_HLS_DOWN_A = (
    'hls_down',
    'hls_down_a',
)
TABLES_HLS_DOWN_S = (
    'hls_down_s',
)

TABLES_HLS_DOWN_ASF = (
    'hls_down_app_stream',
)
TABLES_HLS_DOWN_ASHF = (
    'hls_down_app_stream_hour',
)

TABLES_HLS_DOWN_UF = (
    'hls_down_user',
)

TABLES_HLS_DOWN_UFH = (
    'hls_down_user_hour',
)

TABLES_HLS_DOWN_HC = (
    'hls_down_httpCode',
)


TABLES_RTMP_UP_A = (
    'rtmp_up',
    'rtmp_up_a',
)

TABLES_RTMP_UP_S = (
    'rtmp_up_s',
)

TABLES_RTMP_UP_ASF = (
    'rtmp_up_app_stream',
)
TABLES_RTMP_UP_ASHF = (
    'rtmp_up_app_stream_hour',
)

TABLES_RTMP_UP_UF = (
    'rtmp_up_user',
)

TABLES_RTMP_UP_UFH = (
    'rtmp_up_user_hour',
)


TABLES_RTMP_DOWN_A = (
    'rtmp_down',
    'rtmp_down_a',
)

TABLES_RTMP_DOWN_S = (
    'rtmp_down_s',
)

TABLES_RTMP_DOWN_ASF = (
    'rtmp_down_app_stream',
)
TABLES_RTMP_DOWN_ASHF = (
    'rtmp_down_app_stream_hour',
)

TABLES_RTMP_DOWN_UF = (
    'rtmp_down_user',
)

TABLES_RTMP_DOWN_UFH = (
    'rtmp_down_user_hour',
)

TABLES_RTMP_FORWARD_A = (
    'rtmp_forward',
    'rtmp_forward_a',
)

TABLES_RTMP_FORWARD_S = (
    'rtmp_forward_s',
)

TABLES_RTMP_FORWARD_ASF = (
    'rtmp_forward_app_stream',
)
TABLES_RTMP_FORWARD_ASHF = (
    'rtmp_forward_app_stream_hour',
)

TABLES_RTMP_FORWARD_UF = (
    'rtmp_forward_user',
)

TABLES_RTMP_FORWARD_UFH = (
    'rtmp_forward_user_hour',
)


# utils
LOG_PATH = '/data/spark_runtime_log/'

# Kafka
# KAFKA_BROKERS = 'ops:9092,test2:9092,AD138:9092'
KAFKA_BROKERS = 'kafka-master:9092,kafka-slave01:9092,kafka-slave02:9092'
NODE_FILTER = 'nodeHls'
RTMP_FILTER = 'RtmpFluence'
KAFKA_TOPICS = [NODE_FILTER, RTMP_FILTER]

# zookeeper
ZK_SERVERS = 'localhost:2181'

# Spark
APP_NAME = 'HLS_Analyze'
