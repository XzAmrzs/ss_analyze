# DATABASE
# DATABASE_DRIVER_HOST = 'ops'
DATABASE_DRIVER_HOST = '10.161.135.123'
DATABASE_DRIVER_PORT = 27017
DATABASE_NAME_HLS = 'hls_xzp'
DATABASE_NAME_RTMP = 'rtmp_xzp'

TABLES_HLS_R = (
    'hls_r',
    'hls_r_app',
    'hls_r_app_location',

)

TABLES_HLS_R_S = (
    'hls_r_server',
)


TABLES_HLS_S = (
    'hls_s',
    'hls_s_app',
    'hls_s_app_location',
)

TABLES_HLS_S_S = (
    'hls_s_server',
)

TABLES_RTMP_R = (
    'rtmp_r',
    'rtmp_r_app',

)

TABLES_RTMP_R_USER = (
    'rtmp_r_user',
)

TABLES_RTMP_R_S = (
    'rtmp_r_server',
)


TABLES_RTMP_S = (
    'rtmp_s',
    'rtmp_s_app',
)

TABLES_RTMP_S_USER = (
    'rtmp_s_user',
)

TABLES_RTMP_S_S = (
    'rtmp_s_server',
)


TABLES_RTMP_F = (
    'rtmp_f',
    'rtmp_f_app',
)

TABLES_RTMP_F_USER = (
    'rtmp_f_user',
)

TABLES_RTMP_F_S = (
    'rtmp_f_server',
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


