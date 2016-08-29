if [ ! -d "/data/spark_runtime_log" ]; then
    echo "初始化日志环境"
    mkdir /data/spark_runtime_log
fi

spark-submit --jars external/spark-streaming-kafka-assembly_2.10-1.6.0.jar --master spark://$HOSTNAME:7077 --executor-memory 6G --executor-cores 4 --conf spark.streaming.kafka.maxRatePerPartition=10000 run.py
#spark-submit --jars external/spark-streaming-kafka-assembly_2.10-1.6.0.jar --files ./config/spark-env.conf run.py


