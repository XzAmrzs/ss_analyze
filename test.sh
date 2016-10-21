#rm -rf /home/xzp/checkpoint 
#hadoop fs -rm -r /home/xzp/checkpoint
#spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 --master spark://test1:7077 flux_analyze_test.py
#spark-submit --jars external/spark-streaming-kafka-assembly_2.10-1.6.0.jar --master spark://test1:7077 --executor-memory 1024MB --total-executor-cores 2 flux_analyze_test.py

spark-submit --jars external/spark-streaming-kafka-assembly_2.10-1.6.0.jar test.py
#spark-submit --master spark://test1:7077 flux_analyze_test.py

