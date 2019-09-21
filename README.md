# BPStreamEngine
 
编译打包，利用spark-submit执行

```Shell
$SPARK_HOME/bin/spark-submit \
--name alfred-streaming-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--jars jars/kafka-clients-2.1.0.jar,jars/spark-sql-kafka-0-10_2.11-2.3.0.jar,jars/spark-streaming-kafka-0-10_2.11-2.3.0.jar,jars/spark-streaming_2.11-2.3.0.jar \
--files secrets/kafka.broker1.keystore.jks,secrets/kafka.broker1.truststore.jks \
--class main target/BP-Stream-Engine-1.0-SNAPSHOT.jar
```
