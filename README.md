# Spark Kafka Offset Monitoring Example 

Example project to showcase a simple approach to monitor processing rate and lag of a spark structured streaming application using prometheus. The project includes:
* **WordCount** - example spark structured streaming application
* **DataProducer** - produce sentences continuously to kafka topic
* **KafkaOffsetMonitor** - Reads begin and end offset of all topics in kafka using prometheus simple-client
* **HdfsCheckpointMonitor** -  Reads HDFS checkpoint file, and export metrics for prometheus using prometheus simple-client
* **FileCheckpointMonitor** - Reads local checkpoint file. For testing on your laptop when running spark standalone mode.


## Usage

```
cd spark-kafka-offset-monitoring
mvn package
cd target
```

```
# run wordcount spark application
spark-submit --deploy-mode client --master local --class spark.WordCount spark-kafka-offset-monitoring-1.0.jar localhost:9092 wordcount_input C:/tmp/cp1
```

# run data producer with kafka broker at localhost
```
java -cp spark-kafka-offset-monitoring-1.0.jar producer.DataProducer localhost:9092
```

# run kafka offset monitor at port 18080
```
java -cp spark-kafka-offset-monitoring-1.0.jar monitor.KafkaOffsetMonitor 18080 localhost:9092
```
# run FileCheckpointMonitor at port 18081 and local checkpoint base dir at C:/tmp/cp1

```
java -cp spark-kafka-offset-monitoring-1.0.jar monitor.FileOffsetMonitor 18081 C:/tmp/cp1
```