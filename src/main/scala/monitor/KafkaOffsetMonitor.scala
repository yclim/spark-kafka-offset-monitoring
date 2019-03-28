package monitor

import java.util.Properties

import scala.collection.JavaConverters._
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.HTTPServer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.log4j.Logger


/**
  * Example expose kafka begin and end offset of all topics in kafka using prometheus simple-client
  */
object KafkaOffsetMonitor {

  val LOG = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      LOG.info("args: <prometheus exporter http port> <kafka bootstrap servers>")
      System.exit(1)
    }

    var httpPort = Integer.parseInt(args(0))
    var kafkaBootstrapServers = args(1)

    LOG.info(s"httpPort: $httpPort")
    LOG.info(s"kafkaBootstrapServers: $kafkaBootstrapServers")

    new HTTPServer(httpPort)
    var offsetGauge = Gauge.build()
      .name("kafka_topic_offset")
      .help("kafka topic_offset")
      .labelNames("topic", "partition", "offset_type")
      .register()

    val config = new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaMonitor")
    val consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)

    while (true) {
      val topics = consumer.listTopics().keySet().asScala
      topics.filter(_ != "__consumer_offsets").foreach { topic =>
        val partitions = listPartitionInfo(consumer, topic) match {
          case None =>
            LOG.info(s"Topic $topic does not exist")
            System.exit(1)
            Seq()
          case Some(p) if p.isEmpty =>
            LOG.info(s"Topic $topic has 0 partitions")
            System.exit(1)
            Seq()
          case Some(p) => p
        }

        val topicPartitions = partitions.sortBy(_.partition).flatMap { p =>
          if (p.leader == null) {
            LOG.info(s"Error: partition ${p.partition} does not have a leader. Skip getting offsets")
            None
          } else {
            Some(new TopicPartition(p.topic, p.partition))
          }
        }

        val endPartitionOffsets = consumer.endOffsets(topicPartitions.asJava).asScala

        endPartitionOffsets.toSeq.sortBy { case (tp, _) => tp.partition }.foreach {
          case (tp, offset) =>
            if (Option(offset).isDefined) {
              offsetGauge.labels(topic, tp.partition.toString, "end")
                .set(offset.doubleValue)
            }
            LOG.info(s"topic: $topic partition: ${tp.partition} end_offset:${Option(offset).getOrElse("")}")
        }

        val beginPartitionOffsets = consumer.beginningOffsets(topicPartitions.asJava).asScala

        beginPartitionOffsets.toSeq.sortBy { case (tp, _) => tp.partition }.foreach {
          case (tp, offset) =>
            if (Option(offset).isDefined) {
              offsetGauge.labels(topic, tp.partition.toString, "begin")
                .set(offset.doubleValue)
            }
            LOG.info(
              s"topic: $topic partition: ${tp.partition} begin_offset:${Option(offset).getOrElse("")}")
        }
      }
      LOG.info("=========== END  =============")
      Thread.sleep(5000);
    }
  }

  def listPartitionInfo(consumer: KafkaConsumer[_, _], topic: String): Option[Seq[PartitionInfo]] = {
    val partitionInfo = consumer.listTopics.asScala
      .filterKeys(_ == topic)
      .values
      .flatMap(_.asScala)
      .toBuffer
    if (partitionInfo.isEmpty) {
      None
    } else {
      Some(partitionInfo)
    }
  }
}

