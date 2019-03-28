package producer

import java.util.Properties

import monitor.KafkaOffsetMonitor.LOG
import org.apache.kafka.clients.producer._
import org.apache.log4j.Logger

object DataProducer {


  val LOG = Logger.getLogger(this.getClass.getName)
  // extract from alice in wonderland
  val tokens = """
  |Alice was beginning to get very tired of sitting by her sister
  |on the bank, and of having nothing to do:  once or twice she had
  |peeped into the book her sister was reading, but it had no
  |pictures or conversations in it, `and what is the use of a book,'
  |thought Alice `without pictures or conversation?'
  |
  |  So she was considering in her own mind (as well as she could,
  |for the hot day made her feel very sleepy and stupid), whether
  |the pleasure of making a daisy-chain would be worth the trouble
  |of getting up and picking the daisies, when suddenly a White
  |Rabbit with pink eyes ran close by her.
  |
  |  There was nothing so VERY remarkable in that; nor did Alice
  |think it so VERY much out of the way to hear the Rabbit say to
  |itself, `Oh dear!  Oh dear!  I shall be late!'  (when she thought
  |it over afterwards, it occurred to her that she ought to have
  |wondered at this, but at the time it all seemed quite natural);
  |but when the Rabbit actually TOOK A WATCH OUT OF ITS WAISTCOAT-
  |POCKET, and looked at it, and then hurried on, Alice started to
  |her feet, for it flashed across her mind that she had never
  |before seen a rabbit with either a waistcoat-pocket, or a watch to
  |take out of it, and burning with curiosity, she ran across the
  |field after it, and fortunately was just in time to see it pop
  |down a large rabbit-hole under the hedge.
""".stripMargin.split("\\s+")
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      LOG.info("args: <<kafka.bootstap.servers>>")
      System.exit(1)
    }

    var kafkaBootstrapServers = args(0)

    LOG.info(s"kafkaBootstrapServers: $kafkaBootstrapServers")

    val  props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrapServers)

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "wordcount_input"
    val r = scala.util.Random
    val sb = new StringBuilder()
    while (true) {
      for(i <- 1 to 50){
        val randIndex = r.nextInt(tokens.size)
        sb.append(tokens(randIndex))
        sb.append(" ")
      }
      val line = sb.toString
      val record = new ProducerRecord(TOPIC, "key", line)
      LOG.info(line)
      producer.send(record)
      sb.clear
      Thread.sleep(1000);
    }

    producer.close()

  }
}