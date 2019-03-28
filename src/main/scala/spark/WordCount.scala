package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Example spark structured streaming application
  */
object WordCount {

  val LOG = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      LOG.info("usage: <kafka.bootstap.servers> <topicName> <checkpointBasePath>")
      System.exit(1)
    }

    val bootstrapServer = args(0)
    val topicName = args(1)
    val checkpointBasePath = args(2)

    val spark = SparkSession.builder.appName("WordCountKafka").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()

    val word = lines.select('value).as[String].flatMap(_.split(" "))

    val wordCounts = word.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation", s"$checkpointBasePath/$topicName")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
