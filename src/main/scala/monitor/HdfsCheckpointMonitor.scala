package monitor

import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.HTTPServer
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger


/**
  * Example app that reads HDFS checkpoint file, and export metrics for prometheus using prometheus simple-client
  */

object HdfsCheckpointMonitor {

  val LOG = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      LOG.info("args: <prometheus exporter http port> <hdfsPath checkpoint basePath>")
      System.exit(1)
    }

    var httpPort = Integer.parseInt(args(0))
    val checkpointBasepath = args(1)

    LOG.info(s"httpPort: $httpPort")
    LOG.info(s"checkpointBasepath: $checkpointBasepath")

    new HTTPServer(httpPort)
    var offsetGauge = Gauge.build()
      .name("kafka_topic_offset")
      .help("kafka topic_offset")
      .labelNames("topic", "app_name", "partition", "offset_type")
      .register()

    while (true) {
      try {
        run(checkpointBasepath, offsetGauge)
      } catch {
        case e : Exception =>
          // continue to run to ignore intermittent HDFS error
          LOG.error(e.getMessage)
      }
      Thread.sleep(5000)
    }
  }

  /**
    * (1) Get all folder under the checkpointBasepath
    * (2) For each checkpoints, get the offset file in hdfs with the latest timestamp
    * (3) Use the offset file to get current offset
    */
  def run(checkpointBasepath: String, offsetGauge: Gauge) : Unit = {

    // (1) Get all folder under the checkpointBasepath
    val basePath = new Path(checkpointBasepath)
    var conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    var fs = basePath.getFileSystem(conf)
    val cpfolderIter = fs.listStatus(basePath)
    var checkpoints = new ArrayBuffer[Path]
    for (p <- cpfolderIter) {
      val cpPath = new Path(p.getPath.toString + "/offsets")
      if (p.isDirectory && fs.exists(cpPath)) {
        checkpoints.append(cpPath)
      }
    }

    // (2) For each checkpoints, get the offset file in hdfs with the latest timestamp
    for (cpPath <- checkpoints) {
      var time: Long = Long.MinValue
      var filePathOpt: Option[Path] = None
      for (s <- fs.listStatus(cpPath)) {
        if (s.getModificationTime > time
          && !s.getPath.getName.endsWith("tmp")
          && !s.getPath.getName.startsWith(".")) {
          time = s.getModificationTime
          filePathOpt = Some(s.getPath)
        }
      }

      // (3) Use the offset file to get current offset
      if (filePathOpt.isDefined) {
        var inputStream = fs.open(filePathOpt.get)
        val lines = IOUtils.readLines(inputStream, StandardCharsets.UTF_8).asScala
        val sparkAppName = cpPath.getParent().getName() // use folder as the convention
        // skip first 2 line which is non-json version label
        for (line <- lines.slice(2, lines.length)) {
          val map: util.HashMap[String, util.HashMap[String, Double]] = new Gson()
            .fromJson(line,
              new TypeToken[util.HashMap[String, util.HashMap[String, Double]]] {}.getType)
          for (key <- map.keySet.asScala) {
            val partOffsetMap = map.get(key)
            for (partOffset <- partOffsetMap.asScala) {
              val partition = partOffset._1
              val offset = partOffset._2

              LOG.info(s"topic: $key appName: $sparkAppName " +
                s"partition: $partition offset: $offset")
              offsetGauge.labels(key, sparkAppName, partition, "current")
                .set(offset.doubleValue)
            }
          }
        }
      }
    }
    fs.close()
    LOG.info("=========== END  =============")
  }

}
