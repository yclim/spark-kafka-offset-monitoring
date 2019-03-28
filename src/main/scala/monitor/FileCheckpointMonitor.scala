package monitor

import java.io.{File, FileFilter, FileInputStream}
import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.HTTPServer
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger

/**
  *  Example app that reads local spark structured streaming checkpoint file.
  *  For testing on your pc/laptop when running spark standalone mode
  */
object FileCheckpointMonitor {


  val LOG = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      LOG.info("args: <prometheus exporter http port> <checkpoint basePath>")
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
      .labelNames("topic", "partition", "offset_type")
      .register()

    while (true) {
      try {
        run(checkpointBasepath, offsetGauge)
      } catch {
        // continue to run to ignore intermittent filesystem error
        case e : Exception =>
          LOG.error(e.getMessage)
      }
      Thread.sleep(5000)
    }
  }

  /**
    * (1) Get all folder under the checkpointBasepath
    * (2) For each checkpoints, get the offset file with the latest timestamp
    * (3) Use the offset file to get current offset
    */
  def run(checkpointBasepath: String, offsetGauge: Gauge) : Unit = {
    // (1) Get all folder under the checkpointBasepath
    val dirs = new File(checkpointBasepath).listFiles(new FileFilter() {
      override def accept(f: File): Boolean = f.isDirectory
    })

    var checkpoints = new ArrayBuffer[File]
    for (p <- dirs) {
      val file = new File(p.getPath + "/offsets")
      if (file.exists) {
        checkpoints.append(file)
      }
    }

    // (2) For each checkpoints, get the offset file with the latest timestamp
    for (dir <- checkpoints) {
      var time: Long = Long.MinValue
      var fileOpt: Option[File] = None
      val files = dir.listFiles(new FileFilter() {
        override def accept(f: File): Boolean = f.isFile
      })

      for (f<- files) {
        if (f.lastModified > time && f.getName.matches("\\d+")) {
          time = f.lastModified
          fileOpt = Some(f)
        }
      }

      // (3) Use the offset file to get current offset
      if (fileOpt.isDefined) {

        var inputStream = new FileInputStream(fileOpt.get)
        val lines = IOUtils.readLines(inputStream, StandardCharsets.UTF_8).asScala
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

              LOG.info(s"topic: $key appName: partition: $partition offset: $offset")
              offsetGauge.labels(key, partition, "current")
                .set(offset.doubleValue)
            }
          }
        }
      }
    }
    LOG.info("=========== END  =============")
  }


}
