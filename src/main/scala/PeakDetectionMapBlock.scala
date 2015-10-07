import java.util.Calendar
import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

object PeakDetectionMapBlock extends PeakDetection{
  override def calcDataRaws(cdr: RDD[_]) = {
    cdr.mapBlock( array1 =>
      array1.groupBy(e => e).map{ case(k, v) => (k, v.length)  }.toArray
    ).groupByKey.map{ case (cdr@CDR(_, _, _, _), buff: Iterable[Int]) =>
      DataRaw(cdr, buff.reduce(_+_))
    }
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = (s"Usage: submit.sh ${appName} <master> <cdrLocation> " +
                 "<voronoi> <outputLocation> " +
                 s"<baseSince (${Call.datePattern})> " +
                 s"<baseUntil (${Call.datePattern})> " +
                 "<maxCores> <driverMem> " +
                 "<executorMem>")

    if (args.length != 9) {
      System.err.println(usage)
      System.exit(1)
    }

    val master = args(0)
    val f = args(1)
    val voronoi = args(2).split(",")
    val outputLocation=args(3)
    val baseSince = Call.dateFormat.parseDateTime(args(4))
    val baseUntil = Call.dateFormat.parseDateTime(args(5))
    val maxCores = args(6)
    val driverMem = args(7)
    val executorMem = args(8)

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.cores.max", maxCores)
      .set("spark.driver.memory", driverMem)
      .set("spark.executor.memory", executorMem)
    val sc = new SparkContext(conf)

    val data = sc.textFile(f)

    run(data, voronoi, baseSince, baseUntil, outputLocation)
  }
}
