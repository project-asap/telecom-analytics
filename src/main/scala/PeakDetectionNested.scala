import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

object PeakDetectionNested extends PeakDetection{
  override def calcDataRaws(cdr: RDD[_]) = {
    cdr.map{case c1@CDR(_, _, _, _) =>
      val tmp = cdr.filter{ case c2@CDR(_, _, _, _) =>
        (c1.id == c2.id && c1.hour == c2.hour && c1.doy == c2.doy)
      }
      val num = tmp.collect.length
      DataRaw(c1, num)
    }.distinct.cache
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = (s"Usage: submit.sh ${appName} <master> <cdrLocation> " +
                 "<outputLocation> " +
                 s"<baseSince (${Call.datePattern})> " +
                 s"<baseUntil (${Call.datePattern})> " +
                 "<maxCores> <driverMem> " +
                 "<executorMem>")

    if (args.length != 8) {
      System.err.println(usage)
      System.exit(1)
    }

    val master = args(0)
    val f = args(1)
    val outputLocation=args(2)
    val baseSince = Call.dateFormat.parseDateTime(args(3))
    val baseUntil = Call.dateFormat.parseDateTime(args(4))
    val maxCores = args(5)
    val driverMem = args(6)
    val executorMem = args(7)

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.cores.max", maxCores)
      .set("spark.driver.memory", driverMem)
      .set("spark.executor.memory", executorMem)
    val sc = new SparkContext(conf)

    val data = sc.textFile(f)

    run(data, baseSince, baseUntil, outputLocation)
  }
}
