import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

class PeakDetectionNested extends PeakDetection{
  override def calcCDR(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        (c.cellId1stCellCalling,
         c.dateTime.getHourOfDay,
         c.dateTime.getDayOfWeek,
         c.dateTime.getDayOfYear)
      case _ => None
     }.filter(_ != None)
  }

  override def calcVoronoi(cdr: RDD[_]) =
    cdr.map{case (id: String, _, _, _) => id}.distinct.take(10)
}

object PeakDetectionNested extends PeakDetectionNested{
  override def calcDataRaws(cdr: RDD[_]) = {
    cdr.map{case (id: String, hour: Int, dow: Int, doy: Int) =>
      val tmp = cdr.filter{ case (id2: String, hour2: Int, dow2: Int, doy2: Int) => {
        (id == id2 &&
         hour == hour2 &&
         dow == dow2 &&
         doy == doy2)
      } }
      val num = tmp.collect.length
      new DataRaw(id, hour, dow, doy, num)
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
