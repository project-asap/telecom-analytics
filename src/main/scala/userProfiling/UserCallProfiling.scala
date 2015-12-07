package ta

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.Try

import org.apache.spark.rdd.RDD

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

object UserCallProfiling {
  def parseCall(l: String, delim: String = ";")  = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a).getOrElse(None)
  }

  def parseAntenna(l: String, delim: String = ";")  = {
    val a = l.split(delim, -1).map(_.trim)
    Antenna(a).getOrElse(None)
  }

  def run(data: RDD[String], geoData: RDD[String], since: DateTime,
          until: DateTime, outputLocation: String) {
    val antenne = geoData.map(parseAntenna(_)).map{
      case a@Antenna(_,_,_,_,_,_,_) => a
      case _ => None
    }.filter(_ != None)

    val cells = antenne.map{
      case Antenna(_, _, _, cell, _, _, _) => cell
    }.collect

    val stc = data.filter(_.length != 0).map(parseCall(_)).map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        SpaceTimeCall(
          c.callingSubscriberImsi,
          c.city,
          c.dateTime.getWeekOfWeekyear,
          c.isWeekday,
          c.timeSlot,
          c.dateTime.getDayOfYear,
          c.cellId1stCellCalling
        )
      case _ => None
    }.filter(_ != None)

    val fff = stc.filter{
      case SpaceTimeCall(_, _, week, _, _, _, cell) =>
        (cells.contains(cell) &&
         week >= since.getWeekOfWeekyear &&
         week <= until.getWeekOfWeekyear)
    }

    val zero = collection.mutable.Set[Any]()
    val profiling = fff.map{
      case stc@SpaceTimeCall(_,_,_,_,_,_,_) => (stc.key, stc.view)
    }.aggregateByKey(zero)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2
    ).map{ case (k, s) => (k, s.size) }
      .sortBy{ case (k, num) => (k._1, k._3, k._4, k._5)}
    profiling.saveAsTextFile(outputLocation + "/profiling")
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = (s"Usage: submit.sh ${appName} <master> <cdrLocation> " +
                 "<geoLocation> <outputLocation> " +
                 s"<baseSince (${Call.datePattern})> " +
                 s"<baseUntil (${Call.datePattern})>")

    if (args.length != 6) {
      System.err.println(usage)
      System.exit(1)
    }

    val master = args(0)
    val cdrLocation = args(1)
    val geoLocation = args(2)
    val outputLocation=args(3)
    val baseSince = Call.dateFormat.parseDateTime(args(4))
    val baseUntil = Call.dateFormat.parseDateTime(args(5))

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    val data = sc.textFile(cdrLocation)
    val geoData = sc.textFile(geoLocation)

    run(data, geoData, baseSince, baseUntil, outputLocation)
  }
}
