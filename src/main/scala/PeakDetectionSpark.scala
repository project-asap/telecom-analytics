import scala.util.{Try, Success, Failure}

import java.text.SimpleDateFormat

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.joda.time.DateTime

import scala.util.Try

case class Settings(
    val master: String,
    val inputFile: String,
    val outputLocation: String,
    val baseSince: DateTime,
    val baseUntil: DateTime,
    val driverMem: String,
    val executorMem: String,
    val partitions: Int,
    val persist: Boolean,
    val saveIntermediate: Boolean
)
object Settings {
  private def parseDate = s => Call.dateFormat.parseDateTime(s)

  def apply(args: Array[String]) = {
    new Settings(
      args(0),
      args(1),
      args(2),
      parseDate(args(3)),
      parseDate(args(4)),
      args(5),
      args(6),
      args(7).toInt,
      Try(args(8).toBoolean).getOrElse(false),
      Try(args(9).toBoolean).getOrElse(false)
    )
  }
}

class PeakDetection extends Serializable{
  val appName = this.getClass().getSimpleName
  val usage = (s"Usage: submit.sh ${appName} " +
               "<master> <cdrLocation> <outputLocation> " +
               s"<baseSince (${Call.datePattern})> " +
               s"<baseUntil (${Call.datePattern})>")

  def configure(args: Array[String]): Try[(Settings, RDD[String])] = {
    Try {
      val props = Settings(args)
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(props.master)
        .set("spark.driver.memory", props.driverMem)
        .set("spark.executor.memory", props.executorMem)
      val sc = new SparkContext(conf)
      val data = sc.textFile(props.inputFile)
      (props, data)
    }
  }

  def parse(l: String, delim: String = ";") = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a).getOrElse(None)
  }

  def calcCDR(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        CDR(
          c.cellId1stCellCalling,
          c.dateTime.getHourOfDay,
          c.dateTime.getDayOfWeek,
          c.dateTime.getDayOfYear
        )
      case _ => None
     }.filter(_ != None)
  }

  def calcDataRaws(cdr: RDD[_]) = {
    cdr.map{ case cdr@CDR(_,_,_,_) => (cdr, 1) }
      .aggregateByKey(0)(_ + _, _ + _)
      .map { case(cdr, num) => DataRaw(cdr, num) }
  }

  def calcVoronoi(cdr: RDD[_]) =
    cdr.map{ case CDR(id, _, _, _) => id }.distinct.take(10)

  def calcCpBase(dataRaw: RDD[DataRaw], voronoi: Array[String],
                 since: DateTime, until: DateTime) = {
    dataRaw
      .filter{ case DataRaw(id, _ ,_ , doy, _) =>
        voronoi.contains(id) &&
        doy >= since.getDayOfYear &&
        doy <= until.getDayOfYear
      }.map{
        case DataRaw(id, hour, dow, doy, num) =>
          ((id, hour, dow), (1, num))
      }.aggregateByKey((0, 0))(
        (a, b) => (a._1 + b._1, a._2 + b._2),
        (a, b) => (a._1 + b._1, a._2 + b._2)
      ).mapValues(x => x._2/x._1)
        .map{ case ((id, hour, dow), avg) => CpBase(id, hour, dow, avg) }
  }

  def calcCpAnalyze(dataRaw: RDD[DataRaw], voronoi: Array[String]) =
    dataRaw.filter( dr => voronoi.contains(dr.id) )

  def calcEvents(cpBase: RDD[CpBase], cpAnalyze: RDD[DataRaw]) = {
    val am = cpAnalyze.map( dr => ((dr.dow, dr.doy), dr.num) )
      .aggregateByKey(0)(_ + _, _ + _)
      .map { case ((dow, doy), sum) => (dow, doy, sum) }

    val bm = cpBase.map( cp => (cp.dow, cp.num))
      .aggregateByKey(0)(_ + _, _ + _)

    val cpAnalyzeJoinAm = cpAnalyze.map{ case dr@DataRaw(_, _, _, _, _) =>
      (dr.doy, dr)
    }.join(am.map{ case (dow, doy, num) =>
      (doy, num)
    }).map{ case (doy, (dr, sum)) =>
      (Key(dr.id, dr.hour, dr.dow), (dr.num.toDouble/sum, dr.num))}

    val cpBaseJoinBm = cpBase.map{ case cp@CpBase(_, _, _, _) =>
      (cp.dow, cp)
    }.join(bm).map { case (dow, (cp, sum)) =>
      (Key(cp.id, cp.hour, cp.dow), (cp.num.toDouble/sum, cp.num))}

    cpAnalyzeJoinAm.join(cpBaseJoinBm).map {
      case (k, ((amNum, aNum), (bmNum, bNum))) =>
        Event(k, amNum/bmNum - 1, aNum, bNum)
    }
  }

  def calcEventsFilter(events: RDD[Event]) = {
    events.filter{case Event(_, ratio, aNum, bNum) =>
      Math.abs(ratio) >= 0.2 && Math.abs(aNum - bNum) >= 50 && ratio > 0
    }.map{ case Event(k, ratio, _, _) =>
      (k, Math.floor(Math.abs(ratio / 0.1)) * 0.1 * Math.signum(ratio))
    }
  }

  def run(data: RDD[String], since: DateTime, until: DateTime,
          outputLocation: String, persist: Boolean = false,
          saveIntermediate: Boolean = false) {
    val cdr = calcCDR(data)
    if (persist) cdr.persist(StorageLevel.MEMORY_ONLY_SER)
    if (saveIntermediate) cdr.saveAsTextFile(outputLocation + "/cdr")

    val dataRaw = calcDataRaws(cdr)
    if (persist) dataRaw.persist(StorageLevel.MEMORY_ONLY_SER)
    if (saveIntermediate) dataRaw.saveAsTextFile(outputLocation + "/dataRaw")

    val voronoi = calcVoronoi(cdr)
    //voronoi.saveAsTextFile(outputLocation + "/cpBase")

    val cpBase = calcCpBase(dataRaw, voronoi, since, until)
    if (persist) cpBase.persist(StorageLevel.MEMORY_ONLY_SER)
    if (saveIntermediate) cpBase.saveAsTextFile(outputLocation + "/cpBase")

    val cpAnalyze = calcCpAnalyze(dataRaw, voronoi)
    if (persist) cpAnalyze.persist(StorageLevel.MEMORY_ONLY_SER)
    if (saveIntermediate) cpAnalyze.saveAsTextFile(outputLocation + "/cpAnalyze")

    val events = calcEvents(cpBase, cpAnalyze)
    if (persist) events.persist(StorageLevel.MEMORY_ONLY_SER)
    if (saveIntermediate) events.saveAsTextFile(outputLocation + "/events")
    println(s"Found ${events.count} events.")
    //events.foreach(println(_))

    val eventsFilter = calcEventsFilter(events)
    if (saveIntermediate) eventsFilter.saveAsTextFile(outputLocation + "/eventsFilter")
    println(s"Found ${eventsFilter.count} events after filtering.")
    //eventsFilter.foreach(println(_))
  }
}

object PeakDetectionSpark extends PeakDetection {
  def main(args: Array[String]) {
    configure(args) match {
      case Success((props: Settings, data: RDD[_])) =>
        run(data, props.baseSince, props.baseUntil, props.outputLocation, props.persist, props.saveIntermediate)
      case Failure(e) =>
        System.err.println(this.usage)
        System.exit(1)
    }
  }
}
