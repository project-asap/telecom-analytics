import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

class PeakDetection extends Serializable{
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
          outputLocation: String) {
    val cdr = calcCDR(data)
    cdr.saveAsTextFile(outputLocation + "/cdr")

    val dataRaw = calcDataRaws(cdr)
    dataRaw.saveAsTextFile(outputLocation + "/dataRaw")

    val voronoi = calcVoronoi(cdr)

    val cpBase = calcCpBase(dataRaw, voronoi, since, until)
    cpBase.saveAsTextFile(outputLocation + "/cpBase")

    val cpAnalyze = calcCpAnalyze(dataRaw, voronoi)
    cpAnalyze.saveAsTextFile(outputLocation + "/cpAnalyze")

    val events = calcEvents(cpBase, cpAnalyze)
    events.saveAsTextFile(outputLocation + "/events")

    val eventsFilter = calcEventsFilter(events)
    eventsFilter.saveAsTextFile(outputLocation + "/eventsFilter")
  }
}

object PeakDetectionSpark extends PeakDetection {
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
