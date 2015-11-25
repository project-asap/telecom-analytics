package pd

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
    val cdrPath: String,
    val output: String,
    val trainingSince: DateTime,
    val trainingUntil: DateTime,
    val testSince: DateTime,
    val testUntil: Option[DateTime],
    val voronoiPath: String,
    val binSize: Double
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
      parseDate(args(5)),
      Try(parseDate(args(6))).toOption,
      args(7),
      args(8).toDouble
    )
  }
}

class PeakDetection extends Serializable{
  val appName = this.getClass().getSimpleName
  val usage = (s"Usage: submit.sh ${appName} " +
               "<master> <cdrPath> <output> " +
               s"<trainingSince (${Call.datePattern})> " +
               s"<trainingUntil (${Call.datePattern})> " +
               s"<testSince (${Call.datePattern})> " +
               s"<testUntil (${Call.datePattern} or None)> " +
               "<voronoiPath> " +
              "<binSize> ")

  def configure(args: Array[String]): Try[(Settings, SparkContext, RDD[String], Array[String])] = {
    Try {
      val props = Settings(args)
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(props.master)
      val sc = new SparkContext(conf)

      val data = sc.textFile(props.cdrPath)
      val voronoi = sc.textFile(props.voronoiPath).collect

      (props, sc, data, voronoi)
    }
  }

  def parse(l: String, delim: String = ";") = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a).getOrElse(None)
  }

  def calcCalls(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).filter(_ != None)
  }

  def calcDataRaws(calls: RDD[_]) = {
    val zero = scala.collection.mutable.Set[String]()
    calls.map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        val dt = c.dateTime
        ((c.cellId1stCellCalling.substring(0, 5),
          dt.getHourOfDay,
          dt.getDayOfWeek,
          dt.getDayOfYear),
        c.callingPartyNumberKey)
     }.aggregateByKey(zero)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2
    ).map{ case ((cellId, hour, dow, doy), set) =>
      DataRaw(CDR(cellId, hour, dow, doy), set.size)
    }
  }

  def calcTrainingData(dataRaw: RDD[DataRaw], voronoi: Array[String],
                       since: DateTime, until: DateTime) = {
    dataRaw.filter{ case DataRaw(id, _ ,_ , doy, _) =>
        voronoi.contains(id) &&
        doy >= since.getDayOfYear &&
        doy <= until.getDayOfYear
    }
  }

  def calcCpBase(trainingData: RDD[DataRaw]) = {
    trainingData.map{
        case DataRaw(id, hour, dow, doy, num) =>
          ((id, hour, dow), (1, num))
      }.aggregateByKey((0, 0))(
        (a, b) => (a._1 + b._1, a._2 + b._2),
        (a, b) => (a._1 + b._1, a._2 + b._2)
      ).mapValues(x => x._2/x._1)
        .map{ case ((id, hour, dow), avg) => CpBase(id, hour, dow, avg) }
  }

  def calcCpAnalyze(dataRaw: RDD[DataRaw], voronoi: Array[String],
                    since: DateTime, until: Option[DateTime]) = {
    val cpAnalyze = dataRaw.filter( dr => voronoi.contains(dr.id) &&
      dr.doy >= since.getDayOfYear)
    if (until.isDefined)
      cpAnalyze.filter( dr => dr.doy <= until.get.getDayOfYear)
    cpAnalyze
  }

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

  def calcEventsFilter(events: RDD[Event], binSize: Double) = {
    events.filter{case Event(_, ratio, aNum, bNum) =>
      Math.abs(ratio) >= 0.2 && Math.abs(aNum - bNum) >= 50 && ratio > 0
    }.map{ case Event(k, ratio, _, _) =>
      (k, Math.floor(Math.abs(ratio / binSize)) * binSize * Math.signum(ratio))
    }
  }

  def run(props: Settings, sc: SparkContext, data: RDD[String],
          voronoi: Array[String]) = {
    val calls = calcCalls(data)
    calls.persist(StorageLevel.MEMORY_ONLY_SER)
    calls.saveAsTextFile(props.output + "/calls")

    val dataRaw = calcDataRaws(calls)
    dataRaw.persist(StorageLevel.MEMORY_ONLY_SER)
    dataRaw.saveAsTextFile(props.output + "/dataRaw")

    val trainingData = calcTrainingData(dataRaw, voronoi, props.trainingSince, props.trainingUntil)
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
    trainingData.saveAsTextFile(props.output + "/trainingData")

    val cpBase = calcCpBase(trainingData)
    cpBase.persist(StorageLevel.MEMORY_ONLY_SER)
    cpBase.saveAsTextFile(props.output + "/cpBase")

    val cpAnalyze = calcCpAnalyze(dataRaw, voronoi, props.testSince, props.testUntil)
    cpAnalyze.persist(StorageLevel.MEMORY_ONLY_SER)
    cpAnalyze.saveAsTextFile(props.output + "/cpAnalyze")

    val events = calcEvents(cpBase, cpAnalyze)
    events.persist(StorageLevel.MEMORY_ONLY_SER)
    events.saveAsTextFile(props.output + "/events")
    println(s"Found ${events.count} events.")

    val eventsFilter = calcEventsFilter(events, props.binSize)
    eventsFilter.saveAsTextFile(props.output + "/eventsFilter")
    println(s"Found ${eventsFilter.count} events after filtering.")
    eventsFilter.collect
  }
}

object PeakDetectionSpark extends PeakDetection {
  def main(args: Array[String]) {
    configure(args) match {
      case Success((props, sc, data, voronoi)) =>
        run(props, sc, data, voronoi)
      case Failure(e) =>
        System.err.println(this.usage)
        System.exit(1)
    }
  }
}
