import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

object PeakDetection {
  def parse(l: String, delim: String = ";") = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a).getOrElse(None)
  }

  def run(data: RDD[String], voronoi: Array[String], since: DateTime,
          until: DateTime, outputLocation: String) {
    val cdr = data.filter(_.length != 0).map(parse(_)).map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        CDR(
          c.cellId1stCellCalling,
          c.dateTime.getHourOfDay,
          c.dateTime.getDayOfWeek,
          c.dateTime.getDayOfYear
        )
      case _ => None
     }.filter(_ != None)
    cdr.saveAsTextFile(outputLocation + "/cdr")

    val dataRaw = cdr.map{
      case cdr@CDR(_,_,_,_) =>
        (cdr, 1)
    }.aggregateByKey(0)(_ + _, _ + _)
      .map { case(cdr, num) => DataRaw(cdr, num)
    }
    dataRaw.saveAsTextFile(outputLocation + "/dataRaw")

    val cpBase = dataRaw
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
    cpBase.saveAsTextFile(outputLocation + "/cpBase")

    val cpAnalyze = dataRaw.filter( dr => voronoi.contains(dr.id) )
    cpAnalyze.saveAsTextFile(outputLocation + "/cpAnalyze")

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
    cpAnalyzeJoinAm.saveAsTextFile(outputLocation + "/cpAnalyzeJoinAm")

    val cpBaseJoinBm = cpBase.map{ case cp@CpBase(_, _, _, _) =>
      (cp.dow, cp)
    }.join(bm).map { case (dow, (cp, sum)) =>
      (Key(cp.id, cp.hour, cp.dow), (cp.num.toDouble/sum, cp.num))}
    cpBaseJoinBm.saveAsTextFile(outputLocation + "/cpBaseJoinBm")

    val events = cpAnalyzeJoinAm.join(cpBaseJoinBm).map {
      case (k, ((amNum, aNum), (bmNum, bNum))) => (k, amNum/bmNum - 1, aNum, bNum)
    }
    events.saveAsTextFile(outputLocation + "/events")

    val eventsFilter = events.filter{case (k, ratio, aNum, bNum) =>
      Math.abs(ratio) >= 0.2 && Math.abs(aNum - bNum) >= 50 && ratio > 0
    }.map{ case (k, ratio, anum, bnum) =>
      (k, Math.floor(Math.abs(ratio / 0.1)) * 0.1 * Math.signum(ratio))
    }
    eventsFilter.saveAsTextFile(outputLocation + "/eventsFilter")
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
