package ta

import scala.util.{Try, Success, Failure}

import java.text.SimpleDateFormat

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.joda.time.DateTime

import scala.util.Try

class PeakDetection extends Serializable{
  private def calcEvents(cpBase: RDD[CpBase], cpAnalyze: RDD[DataRaw]) = {
    val am = cpAnalyze.map( dr => ((dr.dow, dr.doy), dr.num) )
      .aggregateByKey(0)(_ + _, _ + _)
      .map { case ((dow, doy), sum) => (dow, doy, sum) }

    val bm = cpBase.map(cb => (cb.dow, cb.num))
      .aggregateByKey(0)(_ + _, _ + _)

    val cpAnalyzeJoinAm = cpAnalyze.map(dr => (dr.doy, dr))
      .join(am.map{ case (dow, doy, num) => (doy, num) })
      .map{ case (doy, (dr, sum)) =>
      (Key(dr.id, dr.hour, dr.dow), (doy, dr.num.toDouble/sum, dr.num))}

    val cpBaseJoinBm = cpBase.map(cp =>
      (cp.dow, cp)
    ).join(bm).map { case (dow, (cp, sum)) =>
      (Key(cp.id, cp.hour, cp.dow), (cp.num.toDouble/sum, cp.num))}

    cpAnalyzeJoinAm.join(cpBaseJoinBm).map {
      case (k, ((doy, amNum, aNum), (bmNum, bNum))) =>
        Event(k.id, k.hour, doy, k.dow, amNum/bmNum - 1, aNum, bNum)
    }
  }

  def calcEventsFilter(events: RDD[Event], binSize: Double) = {
    events.filter{case e@Event(_,_,_,_,_,_,_) =>
//      Math.abs(e.ratio) >= 0.2 && Math.abs(e.aNum - e.bNum) >= 50 && e.ratio > 0
      Math.abs(e.ratio) >= 0.2 && Math.abs(e.aNum - e.bNum) >= 30 && e.ratio > 0
    }.map{ case e@ Event(_,_,_,_,_,_,_) =>
      (e.id, e.hour, e.doy, e.dow, Math.floor(Math.abs(e.ratio / binSize)) * binSize * Math.signum(e.ratio))
    }
  }

  def run(cpBase: RDD[CpBase], testData: RDD[DataRaw], binSize: Double,
    eventsOut: String, eventsFilterOut: String, save: Boolean = false) = {
    val events = calcEvents(cpBase, testData)
    events.persist(StorageLevel.MEMORY_ONLY_SER)
    if ( save ) events.saveAsTextFile(eventsOut)

    val eventsFilter = calcEventsFilter(events, binSize)
    eventsFilter.persist(StorageLevel.MEMORY_ONLY_SER)
    if ( save ) eventsFilter.saveAsTextFile(eventsFilterOut)

    (events, eventsFilter)
  }
}

object PeakDetection extends PeakDetection {
  def main(args: Array[String]) {
      val appName = this.getClass().getSimpleName

      args.toList match {
        case master :: cpBaseIn :: testIn :: eventsOut :: eventsFilterOut :: binSize :: Nil =>
          val conf = new SparkConf()
            .setAppName(appName)
            .setMaster(master)
          val sc = new SparkContext(conf)

          val cpBase = sc.textFile(cpBaseIn).map(CpBase(_))
          val testData = sc.textFile(testIn).map(DataRaw(_))

          val (events, eventsFilter) = run(cpBase, testData, binSize.toDouble, eventsOut, eventsFilterOut, save = true)
          println(s"Found ${eventsFilter.count} events after filtering.")
        case _ =>
          val usage = (s"Usage: submit.sh ${appName} <master> <cpBaseIn> <testIn> <eventsOut> <binSize (Int)>")
          System.err.println(usage)
          System.exit(1)
    }
  }
}
