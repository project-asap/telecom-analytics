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
  val appName = this.getClass().getSimpleName
  val usage = s"Usage: submit.sh ${appName} <master> <cpBaseFile> <testDataFile> <binSize>"

  private def calcEvents(cpBaseStr: RDD[String], cpAnalyzeStr: RDD[String]) = {
    val cpBase = cpBaseStr.map(CpBase(_))
    val cpAnalyze = cpAnalyzeStr.map(DataRaw(_))

    val am = cpAnalyze.map( dr => ((dr.dow, dr.doy), dr.num) )
      .aggregateByKey(0)(_ + _, _ + _)
      .map { case ((dow, doy), sum) => (dow, doy, sum) }

    val bm = cpBase.map(cb => (cb.dow, cb.num))
      .aggregateByKey(0)(_ + _, _ + _)

    val cpAnalyzeJoinAm = cpAnalyze.map(dr => (dr.doy, dr))
      .join(am.map{ case (dow, doy, num) => (doy, num) })
      .map{ case (doy, (dr, sum)) =>
      (Key(dr.id, dr.hour, dr.dow), (dr.num.toDouble/sum, dr.num))}

    val cpBaseJoinBm = cpBase.map(cp =>
      (cp.dow, cp)
    ).join(bm).map { case (dow, (cp, sum)) =>
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

  def run(cpBase: RDD[String], testData: RDD[String], binSize: Double, output: String) = {
    val events = calcEvents(cpBase, testData)
    events.persist(StorageLevel.MEMORY_ONLY_SER)
    events.saveAsTextFile(s"${output}/events")

    val eventsFilter = calcEventsFilter(events, binSize)
    eventsFilter.persist(StorageLevel.MEMORY_ONLY_SER)
    events.saveAsTextFile(s"${output}/eventsFilter")
    eventsFilter
  }
}

object PeakDetection extends PeakDetection {
  def main(args: Array[String]) {
      val appName = this.getClass().getSimpleName

      args.toList match {
        case master :: cpBaseFile :: testDataFile :: output :: binSize :: Nil =>
          val conf = new SparkConf()
            .setAppName(appName)
            .setMaster(master)
          val sc = new SparkContext(conf)

          val cpBase = sc.textFile(cpBaseFile)
          val testData = sc.textFile(testDataFile)

          val eventsFilter = run(cpBase, testData, binSize.toDouble, output)
          println(s"Found ${eventsFilter.count} events after filtering.")
        case _ =>
          val usage = (s"Usage: submit.sh ${appName} <master> <cpBaseFile> <testDataFile> <output> <binSize (Int)>")
          System.err.println(usage)
          System.exit(1)
    }
  }
}
