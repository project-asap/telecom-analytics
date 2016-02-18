package ta

import scala.util.{Try, Success, Failure}

import java.text.SimpleDateFormat

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.joda.time.DateTime

import scala.util.Try

class DataFilter extends Serializable{
  val appName = this.getClass().getSimpleName
  val usage = s"Usage: submit.sh ${appName} ${DataFilterSettings.toString}"

  def configure(args: Array[String]): Try[(DataFilterSettings, SparkContext, RDD[String],
    RDD[String])] = {
    Try {
      val props = DataFilterSettings(args)
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(props.master)
      val sc = new SparkContext(conf)

      val data = sc.textFile(props.cdrIn)
      val voronoi = sc.textFile(props.voronoiIn).map(_.split(";", -1)(0).trim.substring(0, 5))

      (props, sc, data, voronoi)
    }
  }

  def parse(l: String, delim: String = " ; "): Try[Call] = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a)
  }

  def calcCalls(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).filter{
      case Success(c) => true
      case Failure(_) => false
    }.map(_.get)
  }

  def filterCalls(calls: RDD[Call], voronoi: RDD[String], sc: SparkContext) = {
    val vSet = sc.broadcast(voronoi.collect.toSet)
    calls.filter(c => {
        val id = c.cellId1stCellCalling.substring(0, 5)
        vSet.value.contains(id)
    })
  }

  def calcTrainingCalls(calls: RDD[Call], since: DateTime, until: DateTime) = {
    calls.filter(c => {
        val doy = c.dateTime.getDayOfYear
        val id = c.cellId1stCellCalling.substring(0, 5)
        (doy >= since.getDayOfYear &&
         doy <= until.getDayOfYear)
    })
  }

  def calcTestCalls(calls: RDD[Call], since: DateTime, until: Option[DateTime]) = {
    val tmp = calls.filter(c => {
        val doy = c.dateTime.getDayOfYear
        val id = c.cellId1stCellCalling.substring(0, 5)
        doy >= since.getDayOfYear
    })
    if (until.isDefined) {
      tmp.filter(c => {
          val doy = c.dateTime.getDayOfYear
          doy <= until.get.getDayOfYear
      })
    }
    tmp
  }

  def calcDataRaws(calls: RDD[Call]) = {
    val zero = scala.collection.mutable.Set[String]()
    calls.map(c => {
        val dt = c.dateTime
        ((c.cellId1stCellCalling.substring(0, 5),
          dt.getHourOfDay,
          dt.getDayOfWeek,
          dt.getDayOfYear),
        c.callingPartyNumberKey)
    }).aggregateByKey(zero)(
      (set, v) => set += v,
      (set1, set2) => set1 ++= set2
    ).map{ case ((cellId, hour, dow, doy), set) =>
      DataRaw(CDR(cellId, hour, dow, doy), set.size)
    }
  }

  def run(props: DataFilterSettings, sc: SparkContext, data: RDD[String],
          voronoi: RDD[String], save: Boolean = false) = {
    val calls = filterCalls(calcCalls(data), voronoi, sc)
    println(s"--->>>${calls.count}")

    val trainingCalls = calcTrainingCalls(calls, props.trainingSince, props.trainingUntil)

    val testCalls = calcTestCalls(calls, props.testSince, props.testUntil)

    val trainingData = calcDataRaws(trainingCalls)
    if ( save ) trainingData.saveAsTextFile(props.trainingOut)

    val testData = calcDataRaws(testCalls)
    if ( save )testData.saveAsTextFile(props.testOut)

    (trainingData, testData)
  }
}

object DataFilter extends DataFilter {
  def main(args: Array[String]) {
    configure(args) match {
      case Success((props, sc, data, voronoi)) =>
        run(props, sc, data, voronoi, save = true)
      case Failure(e) =>
        System.err.println(this.usage)
        System.exit(1)
    }
  }
}
