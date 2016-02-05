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
    Broadcast[Set[String]])] = {
    Try {
      val props = DataFilterSettings(args)
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(props.master)
      val sc = new SparkContext(conf)

      val data = sc.textFile(props.cdrIn)
      val voronoi = sc.broadcast(sc.textFile(props.voronoiIn).map(_.trim).collect.toSet)

      (props, sc, data, voronoi)
    }
  }

  def parse(l: String, delim: String = " ; ") = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a).getOrElse(None)
  }

  def calcCalls(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).filter(_ != None)
  }

  def calcTrainingCalls(calls: RDD[_], voronoi: Broadcast[Set[String]],
                    since: DateTime, until: DateTime) = {
    calls.filter{
      case c@Call(_,_,_,_,_) =>
        val doy = c.dateTime.getDayOfYear
        val id = c.cellId1stCellCalling.substring(0, 5)
        (voronoi.value.contains(id) &&
         doy >= since.getDayOfYear &&
         doy <= until.getDayOfYear)
    }
  }

  def calcTestCalls(calls: RDD[_], voronoi: Broadcast[Set[String]],
                    since: DateTime, until: Option[DateTime]) = {
    val tmp = calls.filter{
      case c@Call(_,_,_,_,_) =>
        val doy = c.dateTime.getDayOfYear
        val id = c.cellId1stCellCalling.substring(0, 5)
        voronoi.value.contains(id) && doy >= since.getDayOfYear
    }
    if (until.isDefined) {
      tmp.filter{
        case c@Call(_,_,_,_,_) =>
          val doy = c.dateTime.getDayOfYear
          doy <= until.get.getDayOfYear
      }
    }
    tmp
  }

  def calcDataRaws(calls: RDD[_]) = {
    val zero = scala.collection.mutable.Set[String]()
    calls.map{
      case c@Call(_,_,_,_,_) =>
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

  def run(props: DataFilterSettings, sc: SparkContext, data: RDD[String],
          voronoi: Broadcast[Set[String]]) = {
    val calls = calcCalls(data).cache

    val trainingCalls = calcTrainingCalls(calls, voronoi, props.trainingSince, props.trainingUntil)

    val testCalls = calcTestCalls(calls, voronoi, props.testSince, props.testUntil)

    val trainingData = calcDataRaws(trainingCalls)
    trainingData.saveAsTextFile(props.trainingOut)

    val testData = calcDataRaws(testCalls)
    testData.saveAsTextFile(props.testOut)
    trainingData.collect
  }
}

object DataFilter extends DataFilter {
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
