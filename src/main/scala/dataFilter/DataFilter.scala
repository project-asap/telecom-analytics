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

      val data = sc.textFile(props.cdrPath)
      val voronoi = sc.broadcast(sc.textFile(props.voronoiPath).map(_.trim).collect.toSet)

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

  def calcTrainingData(dataRaw: RDD[DataRaw], voronoi: Broadcast[Set[String]],
                       since: DateTime, until: DateTime) = {
    dataRaw.filter{ case DataRaw(id, _ ,_ , doy, _) =>
      voronoi.value.contains(id) &&
      doy >= since.getDayOfYear &&
      doy <= until.getDayOfYear
    }
  }

  def calcTestData(dataRaw: RDD[DataRaw], voronoi: Broadcast[Set[String]],
                       since: DateTime, until: Option[DateTime]) = {
    val tmp = dataRaw.filter{ case DataRaw(id, _ ,_ , doy, _) =>
      voronoi.value.contains(id) && doy >= since.getDayOfYear
    }
    if (until.isDefined)
      tmp.filter{ case DataRaw(_, _ ,_ , doy, _) =>
        doy <= until.get.getDayOfYear
    }
    tmp
  }

  def run(props: DataFilterSettings, sc: SparkContext, data: RDD[String],
          voronoi: Broadcast[Set[String]]) = {
    val calls = calcCalls(data)
    calls.persist(StorageLevel.MEMORY_ONLY_SER)
    calls.saveAsTextFile(props.output + "/calls")

    val dataRaw = calcDataRaws(calls)
    dataRaw.persist(StorageLevel.MEMORY_ONLY_SER)
    dataRaw.saveAsTextFile(props.output + "/dataRaw")

    val trainingData = calcTrainingData(dataRaw, voronoi, props.trainingSince, props.trainingUntil)
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
    trainingData.saveAsTextFile(props.output + "/trainingData")

    val testData = calcTestData(dataRaw, voronoi, props.testSince, props.testUntil)
    testData.persist(StorageLevel.MEMORY_ONLY_SER)
    testData.saveAsTextFile(props.output + "/testData")
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
