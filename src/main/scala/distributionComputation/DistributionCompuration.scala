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


object DistributionComputation extends Serializable {
  private def calcCpBase(trainingData: RDD[DataRaw]) = {
    trainingData.map(dr =>
          ((dr.id, dr.hour, dr.dow), (1, dr.num))
      ).aggregateByKey((0, 0))(
        (a, b) => (a._1 + b._1, a._2 + b._2),
        (a, b) => (a._1 + b._1, a._2 + b._2)
      ).mapValues(x => x._2/x._1)
        .map{ case ((id, hour, dow), avg) => CpBase(id, hour, dow, avg) }
  }

  def run(trainingData: RDD[DataRaw], cpBaseOut: String, save: Boolean = false) = {
      val cpBase = calcCpBase(trainingData)
      cpBase.persist(StorageLevel.MEMORY_ONLY_SER)
      if ( save ) cpBase.saveAsTextFile(cpBaseOut)

      cpBase
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = s"Usage: submit.sh ${appName} <master> <trainingIn> <cpBaseOut>"

    args.toList match {
      case master :: trainingIn :: cpBaseOut :: Nil =>
        val conf = new SparkConf()
          .setAppName(appName)
          .setMaster(master)
        val sc = new SparkContext(conf)

        val trainingData = sc.textFile(trainingIn).map(DataRaw(_))

        run(trainingData, cpBaseOut, save = true)
      case _ => 
        System.err.println(usage)
        System.exit(1)
    }
  }
}
