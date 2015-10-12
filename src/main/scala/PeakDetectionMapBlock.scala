import java.util.Calendar
import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

object PeakDetectionMapBlock extends PeakDetectionNested{
  override def calcDataRaws(cdr: RDD[_]) = {
    val tmp = cdr.mapBlock( array1 => {
      cdr.mapBlock( array2 => {
        array1.map(e => {
          val num = array2.filter(e2 => {
            e == e2
          }).size
          (e, num)
        })
      }).collect()
    })
    tmp.distinct.reduceByKey(_+_).map{ case ((id:String, hour:Int, dow:Int, doy:Int), num: Int) =>
      new DataRaw(id, hour, dow, doy, num)
    }
  }

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
