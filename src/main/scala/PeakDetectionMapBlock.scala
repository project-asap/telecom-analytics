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
    configure(args) match {
      case Success((props: Settings, data: RDD[_])) =>
        run(data, props.baseSince, props.baseUntil, props.outputLocation, props.persist, props.saveIntermediate)
      case Failure(e) =>
        System.err.println(this.usage)
        System.exit(1)
    }
  }
}
