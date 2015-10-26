import java.util.Calendar
import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

object PeakDetectionMapBlockLight extends PeakDetectionNested{
  override def calcDataRaws(cdr: RDD[_]) = {
    val tmp = cdr.mapBlock( (i, array1) => {
      val a1 = array1.groupBy(e => e).map{ case (k, v) => (k, v.size) }
      println(s"-->array1: ${array1.mkString("\n")}, a1: ${a1.mkString("\n")}}")
      cdr.mapBlock( (j, array2) => {
        val m = if (i < j) {
          val a2 = array2.groupBy(e => e).map{ case (k, v) => (k, v.size) }
          a1 ++ a2.map{ case (k, v) => k -> (v + a1.getOrElse(k, 0)) }
        } else Map()
        m.toArray
      }).collect
    })
    tmp.map{ case ((id:String, hour:Int, dow:Int, doy:Int), num: Int) =>
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
