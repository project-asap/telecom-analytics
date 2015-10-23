import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

class PeakDetectionNested extends PeakDetection{
  override def calcCDR(data: RDD[String]) = {
    data.filter(_.length != 0).map(parse(_)).map{
      case c@Call(_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) =>
        (c.cellId1stCellCalling,
         c.dateTime.getHourOfDay,
         c.dateTime.getDayOfWeek,
         c.dateTime.getDayOfYear)
      case _ => None
     }.filter(_ != None)
  }

  override def calcVoronoi(cdr: RDD[_]) =
    cdr.map{case (id: String, _, _, _) => id}.distinct.take(10)
}

object PeakDetectionNested extends PeakDetectionNested{
  override def calcDataRaws(cdr: RDD[_]) = {
    cdr.map{case (id: String, hour: Int, dow: Int, doy: Int) =>
      val tmp = cdr.filter{ case (id2: String, hour2: Int, dow2: Int, doy2: Int) => {
        (id == id2 &&
         hour == hour2 &&
         dow == dow2 &&
         doy == doy2)
      } }
      val num = tmp.collect.length
      new DataRaw(id, hour, dow, doy, num)
    }.distinct.cache
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
