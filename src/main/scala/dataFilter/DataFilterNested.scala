package ta

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

object DataFilterNested extends DataFilter{
  override def filterCalls(calls: RDD[Call], voronoi: RDD[String],
    sc: SparkContext) = {
    calls.first  // hack
    calls.filter(c => {
      val id = c.cellId1stCellCalling.substring(0, 5)
      voronoi.map(v => id == v).collect.aggregate(false)(_||_, _||_)
    })
  }

  /*
  def main(args: Array[String]) {
    configure(args) match {
      case Success((props, sc, data, voronoi)) =>
        run(props, sc, data, voronoi, save = true)
      case Failure(e) =>
        throw new Exception(e)
        System.err.println(this.usage)
        System.exit(1)
    }
  }*/
}
