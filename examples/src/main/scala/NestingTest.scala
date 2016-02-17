package examples

import scala.util.{Try, Success, Failure}

import org.apache.spark.{SparkConf, SparkContext}

import org.joda.time.DateTime

import ta._

object NestingTest {
  def parse(l: String, delim: String = " ; "): Try[Call] = {
    val a = l.split(delim, -1).map(_.trim)
    Call(a)
  }

  def main(args: Array[String]) {
    val master = Try{args(0)}.getOrElse("spark://localhost:7077")
    val conf = new SparkConf().setAppName("NestingTest")
      .setMaster(master)
    val sc = new SparkContext(conf)
    val cdrIn = "/Users/butters/src/asap-project/telecom-analytics/src/test/resources/cdr-small.csv"
    //val cdrIn = "/Users/butters/src/asap-project/telecom-analytics/src/test/resources/cdr.csv"
    val voronoiIn = "/Users/butters/src/asap-project/telecom-analytics/src/test/resources/voronoi"

    val cdrData = sc.textFile(cdrIn).filter(_.length != 0).map(parse(_)).filter{
      case Success(c) => true
      case Failure(_) => false
    }.map(_.get)
    cdrData.first  // hack
    val voronoi = sc.textFile(voronoiIn).map(_.trim)

    val nst = cdrData.filter(c => {
        val id = c.cellId1stCellCalling.substring(0, 5)
        voronoi.map(v => id == v).collect.aggregate(false)(_||_, _||_)
    })
    nst.saveAsTextFile("/tmp/dataRaws")
  }
}
