package examples

import java.io.File

import scala.util.{Random, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast

import org.joda.time.DateTime

import ta._

object PeakDetectionNestedEx {
  def main(args: Array[String]) {
    val master = Try{args(0)}.getOrElse("spark://localhost:7077")
    val conf = new SparkConf().setAppName("PeakDetectionEx")
      .setMaster(master)
    val sc = new SparkContext(conf)

    val currentDir = new File(".").getCanonicalPath()
    val cdrIn = s"${currentDir}/src/test/resources/cdr-bigger.csv"
    //val cdrIn = s"${currentDir}/src/test/resources/cdr-small.csv"
    val voronoiIn = s"${currentDir}/src/test/resources/centro_roma.csv"
    val randomOut = Random.alphanumeric.take(10).mkString
    val trainingOut = s"${currentDir}/src/test/resources/${randomOut}/trainingData"
    val testOut = s"${currentDir}/src/test/resources/${randomOut}/testData"
    val cpBaseOut = s"${currentDir}/src/test/resources/${randomOut}/cpBase"
    val eventsOut = s"${currentDir}/src/test/resources/${randomOut}/events"
    val eventsFilterOut = s"${currentDir}/src/test/resources/${randomOut}/eventsFilter"

    val props = ta.DataFilterSettings(
      Array("local", cdrIn, voronoiIn, trainingOut, testOut, "2015-06-10",
        "2015-06-17", "2015-06-18", "None"))

    val cdrData = sc.textFile(cdrIn, 150)
    //val cdrData = sc.textFile(cdrIn)

    // should be absolute path to be accessible by the workers
    val voronoi = sc.textFile(voronoiIn).map(_.split(";", -1)(0).trim.substring(0, 5))

    val (trainingData, testData) = ta.DataFilterNested.run(props, sc, cdrData, voronoi)

    val cpBaseData = ta.DistributionComputation.run(trainingData, cpBaseOut)

    val (_, eventsFilter) = ta.PeakDetection.run(cpBaseData, testData, 0.1,
      eventsOut, eventsFilterOut, save=true)
    println(s"Results are dumped in: src/test/resources/${randomOut}")
    sc.stop()
  }
}
