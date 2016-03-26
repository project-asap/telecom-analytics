package ta

import scala.collection.parallel._
import scala.collection.parallel.mutable._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd._
import org.apache.spark.rdd.HierRDD._

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import scala.util.{Try, Success, Failure}

class Cluster(id: Int, maxIterations: Int) extends Splittable[Vector] with Serializable{
  def id() = id
  var m : Try[KMeansModel] = null

  private def safeTraining(data: RDD[Vector]) = {
    Try (
      KMeans.train(data, k=2, maxIterations=this.maxIterations)
    )
  }

  def this(id:Int, maxIterations: Int, data:RDD[Vector]) {
    this(id, maxIterations)
    m = safeTraining(data)
  }

  def splitIndex(point: Vector) = m.get.predict(point)

  def contains(point: Vector) = (m.get.predict(point) == id)

  def split(level:Int, data:RDD[Vector]) : Array[Cluster] = {
    if (m eq null) m = safeTraining(data)
    m match {
      case Success(mdl) => mdl.clusterCenters.zipWithIndex.map{ case (c, idx) =>
          new Cluster(idx, this.maxIterations, data)
      }.toArray
      case Failure(f) =>
        Array(this)
    }
  }

  def splitPar(level:Int, data:RDD[Vector]) : ParArray[Cluster] = {
    split(level, data).par
  }
}

object ClusteringHierRDD extends Clustering {
  override def divise(data: RDD[Vector], clusterNum: Int, subIterations: Int) = {
    val initCluster = new Cluster(0, subIterations)
    val hierRDD = data.hierarchical2(initCluster, false)
    var split = hierRDD.splitPar

    val depth = math.ceil(math.log10(clusterNum) / math.log10(2)).toInt
    for (_ <- 1 to depth) {
      split = split.flatMap(_.splitPar)
    }
    // keep only the leaf centers
    split.map(_.s.asInstanceOf[Cluster].m.get.clusterCenters).flatten
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val master = Try{args(0)}.getOrElse("spark://localhost:7077")
    val region = args(1)
    val timeframe = args(2)
    val clusterNum = Try{args(3).toInt}.getOrElse(100)
    val subIterations = Try{args(4).toInt}.getOrElse(5)
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    val p1 = """^\(u'(\w+)', \[(.*)\]\)$""".r
    val dir = "/profiles-${region}-${timeframe}"
    val data = sc.textFile(dir).map{ case p1(id, rest) =>
      toCarretto( Profile.pattern.findAllIn(rest).toArray.map(Profile(_)) )
    }.cache

    val tipiCentroidi = run(data, clusterNum, subIterations)

    sc.parallelize(tipiCentroidi.toList.toSeq).saveAsTextFile(s"/centroids-${region}-${timeframe}")
    sc.stop
  }
}
