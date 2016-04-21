/*

Copyright 2015-2016 FORTH

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

*/
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

class Cluster(id: Int, maxIterations: Int) extends Splittable[Vector] with Serializable {
  def id() = id
  var m : Try[KMeansModel] = null

  def this(id:Int, maxIterations: Int, data:RDD[Vector]) {
    this(id, maxIterations)
    m = Try ( KMeans.train(data, k = 2, maxIterations=this.maxIterations) )
  }

  def splitIndex(point: Vector) = m match {
    case Success(mdl) => mdl.predict(point)
    case Failure(f) => 0
  }

  def contains(point: Vector) = m match {
    case Success(mdl) => mdl.predict(point) == id
    case Failure(f) => false
  }

  def split(level:Int, data:RDD[Vector]) : Array[Cluster] = {
    if (m eq null) m = Try ( KMeans.train(data, k=2, maxIterations=this.maxIterations) )
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

object ClusteringHierRDD extends Clustering {//with Logging {
  override def divise(data: RDD[Vector], clusterNum: Int, subIterations: Int) = {
    val initCluster = new Cluster(0, subIterations)
    val hierRDD = data.hierarchical2(initCluster, false)
    var split = hierRDD.splitPar
    var clusters = Array[Vector]().par

    val depth = math.ceil(math.log10(clusterNum) / math.log10(2)).toInt
    for (i <- 1 to depth - 2) { // already split twice
      split = split.flatMap(_.splitPar)
    }
    clusters = split.map(c =>
        Try(c.s.asInstanceOf[Cluster].m.get.clusterCenters).getOrElse(Array[Vector]())).flatten
    clusters.toSet
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
    //val dir = "/profiles-${region}-${timeframe}"
    val dir = "profiles-text"
    val data = sc.textFile(dir).map{ case p1(id, rest) =>
      toCarretto( Profile.pattern.findAllIn(rest).toArray.map(Profile(_)) )
    }.cache

    val tipiCentroidi = run(data, clusterNum, subIterations)
    println(s"#centroids: ${tipiCentroidi.size}")

    sc.parallelize(tipiCentroidi.toList.toSeq).saveAsTextFile(s"/centroids-${region}-${timeframe}")
    sc.stop
  }
}
