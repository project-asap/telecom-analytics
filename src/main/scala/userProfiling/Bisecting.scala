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

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}
import org.apache.spark.util

class Clustering {
  val archetipi = """0;resident;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0
  1;resident;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5
  2;resident; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1
  3;dynamic_resident;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0
  4;dynamic_resident;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5
  5;dynamic_resident;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1
  5;commuter;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0
  6;commuter;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0
  7;commuter; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0
  8;visitor;1.0;1.0;1.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  9;visitor;0.5;0.5;0.5;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  10;visitor; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  11;resident;1.0;1.0;1.0;1.0;1.0;1.0;0.5;0.5;0.5;0.5;0.5;0.5; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0
  12;resident;0.5;0.5;0.5;0.5;0.5;0.5; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  13;visitor;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  14;visitor;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
  15;visitor;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0"""

  def parseArchetipi(s: String = archetipi, vLen: Int = 18) = {
    archetipi.split("\n", -1).map(l => {
      val w = l.split(";", -1).map(_.trim)
      Vectors.dense(w.slice(2, vLen + 2).map(_.toDouble)) -> w(1)
    }).toMap
  }

  /** split into two clusters using kmeans **/
  def split(cluster: RDD[Vector], subIterations: Int) = {
    Try(
      KMeans.train(cluster, k = 2, maxIterations = subIterations)
    )
  }

  def divise(data: RDD[Vector], clusterNum: Int, subIterations: Int) = {
    var clusters = Seq((Vectors.dense(Array[Double]()), data)).par

    val depth = math.ceil(math.log10(clusterNum) / math.log10(2)).toInt
    for (_ <- 1 to depth) {
      clusters = clusters.flatMap{ case (c, d) => {
        split(d, subIterations) match {
          case Success(model) =>
            model.clusterCenters.zipWithIndex.map{
              case(cntr, idx) => (cntr, (d.filter(x => model.predict(x) == idx)))
            }.par
          case Failure(f) =>
            Seq((c, d)).par
        }
      }}
    }
    // keep only the centers
    clusters.map(_._1).toSet
  }

  def run(data: RDD[Vector], clusterNum: Int, subIterations: Int) = {
    val centers = divise(data, clusterNum, subIterations)

    val archetipiMap = parseArchetipi(vLen = data.first.size)
    val tipiCentroidi = centers.map( ctr => {
      val min_ = archetipiMap.keys.reduceLeft( (a, b) => if (euclidean(ctr, a) < euclidean(ctr, b)) a else b)
      (archetipiMap(min_), ctr)
    })
    tipiCentroidi
  }

  def euclidean(v1: Vector, v2: Vector) = {
    val uv1 = util.Vector(v1.toArray)
    val uv2 = util.Vector(v2.toArray)
    math.sqrt(uv1.squaredDist(uv2))
  }

  def toCarretto(profilo: Array[Profile]) = {
    val carretto = scala.collection.mutable.ArrayBuffer.empty[Double]
    // consider first four weeks
    // resulted Vector size should conform with the archetipi
    for (w <- 1 to 4) for (isWeekend <- 0 to 1) for (t <- 0 to 2) {
      profilo.find(p => p.week == w && (p.isWeekend == isWeekend && p.timesplit == t)) match {
        case Some(p) => carretto += p.asInstanceOf[Profile].count
        case _ => carretto += 0.0
      }
    }
    Vectors.dense(carretto.toArray)
  }
}

object Clustering extends Clustering {
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
