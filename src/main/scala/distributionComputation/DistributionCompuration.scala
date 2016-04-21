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

import scala.util.{Try, Success, Failure}

import java.text.SimpleDateFormat

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.joda.time.DateTime

import scala.util.Try

object DistributionComputation extends Serializable {
  private def calcCpBase(trainingData: RDD[String]) = {
    trainingData.map(DataRaw(_)).map(dr =>
          ((dr.id, dr.hour, dr.dow), (1, dr.num))
      ).aggregateByKey((0, 0))(
        (a, b) => (a._1 + b._1, a._2 + b._2),
        (a, b) => (a._1 + b._1, a._2 + b._2)
      ).mapValues(x => x._2/x._1)
        .map{ case ((id, hour, dow), avg) => CpBase(id, hour, dow, avg) }
  }

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = s"Usage: submit.sh ${appName} <master> <trainingDataFile> <output>"

    args.toList match {
      case master :: trainingDataFile :: output :: Nil =>
        val conf = new SparkConf()
          .setAppName(appName)
          .setMaster(master)
        val sc = new SparkContext(conf)

        val trainingData = sc.textFile(trainingDataFile)

        val cpBase = calcCpBase(trainingData)
        cpBase.persist(StorageLevel.MEMORY_ONLY_SER)
        println(s"output: ${output}, count: ${cpBase.count}")
        cpBase.saveAsTextFile(output + "/cpBase")
      case _ => 
        System.err.println(usage)
        System.exit(1)
    }
  }
}
