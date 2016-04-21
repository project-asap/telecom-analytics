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
package pd

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.FunSuite

class PeakDetectionSuite extends FunSuite {// with LocalSparkContext{
  test("same results") {
    val props = Settings(Array("local",
                         "",
                         "",
                         "20150603",
                         "20150604",
                         "512m",
                         "512m",
                         "2",
                         "false",
                         "false"))

    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.driver.memory", props.driverMem)
      .set("spark.executor.memory", props.executorMem)
      .set("spark.sql.hive.metastore.jars", "sbt")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/test/resources/sample-dataset")
    val spark_impl = PeakDetectionSpark
    val sql_impl = PeakDetectionDF

    val r1 = spark_impl.run(props, data, sc)
    val r2 = sql_impl.run(props, data, sc)
    assert(r1 == r2)
  }
}
