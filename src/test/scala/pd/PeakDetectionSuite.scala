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
