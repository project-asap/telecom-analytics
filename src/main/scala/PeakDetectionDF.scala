package pd

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Try, Success, Failure}

object PeakDetectionDF extends PeakDetection{
  override def run(props:Settings, data: RDD[String], sc: SparkContext) = {
    val sqlContext = new HiveContext(sc)

    // Import Row.
    import org.apache.spark.sql.Row;

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

    val schemaString = Map(
      "id" -> StringType,
      "hour" -> IntegerType,
      "dow" -> IntegerType,
      "doy" -> IntegerType
    )

    val schema =
      StructType(
        schemaString.map{ case (fieldName, fieldType) => StructField(fieldName, fieldType, true)}.toArray
    )

    val tmp = calcCDR(data).map{ case CDR(id, hour, dow, doy) =>  Row(id, hour, dow, doy)}
    val cdr = sqlContext.createDataFrame(tmp, schema)
    cdr.registerTempTable("cdr")
    //cdr.write.saveAsTable(props.outputLocation + "cdr")

    val voronoi = sqlContext.sql(
      "SELECT DISTINCT(id) AS nidt FROM cdr LIMIT 10")
    voronoi.registerTempTable("voronoi")
    //voronoi.write.saveAsTable(props.outputLocation + "/voronoi")

    val data_raw = sqlContext.sql(
      "SELECT id, hour, dow, doy, COUNT(*) AS num FROM cdr GROUP BY id, hour, dow, doy")
    data_raw.registerTempTable("data_raw")
    //data_raw.write.saveAsTable(props.outputLocation + "/data_raw")

    val cp_base = sqlContext.sql(
      s"""SELECT rid,hour,dow, AVG(num) AS num FROM 
          (SELECT id AS rid, hour, dow, num FROM data_raw
            WHERE doy>=${props.baseSince.getDayOfYear} AND doy<=${props.baseUntil.getDayOfYear}) t
          JOIN voronoi ON t.rid = nidt
          GROUP BY rid, hour, dow""")
    cp_base.registerTempTable("cp_base")
    //cp_base.write.saveAsTable(props.outputLocation + "/cp_base")

    val cp_analyze = sqlContext.sql(
      "SELECT id AS rid, hour, doy, dow, num FROM data_raw JOIN voronoi ON id = nidt")
    cp_analyze.registerTempTable("cp_analyze")
    //cp_analyze.write.saveAsTable(props.outputLocation + "/cp_analyze")

    val events = sqlContext.sql(
      """SELECT  a.rid, a.hour, a.doy, a.dow, 
          ((CAST(a.num AS FLOAT)/am.num)/(CAST(b.num AS FLOAT)/bm.num)) - 1.0 AS ratio,
          a.num AS anum,
          b.num AS bnum
      FROM 
          cp_analyze a, 
          cp_base b, 
          (SELECT doy, dow, SUM(num) AS num FROM cp_analyze GROUP BY doy, dow) am,
          (SELECT dow, SUM(num) AS num FROM cp_base GROUP BY dow) bm
      WHERE 
      a.rid=b.rid AND 
      a.hour=b.hour AND 
      a.dow = b.dow AND
      b.dow = bm.dow AND
      a.doy = am.doy""")
    events.registerTempTable("events")
    //events.write.saveAsTable(props.outputLocation + "/events")

    val events_filter = sqlContext.sql(
      """SELECT rid, hour, doy, dow,
         FLOOR(ABS(ratio/0.1)) * 0.1 * SIGN(ratio) AS ratio
         FROM events
         WHERE abs(ratio) >= 0.2 AND
         ABS(anum-bnum) >= 50 and
         ratio>0""")
    events_filter.show
    events_filter.printSchema
    events_filter.map(r => (Key(r.getAs[String]("rid"),
                                r.getAs[Int]("hour"),
                                r.getAs[Int]("dow")),
                            r.getAs[Double]("ratio"))).collect

    //events_filter.write.saveAsTable(props.outputLocation + "/events_filter")
  }

  def main(args: Array[String]) {
    configure(args) match {
      case Success((props: Settings, data: RDD[_], sc)) =>
        run(props, data, sc)
      case Failure(e) =>
        System.err.println(this.usage)
        System.exit(1)
    }
  }
}
