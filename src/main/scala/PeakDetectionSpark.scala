package asap

import java.text.SimpleDateFormat
import java.lang.Math

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD

import org.joda.time.format.DateTimeFormat

object PeakDetectionSpark {
  val date_pattern = "yyyyMMdd"
  val time_pattern = "HHmmss"
  val date_format = DateTimeFormat.forPattern(date_pattern)
  val datetime_format = DateTimeFormat.forPattern(date_pattern + ":" + time_pattern)

  def main(args: Array[String]) {
    val appName = this.getClass().getSimpleName
    val usage = (s"Usage: submit.sh ${appName} <master> <cdr_location> <output_location> <base_since (${date_pattern})> <base_until (${date_pattern})> <max_cores> <driver_mem> <executor_mem>>")

    if (args.length != 8) {
      System.err.println(usage)
      System.exit(1)
    }

    val master = args(0)
    val data=args(1)
    val output_location=args(2)
    val base_since = date_format.parseDateTime(args(3))
    val base_until = date_format.parseDateTime(args(4))
    val maxCores = args(5)
    val driver_mem = args(6)
    val executor_mem = args(7)

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.cores.max", maxCores)
      .set("spark.driver.memory", driver_mem)
      .set("spark.executor.memory", executor_mem)
    val sc = new SparkContext(conf)

    val cdr = sc.textFile(data).filter(_.length != 0).map(_.split(";", -1).map(_.trim)).map{
      case Array(
            cdr_type,
            calling_party_number_key,
            calling_subscriber_imsi,
            call_date,
            time_start_charge,
            chargeable_duration,
            exchange_identity,
            outgoing_route,
            incoming_route,
            cell_id_1st_cell_calling,
            gsm_tele_service_code,
            cell_id_last_cell_calling,
            disconnecting_party,
            calling_subscriber_imei,
            tac,
            resident_customer_flag,
            payment_type,
            contract_status,
            contract_starting_date,
            country,
            city_postal_code,
            city) => {
            val date_time = call_date + ":" + time_start_charge
            Try(datetime_format.parseDateTime(date_time)).toOption match {
                case Some(parsed) => {
                    (cell_id_1st_cell_calling,
                     parsed.getHourOfDay,
                     parsed.getDayOfWeek,
                     parsed.getDayOfYear)
                }
                case None => None 
            }
         }
     }.filter(_ != None)
     cdr.saveAsTextFile(output_location + "/cdr")

    val data_raw = cdr.map{case (id:String, hour:Int, dow:Int, doy:Int) => ((id, hour, dow, doy), 1)}.reduceByKey(_+_).
        map{ case ((id, hour, dow, doy), num) => (id, hour, dow, doy, num)}
    data_raw.saveAsTextFile(output_location + "/data_raw")

    val voronoi = cdr.map{ case (id, hour, dow, doy) => id }.distinct.take(10)
    sc.parallelize(voronoi, 1).saveAsTextFile(output_location + "/voronoi")

    val cp_base = data_raw
      .filter(x =>
        voronoi.contains(x._1) &&
        x._4 >= base_since.getDayOfYear && x._4 <= base_until.getDayOfYear)
          .map{
          case (id, hour, dow, doy, num) => 
          ((id, hour, dow), (1, num))
          }.reduceByKey( (a, b) => (a._1 + b._1, a._2 + b._2) ).
            mapValues(x => x._2/x._1).
              map{ case (k, v) => (k._1, k._2, k._3, v) }
    cp_base.saveAsTextFile(output_location + "/cp_base")

    val cp_analyze = data_raw.filter(x => voronoi.contains(x._1))
    cp_analyze.saveAsTextFile(output_location + "/cp_analyze")

    val am = cp_analyze.map {
      case (rid, hour, dow, doy, num) => ((dow, doy), num)
    }.reduceByKey(_+_).map { case (k, v) => (k._1, k._2, v) }

    val bm = cp_base.map {
      case (id, hour, dow, num) => (dow, num)
    }.reduceByKey(_+_)

    val cp_analyze_join_am = cp_analyze.map{ case (id, hour, dow, doy, num) =>
      (doy, (id, hour, dow, doy, num))
      }.join(am.map{ case (dow, doy, num) =>
        (doy, num)
      }).map{ case (k, (a, am)) => ((a._1, a._2, a._3), (a._4, a._5.toDouble/am, a._5, am , k))}

    val cp_base_join_bm = cp_base.map{ case (rid, hour, dow, num) =>
      (dow, (rid, hour, dow, num))
    }.join(bm).map { case (k, (b, bm)) => ((b._1, b._2, b._3), (b._4.toDouble/bm, b._4, bm, k))}

    val events = cp_analyze_join_am.join(cp_base_join_bm).map {
      case (k, (a, b)) => (k, a._2/b._1 - 1, a._3, b._2)
    }
    events.saveAsTextFile(output_location + "/events")

    val events_filter = events.filter{case (k, ratio, anum, bnum) =>
      Math.abs(ratio) >= 0.2 && Math.abs(anum - bnum) >= 50 && ratio > 0
    }.map{ case (k, ratio, anum, bnum) =>
      (k, Math.floor(Math.abs(ratio / 0.1)) * 0.1 * Math.signum(ratio))
    }
    events_filter.saveAsTextFile(output_location + "/events_filter")
  }
}
