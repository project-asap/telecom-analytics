/*
 * Copyright 2017 ASAP.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.joda.time.format.DateTimeFormat

import java.io._

object PresenceNestedBulk {
    def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
        val p = new java.io.PrintWriter(f)
        try { op(p)  } finally { p.close()  }
    }

  def main(args: Array[String]) = {
      val appName = this.getClass().getSimpleName
      val usage = (s"Usage: submit.sh ${appName} <master> <interestRegion> <user2label> <field2col> <dateFormat> <input> <tag>")

      if (args.length != 7) {
          System.err.println(usage)
          System.exit(1)
      }

     val master = args(0)
     val interestRegion = args(1)
     val user2label = args(2)
     //val field2col = args(3)
     val field2col: Map[String,Int] =
        Map("user_id" -> 0, "cdr_type" -> 11, "start_cell" -> 9 ,"end_cell"->10 , "date" -> 3 ,"time"-> 4)
     val dateFormat = args(4)
     val input = args(5)
     val tag = args(6)

     val conf = new SparkConf().setAppName(appName).setMaster(master)
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired","false")
        .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))

    val sc = new SparkContext()
    val pattern = """\(\(u'(\S*)', '(\S)'\), '(dynamic_resident|visitor|resident|commuter|passing\ by)'\)""".r

    val user_annotation = sc.textFile(s"${user2label}/${tag}").map{
        case pattern(user, region, user_class) => ((user, region), user_class)
    }

    val userCache = scala.collection.mutable.HashMap.empty[(String, String), String]

    def getUserClass(user: String, region: String): String = {
        // println(">>> getUserClass: " + user + ", " + region)
        val label = userCache.getOrElse(
            (user, region),
            {
                val results = user_annotation.filter{ case ((u, r), _) => u == user && r == region }.collect
                results.length match {
                    case 0 => "not classified"
                    case _ => results(0)._2
                }
            }
        )
        // println("<<< getUserClass: " + user + ", " + region + ", " + label)
        userCache((user, region)) = label
        label
    }

/*
    getUserClass("29917139386874174", "5")
    getUserClass("107451309044480586", "2")
    println("<<<" + userCache)
*/
    def users2class(users: Set[String], region: String) = {
        val rest = users.filter( u => !(userCache contains (u, region)) )
        val results = user_annotation
            .filter{ case ((u, r), _) => rest.contains(u) && r == region }
            .collect
            .toMap
        userCache ++= results
        userCache ++= rest.filter(u => !results.contains((u, region)))
            .map(u => (u, region) -> "not classified").toMap
    }

//    users2class(Set("29917139386874174", "107451309044480586", "invalid"), "5")


    val sites2zones  = sc.textFile(interestRegion).map(_.split(";")).map{
        case a => (a(0).substring(0, 5), a(1))}.collectAsMap.toMap


    val results  = sc.textFile(input)
        .map( line => CDR.from_string(line, field2col) )
        .filter ( cdr => cdr.valid_region(sites2zones) )
        .map( cdr => ((sites2zones(cdr.site), cdr.date), Set(cdr.user_id) ))
        .reduceByKey( _ ++ _  )
        .flatMap{ case ((zone, date), users) => {
            users2class(users, zone)
            for(u <- users) yield (zone, date, u)
        }}
        .map{ case (zone, date, user) => ((zone, date, userCache((user, zone))), 1) }
        .reduceByKey( _ + _ )
        .collect


    val datePattern = "yyyy-MM-dd"
    val timePattern = "HH:mm:ss"
    val datetimeDelim = " "
    val datetimePattern = Array(datePattern, timePattern).mkString(datetimeDelim)
    val datetimeFormat = DateTimeFormat.forPattern(datetimePattern)

    printToFile(new File(s"presence_timeseries-${tag}.csv")) { p =>
        results.foreach{ case ((zone, date, user_class), count) => p.println(s"${zone};${datetimeFormat.print(date)};${user_class};${count}") }
    }
  }
}
