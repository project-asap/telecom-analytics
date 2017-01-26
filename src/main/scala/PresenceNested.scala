import org.apache.spark._
import org.apache.spark.SparkContext._

object PresenceNested {
  def main(args: Array[String]) = {
     val master = "spark://asap42:7077"
     val conf = new SparkConf().setAppName("Presence").setMaster(master)
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired","false")
        .registerKryoClasses(Array(classOf[Array[Double]],classOf[Array[Int]]))

    val sc = new SparkContext()
/*
    val pattern = """\(\(u'(\S*)', '(\S)'\), '(dynamic_resident|visitor|resident|commuter|passing\ by)'\)""".r
    val s1 = "((u'107451309044480586', '2'), 'dynamic_resident')"
    val s2 = "((u'92863552200163866', '4'), 'passing by')"
    val s3 = "((u'29917139386874174', '5'), 'commuter')"

    val user2label = "/annotation_global/forth"
*/
    val pattern = """\(u'(\S*)', '(\S)', '(dynamic_resident|visitor|resident|commuter|passing\ by)'\)""".r
/*
    val s1 = "(u'106541285272914564', '1', 'passing by')"
    val s2 = "(u'205799597446887730', '3', 'commuter')"
    val s3 = "(u'135520406994217194', '1', 'dynamic_resident')"

    s1 match {case pattern(user, region, user_class) => print("ole")}
    s2 match {case pattern(user, region, user_class) => print("ole")}
    s3 match {case pattern(user, region, user_class) => print("ole")}
*/
    val user2label = "file:///asap_data/user2label.csv"
    val user_annotation = sc.textFile(user2label).map{
        case pattern(user, region, user_class) => ((user, region), user_class)
    }

    val userCache = scala.collection.mutable.HashMap.empty[(String, String), String]

    def getUserClass(user: String, region: String): String = {
        println(">>> getUserClass: " + user + ", " + region)
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
        println("<<< getUserClass: " + user + ", " + region + ", " + label)
        userCache((user, region)) = label
        label
    }

    getUserClass("29917139386874174", "5")
    getUserClass("107451309044480586", "2")
/*
    getClass(users: Set[String], region: String): String = {
        val rest = users.filter( u => !(userCache contains (u, region)) )
                        .map(_._1)
        val results = user_annotation
            .filter{ case ((u, r), _) => rest contains U && r == region }
            .collect
        results.map( case (u, r, label) => ((u, r), label) )
    }
*/
    val interestRegion = "file:///asap_data/aree_roma.csv"

    val sites2zones  = sc.textFile(interestRegion).map(_.split(";")).map{
        case a => (a(0).substring(0, 5), a(1))}.collectAsMap.toMap

    val field2col: Map[String,Int] =
        Map("user_id" -> 0, "cdr_type" -> 11, "start_cell" -> 9 ,"end_cell"->10 , "date" -> 3 ,"time"-> 4)

    val results = sc.textFile("hdfs://sith0:9000/user/spapagian/dataset_simulated/REPORT_ESTRAZ_TRAFF_COMUNE_ROMA_20160301.csv")
        .map( line => CDR.from_string(line, field2col) )
        .filter ( cdr => cdr.valid_region(sites2zones) )
        .map( cdr => (sites2zones(cdr.site), cdr.date, cdr.user_id) )
        .distinct
        .map{ case (zone, date, user_id)  => ((zone, date, getUserClass(user_id, zone) ), 1) }
        .reduceByKey( _ + _ )
        .collectAsMap
    print("<<< <<<" + results)
  }
}
