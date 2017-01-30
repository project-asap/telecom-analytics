import scala.util.{Try, Success, Failure}

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

object CDR {
    def from_string(row: String, field2col: Map[String,Int]) = {
        println(row)
        val split      = row.split(";")
        val user_id    = split( field2col("user_id")  )
        val start_cell = split( field2col("start_cell")  )
        val end_cell   = split( field2col("end_cell")  )
        val date       = string_to_date( split( field2col("date")  )  )
        val time       = split( field2col("time")  ).toInt

        new CDR(user_id, start_cell, end_cell, date, time)
    }

  def string_to_date(str: String): DateTime = {
    val datePattern = "yyyy-MM-dd"
    val timePattern = "HH:mm:ss"
    val datetimeDelim = " "
    val datetimePattern = Array(datePattern, timePattern).mkString(datetimeDelim)
    val dateFormat = DateTimeFormat.forPattern(datePattern)
    val datetimeFormat = DateTimeFormat.forPattern(datetimePattern)
    datetimeFormat.parseDateTime(str)
  }
}

class CDR(val user_id: String, start_cell: String, end_cell: String, val date: DateTime, val time: Int) extends Serializable{
    def is_we(): Boolean = { Range(0,6).contains( date.dayOfWeek().get() )  }
    def day_of_week(): Int = { date.dayOfWeek().get()  }
    def year(): Int = { date.year().get()  }
    def region(cell2region: Map[String,Int]): Int = { cell2region(start_cell)  }
    def site(): String = {
      Try(start_cell.substring(0, 5)) match {
        case Success(v) => v
        case Failure(e) => "invalid"
      }
    }
    def valid_region(cell2region: Map[String, String]): Boolean = cell2region.contains(site)
    def day_time(): Int = { time  }
    def week(): Int = { date.weekOfWeekyear().get()  }
}
