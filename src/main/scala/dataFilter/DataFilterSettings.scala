package ta

import scala.util.Try

import org.joda.time.DateTime

case class DataFilterSettings(
    val master: String,
    val cdrIn: String,
    val voronoiIn: String,
    val trainingOut: String,
    val testOut: String,
    val trainingSince: DateTime,
    val trainingUntil: DateTime,
    val testSince: DateTime,
    val testUntil: Option[DateTime]
)
object DataFilterSettings {
  private def parseDate = s => Call.dateFormat.parseDateTime(s)

  def apply(args: Array[String]) = {
    new DataFilterSettings(
      args(0),
      args(1),
      args(2),
      args(3),
      args(4),
      parseDate(args(5)),
      parseDate(args(6)),
      parseDate(args(7)),
      Try(parseDate(args(8))).toOption
    )
  }

  override def toString = "<master> <cdrIn> <voronoiIn> <trainingOut> <testOut> " +
  s"<trainingSince (${Call.datePattern})> " +
  s"<trainingUntil (${Call.datePattern})> " +
  s"<testSince (${Call.datePattern})> " +
  s"<testUntil (${Call.datePattern} or None)> "
}
