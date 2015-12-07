package ta

import scala.util.Try

import org.joda.time.DateTime

case class DataFilterSettings(
    val master: String,
    val cdrPath: String,
    val output: String,
    val trainingSince: DateTime,
    val trainingUntil: DateTime,
    val testSince: DateTime,
    val testUntil: Option[DateTime],
    val voronoiPath: String
)
object DataFilterSettings {
  private def parseDate = s => Call.dateFormat.parseDateTime(s)

  def apply(args: Array[String]) = {
    new DataFilterSettings(
      args(0),
      args(1),
      args(2),
      parseDate(args(3)),
      parseDate(args(4)),
      parseDate(args(5)),
      Try(parseDate(args(6))).toOption,
      args(7)
    )
  }

  override def toString = "<master> <cdrPath> <output> " +
  s"<trainingSince (${Call.datePattern})> " +
  s"<trainingUntil (${Call.datePattern})> " +
  s"<testSince (${Call.datePattern})> " +
  s"<testUntil (${Call.datePattern} or None)> " +
  "<voronoiPath>"
}
