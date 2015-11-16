package pd

import scala.util.Try

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

case class Call(
  val cdrType: String,
  val callingPartyNumberKey: String,
  val callingSubscriberImsi: String,
  val dateTime: DateTime,
  val chargeableDuration: String,
  val exchangeIdentity: String,
  val outgoingRoute: String,
  val incomingRoute: String,
  val cellId1stCellCalling: String,
  val gsmTeleServiceCode: String,
  val cellIdLastCellCalling: String,
  val disconnectingParty: String,
  val callingSubscriberImei: String,
  val tac: String,
  val residentCustomerFlag: String,
  val paymentType: String,
  val contractStatus: String,
  val contractStartingDate: String,
  val country: String,
  val cityPostalCode: String,
  val city: String
){
  val isWeekday = dateTime.getDayOfWeek < DateTimeConstants.SATURDAY
  def timeSlot(duration: Int) = dateTime.getHourOfDay / duration
  val timeSlot: Int = timeSlot(8)
}

object Call {
  val datePattern = "yyyyMMdd"
  val timePattern = "HHmmss"
  val datetimeDelim = ":"
  val datetimePattern = Array(datePattern, timePattern).mkString(datetimeDelim)
  val dateFormat = DateTimeFormat.forPattern(datePattern)
  val datetimeFormat = DateTimeFormat.forPattern(datetimePattern)

  def apply(a: Array[String]): Try[Call] = {
   Try {
    val (callDate, timeStartCharge) = (a(3), a(4))
    val dateTime = Array(callDate, timeStartCharge).mkString(datetimeDelim)
    val dt = datetimeFormat.parseDateTime(dateTime)

    new Call(
      a(0), a(1), a(2), dt, a(5), a(6), a(7), a(8), a(9), a(10),
      a(11), a(12), a(13), a(14), a(15), a(16), a(17), a(18), a(19),
      a(20), a(21)
    )
   }
  }
}

case class CDR(
  val id: String,
  val hour: Int,
  val dow: Int,
  val doy: Int
)

case class DataRaw(
  val id: String,
  val hour: Int,
  val dow: Int,
  val doy: Int,
  val num: Int
)
object DataRaw {
  def apply(c: CDR, num: Int): DataRaw =
    new DataRaw(c.id, c.hour, c.dow, c.doy, num)
}

case class CpBase(
  val id: String,
  val hour: Int,
  val dow: Int,
  val num: Int
)

case class Key(
  val id: String,
  val hour: Int,
  val dow: Int
)

case class Event(
  val k: Key,
  val ratio: Double,
  val aNum: Double,
  val bNum: Double
)

case class SpaceTimeCall(
  val callingSubscriberImsi: String,
  val city: String,
  val week: Int,
  val weekday: Boolean,
  val timeSlot: Int,
  val day: Int,
  val cell: String
){
  val key = (callingSubscriberImsi, city, week, weekday, timeSlot)
  val view = (week, weekday, day)
}

case class Antenna(
  val site: String,
  val sector: String,
  val sector_uuid: String,
  val cell: String,
  val x: Double,
  val y: Double,
  val azimuth: Double
)
object Antenna {
  def safeDouble(s: String, default: Double = 0.0) =
    Try(s.toDouble).getOrElse(default)

  def apply(a: Array[String]): Try[Antenna] = {
   Try {
    new Antenna(
      a(0), a(1), a(2), a(3),
      safeDouble(a(4)),
      safeDouble(a(5)),
      safeDouble(a(6))
    )
   }
  }
}
