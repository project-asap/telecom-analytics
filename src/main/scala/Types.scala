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
package ta

import scala.util.Try

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

case class Call(
  val callingPartyNumberKey: String,
  val dateTime: DateTime,
  val chargeableDuration: String,
  val cellId1stCellCalling: String,
  val cellIdLastCellCalling: String
){
  val isWeekday = dateTime.getDayOfWeek < DateTimeConstants.SATURDAY
  def timeSlot(duration: Int) = dateTime.getHourOfDay / duration
  val timeSlot: Int = timeSlot(8)
}

object Call {
  val datePattern = "yyyy-MM-dd"
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
      a(0), dt, a(5), a(9), a(10)
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

  def apply(str: String) = {
    val pattern = """^DataRaw\((\S*),(\d*),(\d*),(\d*),(\d*)\)""".r
    str match {
      case pattern(id, hour, dow, doy, num) =>
        new DataRaw(id, hour.toInt, dow.toInt, doy.toInt, num.toInt)
    }
  }
}

case class CpBase(
  val id: String,
  val hour: Int,
  val dow: Int,
  val num: Int
)
object CpBase {
  def apply(str: String) = {
    val pattern = """^CpBase\((\S*),(\d*),(\d*),(\d*)\)""".r
    str match {
      case pattern(id, hour, dow, num) =>
        new CpBase(id, hour.toInt, dow.toInt, num.toInt)
    }
  }
}

case class Key(
  val id: String,
  val hour: Int,
  val dow: Int
)

case class Event(
  val id: String,
  val hour: Int,
  val doy: Int,
  val dow: Int,
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

case class Profile(
  val city: String,
  val week: Int,
  val isWeekend: Int,
  val timesplit: Int,
  val count: Double
)
object Profile {
  val pattern = """\((\S*), (\d), (\d), (\d), (\d*.\d*)\)""".r
  def apply(str: String) = {
    str match {
      case pattern(city, week, isWeekend, timesplit, count) =>
        new Profile(city, week.toInt, isWeekend.toInt, timesplit.toInt, count.toDouble)
    }
  }
}
