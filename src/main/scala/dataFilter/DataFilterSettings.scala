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
