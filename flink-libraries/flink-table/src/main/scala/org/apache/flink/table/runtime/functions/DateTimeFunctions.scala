/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.runtime.functions

import java.time.Instant
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle, TextStyle}
import java.time.temporal.{ChronoField, WeekFields}

object DateTimeFunctions {
  private val BASE_YEAR = 1970

  private val DATETIME_FORMATTER_CACHE = new ThreadLocalCache[String, DateTimeFormatter](64) {
    protected override def getNewInstance(format: String): DateTimeFormatter
    = createDateTimeFormatter(format)
  }

  def dateFormat(ts: Long, formatString: String): String = {
    val formatter = DATETIME_FORMATTER_CACHE.get(formatString)
    val instant = Instant.ofEpochMilli(ts)
    formatter.format(instant)
  }

  def createDateTimeFormatter(format: String): DateTimeFormatter = {
    val builder = new DateTimeFormatterBuilder
    var escaped = false
    var i = 0
    while (i < format.length) {
      val character = format.charAt(i)
      i = i + 1
      if (escaped) {
        character match {
          // %a Abbreviated weekday name (Sun..Sat)
          case 'a' => builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.SHORT)
          // %b Abbreviated month name (Jan..Dec)
          case 'b' => builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT)
          // %c Month, numeric (1..12)
          case 'c' => builder.appendValue(ChronoField.MONTH_OF_YEAR)
          // %d Day of the month, numeric (01..31)
          case 'd' => builder.appendValue(ChronoField.DAY_OF_MONTH, 2)
          // %e Day of the month, numeric (1..31)
          case 'e' => builder.appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
          // %f Microseconds (000000..999999)
          case 'f' => builder.appendValue(ChronoField.SECOND_OF_DAY, 6, 9, SignStyle.NOT_NEGATIVE)
          // %H Hour (00..23)
          case 'H' => builder.appendValue(ChronoField.HOUR_OF_DAY, 2)
          // %h Hour (01..12)
          case 'h' | 'I' => builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 2)
          // %i Minutes, numeric (00..59)
          case 'i' => builder.appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          // %j Day of year (001..366)
          case 'j' => builder.appendValue(ChronoField.DAY_OF_YEAR, 3)
          // %k Hour (0..23)
          case 'k' => builder.appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
          // %l Hour (1..12)
          case 'l' => builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 1, 2, SignStyle.NOT_NEGATIVE)
          // %M Month name (January..December)
          case 'M' => builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL)
          // %m Month, numeric (00..12)
          case 'm' => builder.appendValue(ChronoField.MONTH_OF_YEAR, 2)
          // %p AM or PM
          case 'p' => builder.appendText(ChronoField.AMPM_OF_DAY, TextStyle.SHORT)
          // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
          case 'r' => builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 2).appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2).appendLiteral(' ')
            .appendText(ChronoField.AMPM_OF_DAY, TextStyle.SHORT)
          // %S Seconds (00..59)
          case 'S' | 's' => builder.appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          // %T Time, 24-hour (hh:mm:ss)
          case 'T' => builder.appendValue(ChronoField.CLOCK_HOUR_OF_DAY, 2).appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          // %v Week (01..53), where Monday is the first day of the week; used with %x
          case 'v' => builder.appendValue(WeekFields.ISO.weekOfWeekBasedYear(), 2)
          // %x Year for the week, where Monday is the first day of the week, numeric,
          // four digits; used with %v
          case 'x' => builder.appendValue(WeekFields.ISO.weekOfYear(), 4, 4, SignStyle.NOT_NEGATIVE)
          // %W Weekday name (Sunday..Saturday)
          case 'W' => builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.FULL)
          // %Y Year, numeric, four digits
          case 'Y' => builder.appendValue(ChronoField.YEAR, 4)
          // %y Year, numeric (two digits)
          case 'y' => builder.appendValueReduced(ChronoField.YEAR, 2, 2, BASE_YEAR)

          // %w Day of the week (0=Sunday..6=Saturday)
          // %U Week (00..53), where Sunday is the first day of the week
          // %u Week (00..53), where Monday is the first day of the week
          // %V Week (01..53), where Sunday is the first day of the week; used with %X
          // %X Year for the week where Sunday is the first day of the
          // week, numeric, four digits; used with %V
          // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, ...)
          case 'w' | 'U' | 'u' | 'V' | 'X' | 'D' =>
            throw new UnsupportedOperationException(
              s"%%$character not supported in date format string")
          // %% A literal "%" character
          case '%' => builder.appendLiteral('%')
          // %<x> The literal character represented by <x>
          case _ => builder.appendLiteral(character)
        }
        escaped = false
      }
      else if (character == '%') {
        escaped = true
      }
      else {
        builder.appendLiteral(character)
      }
    }
    builder.toFormatter
  }
}
