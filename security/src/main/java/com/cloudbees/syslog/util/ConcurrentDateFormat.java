/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.cloudbees.syslog.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Thread safe date formatter.
 *
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class ConcurrentDateFormat {
  private final BlockingQueue<SimpleDateFormat> dateFormats;
  private final String pattern;
  private final Locale locale;
  private final TimeZone timeZone;

  /**
   * <b>Note:</b> This constructor may not support all locales. For full coverage, use the factory
   * methods in the {@link java.text.DateFormat} class.
   *
   * @param pattern the pattern describing the date and time pattern
   * @param locale the locale whose date pattern symbols should be used
   * @param timeZone the timezone used by the underlying calendar
   * @param maxCacheSize
   * @throws NullPointerException if the given pattern or locale is null
   * @throws IllegalArgumentException if the given pattern is invalid
   */
  public ConcurrentDateFormat(String pattern, Locale locale, TimeZone timeZone, int maxCacheSize) {
    this.dateFormats = new LinkedBlockingDeque<SimpleDateFormat>(maxCacheSize);
    this.pattern = pattern;
    this.locale = locale;
    this.timeZone = timeZone;
  }

  /**
   * Formats a Date into a date/time string.
   *
   * @param date the time value to be formatted into a time string.
   * @return the formatted time string.
   */
  public String format(Date date) {
    SimpleDateFormat dateFormat = dateFormats.poll();
    if (dateFormat == null) {
      dateFormat = new SimpleDateFormat(pattern, locale);
      dateFormat.setTimeZone(timeZone);
    }
    try {
      return dateFormat.format(date);
    } finally {
      dateFormats.offer(dateFormat);
    }
  }

  @Override
  public String toString() {
    return "ConcurrentDateFormat[pattern=" + pattern + "]";
  }
}
