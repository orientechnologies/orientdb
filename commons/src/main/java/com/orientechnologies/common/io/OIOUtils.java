/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.io;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class OIOUtils {
  public static final long SECOND = 1000;
  public static final long MINUTE = SECOND * 60;
  public static final long HOUR   = MINUTE * 60;
  public static final long DAY    = HOUR * 24;
  public static final long YEAR   = DAY * 365;
  public static final long WEEK   = DAY * 7;

  public static byte[] toStream(Externalizable iSource) throws IOException {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(stream);
    iSource.writeExternal(oos);
    oos.flush();
    stream.flush();
    return stream.toByteArray();
  }

  public static long getTimeAsMillisecs(final Object iSize) {
    if (iSize == null)
      throw new IllegalArgumentException("Time is null");

    if (iSize instanceof Number)
      // MILLISECS
      return ((Number) iSize).longValue();

    String time = iSize.toString();

    boolean number = true;
    for (int i = time.length() - 1; i >= 0; --i) {
      if (!Character.isDigit(time.charAt(i))) {
        number = false;
        break;
      }
    }

    if (number)
      // MILLISECS
      return Long.parseLong(time);
    else {
      time = time.toUpperCase(Locale.ENGLISH);

      int pos = time.indexOf("MS");
      final String timeAsNumber = time.replaceAll("[^\\d]", "");
      if (pos > -1)
        return Long.parseLong(timeAsNumber);

      pos = time.indexOf("S");
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * SECOND;

      pos = time.indexOf("M");
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * MINUTE;

      pos = time.indexOf("H");
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * HOUR;

      pos = time.indexOf("D");
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * DAY;

      pos = time.indexOf('W');
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * WEEK;

      pos = time.indexOf('Y');
      if (pos > -1)
        return Long.parseLong(timeAsNumber) * YEAR;

      // RE-THROW THE EXCEPTION
      throw new IllegalArgumentException("Time '" + time + "' has a unrecognizable format");
    }
  }

  public static String getTimeAsString(final long iTime) {
    if (iTime > YEAR && iTime % YEAR == 0)
      return String.format("%dy", iTime / YEAR);
    if (iTime > WEEK && iTime % WEEK == 0)
      return String.format("%dw", iTime / WEEK);
    if (iTime > DAY && iTime % DAY == 0)
      return String.format("%dd", iTime / DAY);
    if (iTime > HOUR && iTime % HOUR == 0)
      return String.format("%dh", iTime / HOUR);
    if (iTime > MINUTE && iTime % MINUTE == 0)
      return String.format("%dm", iTime / MINUTE);
    if (iTime > SECOND && iTime % SECOND == 0)
      return String.format("%ds", iTime / SECOND);

    // MILLISECONDS
    return String.format("%dms", iTime);
  }

  public static Date getTodayWithTime(final String iTime) throws ParseException {
    final SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    final long today = System.currentTimeMillis();
    final Date rslt = new Date();
    rslt.setTime(today - (today % DAY) + df.parse(iTime).getTime());
    return rslt;
  }

  public static String readFileAsString(final File iFile) throws java.io.IOException {
    final StringBuffer fileData = new StringBuffer(1000);
    final BufferedReader reader = new BufferedReader(new FileReader(iFile));
    try {
      final char[] buf = new char[1024];
      int numRead = 0;
      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
      }
    } finally {
      reader.close();
    }
    return fileData.toString();
  }

  /**
   * Returns the Unix file name format converting backslashes (\) to slasles (/)
   */
  public static String getUnixFileName(final String iFileName) {
    return iFileName != null ? iFileName.replace('\\', '/') : null;
  }

  public static String getRelativePathIfAny(final String iDatabaseURL, final String iBasePath) {
    if (iBasePath == null) {
      final int pos = iDatabaseURL.lastIndexOf('/');
      if (pos > -1)
        return iDatabaseURL.substring(pos + 1);
    } else {
      final int pos = iDatabaseURL.indexOf(iBasePath);
      if (pos > -1)
        return iDatabaseURL.substring(pos + iBasePath.length() + 1);
    }

    return iDatabaseURL;
  }

  public static String getDatabaseNameFromPath(final String iPath) {
    return iPath.replace('/', '$');
  }

  public static String getPathFromDatabaseName(final String iPath) {
    return iPath.replace('$', '/');
  }
}
