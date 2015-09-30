/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.common.io;

import com.orientechnologies.common.util.OPatternConst;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class OIOUtils {
  public static final long   SECOND   = 1000;
  public static final long   MINUTE   = SECOND * 60;
  public static final long   HOUR     = MINUTE * 60;
  public static final long   DAY      = HOUR * 24;
  public static final long   YEAR     = DAY * 365;
  public static final long   WEEK     = DAY * 7;
  public static final String UTF8_BOM = "\uFEFF";

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
      final String timeAsNumber = OPatternConst.PATTERN_NUMBERS.matcher(time).replaceAll("");
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
    Calendar calParsed = Calendar.getInstance();
    calParsed.setTime(df.parse(iTime));
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, calParsed.get(Calendar.HOUR_OF_DAY));
    cal.set(Calendar.MINUTE, calParsed.get(Calendar.MINUTE));
    cal.set(Calendar.SECOND, calParsed.get(Calendar.SECOND));
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  public static String readFileAsString(final File iFile) throws IOException {
    return readStreamAsString(new FileInputStream(iFile));
  }

  public static String readStreamAsString(final InputStream iStream) throws IOException {
    final StringBuffer fileData = new StringBuffer(1000);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(iStream));
    try {
      final char[] buf = new char[1024];
      int numRead = 0;

      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);

        if (fileData.length() == 0 && readData.startsWith(UTF8_BOM))
          // SKIP UTF-8 BOM IF ANY
          readData = readData.substring(1);

        fileData.append(readData);
      }
    } finally {
      reader.close();
    }
    return fileData.toString();
  }

  public static long copyStream(final InputStream in, final OutputStream out, long iMax) throws IOException {
    if (iMax < 0)
      iMax = Long.MAX_VALUE;

    final byte[] buf = new byte[8192];
    int byteRead = 0;
    long byteTotal = 0;
    while ((byteRead = in.read(buf, 0, (int) Math.min(buf.length, iMax - byteTotal))) > 0) {
      out.write(buf, 0, byteRead);
      byteTotal += byteRead;
    }
    return byteTotal;
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

  public static String getStringMaxLength(final String iText, final int iMax) {
    return getStringMaxLength(iText, iMax, "");
  }

  public static String getStringMaxLength(final String iText, final int iMax, final String iOther) {
    if (iText == null)
      return null;
    if (iMax > iText.length())
      return iText;
    return iText.substring(0, iMax) + iOther;
  }

  public static Object encode(final Object iValue) {
    if (iValue instanceof String) {
      return java2unicode(((String) iValue).replace("\\", "\\\\").replace("\"", "\\\""));
    } else
      return iValue;
  }

  public static String java2unicode(final String iInput) {
    final StringBuilder result = new StringBuilder(iInput.length() * 2);
    final int inputSize = iInput.length();

    char ch;
    String hex;
    for (int i = 0; i < inputSize; i++) {
      ch = iInput.charAt(i);

      if (ch >= 0x0020 && ch <= 0x007e) // Does the char need to be converted to unicode?
        result.append(ch); // No.
      else // Yes.
      {
        result.append("\\u"); // standard unicode format.
        hex = Integer.toHexString(ch & 0xFFFF); // Get hex value of the char.
        for (int j = 0; j < 4 - hex.length(); j++)
          // Prepend zeros because unicode requires 4 digits
          result.append('0');
        result.append(hex.toLowerCase()); // standard unicode format.
        // ostr.append(hex.toLowerCase(Locale.ENGLISH));
      }
    }

    return result.toString();
  }

  public static boolean isStringContent(final Object iValue) {
    if (iValue == null)
      return false;

    final String s = iValue.toString();

    if (s == null)
      return false;

    return s.length() > 1
        && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'' || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"');
  }

  public static String getStringContent(final Object iValue) {
    if (iValue == null)
      return null;

    final String s = iValue.toString();

    if (s == null)
      return null;

    if (s.length() > 1
        && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'' || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))
      return s.substring(1, s.length() - 1);

    if (s.length() > 1
        && (s.charAt(0) == '`' && s.charAt(s.length() - 1) == '`'))
      return s.substring(1, s.length() - 1);

    return s;
  }

  public static boolean equals(final byte[] buffer, final byte[] buffer2) {
    if (buffer == null || buffer2 == null || buffer.length != buffer2.length)
      return false;

    for (int i = 0; i < buffer.length; ++i)
      if (buffer[i] != buffer2[i])
        return false;

    return true;
  }

  public static boolean isLong(final String iText) {
    boolean isLong = true;
    final int size = iText.length();
    for (int i = 0; i < size && isLong; i++) {
      final char c = iText.charAt(i);
      isLong = isLong & ((c >= '0' && c <= '9'));
    }
    return isLong;
  }
}
