/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.io;

import com.kenai.jffi.Platform;
import com.orientechnologies.common.jnr.LastErrorException;
import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.util.OPatternConst;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class OIOUtils {
  public static final long SECOND = 1000;
  public static final long MINUTE = SECOND * 60;
  public static final long HOUR = MINUTE * 60;
  public static final long DAY = HOUR * 24;
  public static final long YEAR = DAY * 365;
  public static final long WEEK = DAY * 7;
  public static final String UTF8_BOM = "\uFEFF";

  public static long getTimeAsMillisecs(final Object iSize) {
    if (iSize == null) throw new IllegalArgumentException("Time is null");

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
      if (pos > -1) return Long.parseLong(timeAsNumber);

      pos = time.indexOf("S");
      if (pos > -1) return Long.parseLong(timeAsNumber) * SECOND;

      pos = time.indexOf("M");
      if (pos > -1) return Long.parseLong(timeAsNumber) * MINUTE;

      pos = time.indexOf("H");
      if (pos > -1) return Long.parseLong(timeAsNumber) * HOUR;

      pos = time.indexOf("D");
      if (pos > -1) return Long.parseLong(timeAsNumber) * DAY;

      pos = time.indexOf('W');
      if (pos > -1) return Long.parseLong(timeAsNumber) * WEEK;

      pos = time.indexOf('Y');
      if (pos > -1) return Long.parseLong(timeAsNumber) * YEAR;

      // RE-THROW THE EXCEPTION
      throw new IllegalArgumentException("Time '" + time + "' has a unrecognizable format");
    }
  }

  public static String getTimeAsString(final long iTime) {
    if (iTime > YEAR && iTime % YEAR == 0) return String.format("%dy", iTime / YEAR);
    if (iTime > WEEK && iTime % WEEK == 0) return String.format("%dw", iTime / WEEK);
    if (iTime > DAY && iTime % DAY == 0) return String.format("%dd", iTime / DAY);
    if (iTime > HOUR && iTime % HOUR == 0) return String.format("%dh", iTime / HOUR);
    if (iTime > MINUTE && iTime % MINUTE == 0) return String.format("%dm", iTime / MINUTE);
    if (iTime > SECOND && iTime % SECOND == 0) return String.format("%ds", iTime / SECOND);

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

  public static String readFileAsString(final File iFile, Charset iCharset) throws IOException {
    return readStreamAsString(new FileInputStream(iFile), iCharset);
  }

  public static String readStreamAsString(final InputStream iStream) throws IOException {
    return readStreamAsString(iStream, StandardCharsets.UTF_8);
  }

  public static String readStreamAsString(final InputStream iStream, Charset iCharset)
      throws IOException {
    final StringBuffer fileData = new StringBuffer(1000);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(iStream, iCharset));
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

  public static void writeFile(final File iFile, final String iContent) throws IOException {
    final FileOutputStream fos = new FileOutputStream(iFile);
    try {
      final OutputStreamWriter os = new OutputStreamWriter(fos);
      try {
        final BufferedWriter writer = new BufferedWriter(os);
        try {
          writer.write(iContent);
        } finally {
          writer.close();
        }
      } finally {
        os.close();
      }
    } finally {
      fos.close();
    }
  }

  public static long copyStream(final InputStream in, final OutputStream out) throws IOException {
    final byte[] buf = new byte[8192];
    int byteRead = 0;
    long byteTotal = 0;
    while ((byteRead = in.read(buf)) != -1) {
      out.write(buf, 0, byteRead);
      byteTotal += byteRead;
    }
    return byteTotal;
  }

  /** Returns the Unix file name format converting backslashes (\) to slasles (/) */
  public static String getUnixFileName(final String iFileName) {
    return iFileName != null ? iFileName.replace('\\', '/') : null;
  }

  public static String getRelativePathIfAny(final String iDatabaseURL, final String iBasePath) {
    if (iBasePath == null) {
      final int pos = iDatabaseURL.lastIndexOf('/');
      if (pos > -1) return iDatabaseURL.substring(pos + 1);
    } else {
      final int pos = iDatabaseURL.indexOf(iBasePath);
      if (pos > -1) return iDatabaseURL.substring(pos + iBasePath.length() + 1);
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
    if (iText == null) return null;
    if (iMax > iText.length()) return iText;
    return iText.substring(0, iMax) + iOther;
  }

  public static Object encode(final Object iValue) {
    if (iValue instanceof String) {
      return java2unicode(((String) iValue).replace("\\", "\\\\").replace("\"", "\\\""));
    } else return iValue;
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
        result.append(hex.toLowerCase(Locale.ENGLISH)); // standard unicode format.
        // ostr.append(hex.toLowerCase(Locale.ENGLISH));
      }
    }

    return result.toString();
  }

  public static boolean isStringContent(final Object iValue) {
    if (iValue == null) return false;

    final String s = iValue.toString();

    if (s == null) return false;

    return s.length() > 1
        && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\''
            || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"');
  }

  public static String getStringContent(final Object iValue) {
    if (iValue == null) return null;

    final String s = iValue.toString();

    if (s == null) return null;

    if (s.length() > 1
        && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\''
            || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))
      return s.substring(1, s.length() - 1);

    if (s.length() > 1 && (s.charAt(0) == '`' && s.charAt(s.length() - 1) == '`'))
      return s.substring(1, s.length() - 1);

    return s;
  }

  public static String wrapStringContent(final Object iValue, final char iStringDelimiter) {
    if (iValue == null) return null;

    final String s = iValue.toString();

    if (s == null) return null;

    return iStringDelimiter + s + iStringDelimiter;
  }

  public static boolean equals(final byte[] buffer, final byte[] buffer2) {
    if (buffer == null || buffer2 == null || buffer.length != buffer2.length) return false;

    for (int i = 0; i < buffer.length; ++i) if (buffer[i] != buffer2[i]) return false;

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

  public static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
    while (len > 0) {
      int n = in.read(b, off, len);

      if (n == -1) {
        throw new EOFException();
      }
      off += n;
      len -= n;
    }
  }

  public static void readByteBuffer(
      ByteBuffer buffer, FileChannel channel, long position, boolean throwOnEof)
      throws IOException {
    int bytesToRead = buffer.limit();

    int read = 0;
    while (read < bytesToRead) {
      buffer.position(read);

      final int r = channel.read(buffer, position + read);
      if (r < 0)
        if (throwOnEof) throw new EOFException("End of file is reached");
        else {
          buffer.put(new byte[buffer.remaining()]);
          return;
        }

      read += r;
    }
  }

  public static void readByteBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
    int bytesToRead = buffer.limit();

    int read = 0;

    while (read < bytesToRead) {
      buffer.position(read);
      final int r = channel.read(buffer);

      if (r < 0) throw new EOFException("End of file is reached");

      read += r;
    }
  }

  public static void readByteBuffer(ByteBuffer buffer, int fd) throws IOException {
    int bytesToRead = buffer.limit();

    int read = 0;

    while (read < bytesToRead) {
      buffer.position(read);

      final int r;

      try {
        r = (int) ONative.instance().read(fd, buffer, buffer.remaining());
      } catch (LastErrorException e) {
        throw new IOException("Error during reading from file", e);
      }

      if (r < 0) {
        throw new EOFException("End of file is reached");
      }

      read += r;
    }

    buffer.position(read);
  }

  public static int writeByteBuffer(ByteBuffer buffer, FileChannel channel, long position)
      throws IOException {
    int bytesToWrite = buffer.limit();

    int written = 0;
    while (written < bytesToWrite) {
      buffer.position(written);

      written += channel.write(buffer, position + written);
    }

    return written;
  }

  public static int calculateBlockSize(String path) {
    if (Platform.getPlatform().getOS() != Platform.OS.LINUX) {
      return -1;
    }

    final int linuxVersion = 0;
    final int majorRev = 1;
    final int minorRev = 2;

    List<Integer> versionNumbers = new ArrayList<>();
    for (String v : System.getProperty("os.version").split("[.\\-]")) {
      if (v.matches("\\d")) {
        versionNumbers.add(Integer.parseInt(v));
      }
    }

    if (versionNumbers.get(linuxVersion) < 2) {
      return -1;
    } else if (versionNumbers.get(linuxVersion) == 2) {
      if (versionNumbers.get(majorRev) < 4) {
        return -1;
      } else if (versionNumbers.get(majorRev) == 4 && versionNumbers.get(minorRev) < 10) {
        return -1;
      }
    }

    final int _PC_REC_XFER_ALIGN = 0x11;

    int fsBlockSize = ONative.instance().pathconf(path, _PC_REC_XFER_ALIGN);
    int pageSize = ONative.instance().getpagesize();
    fsBlockSize = lcm(fsBlockSize, pageSize);

    // just being completely paranoid:
    // (512 is the rule for 2.6+ kernels)
    fsBlockSize = lcm(fsBlockSize, 512);

    if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
      return -1;
    }

    return fsBlockSize;
  }

  private static int lcm(long x, long y) {
    long g = x; // will hold gcd
    long yc = y;

    // get the gcd first
    while (yc != 0) {
      long t = g;
      g = yc;
      yc = t % yc;
    }

    return (int) (x * y / g);
  }
}
