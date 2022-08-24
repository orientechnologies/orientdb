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

package com.orientechnologies.common.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.IllegalFormatException;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Basic Log formatter.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OLogFormatter extends Formatter {

  protected static final DateTimeFormatter dateFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

  /** The end-of-line character for this platform. */
  protected static final String EOL = System.getProperty("line.separator");

  @Override
  public String format(final LogRecord record) {
    final String formattedMessage = customFormatMessage(record);
    final Throwable error = record.getThrown();
    if (error == null) {
      return formattedMessage;
    }

    // FORMAT THE STACK TRACE
    final StringWriter writer = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(writer);
    printWriter.println(formattedMessage);
    error.printStackTrace(printWriter);
    printWriter.close();

    return writer.toString();
  }

  protected String customFormatMessage(final LogRecord iRecord) {
    final Level level = iRecord.getLevel();
    final String message = OAnsiCode.format(iRecord.getMessage(), false);
    final Object[] additionalArgs = iRecord.getParameters();
    final String requester = getSourceClassSimpleName(iRecord.getLoggerName());

    final StringBuilder buffer = new StringBuilder(512);

    buffer.append(EOL);
    buffer.append(dateFormatter.format(LocalDateTime.now()));
    buffer.append(String.format(" %-5.5s ", level.getName()));

    // FORMAT THE MESSAGE
    try {
      if (additionalArgs != null) buffer.append(String.format(message, additionalArgs));
      else buffer.append(message);
    } catch (IllegalFormatException ignore) {
      buffer.append(message);
    }

    if (requester != null) {
      buffer.append(" [");
      buffer.append(requester);
      buffer.append(']');
    }

    return OAnsiCode.format(buffer.toString(), false);
  }

  protected String getSourceClassSimpleName(final String iSourceClassName) {
    if (iSourceClassName == null) return null;
    return iSourceClassName.substring(iSourceClassName.lastIndexOf(".") + 1);
  }
}
