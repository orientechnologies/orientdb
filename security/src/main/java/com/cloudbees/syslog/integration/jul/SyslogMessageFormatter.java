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
package com.cloudbees.syslog.integration.jul;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/** @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a> */
public class SyslogMessageFormatter extends Formatter {

  @Override
  public String format(LogRecord record) {
    String message = formatMessage(record);
    String throwable = "";
    if (record.getThrown() != null) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.println();
      record.getThrown().printStackTrace(pw);
      pw.close();
      throwable = sw.toString();
    }
    return record.getLevel()
        + " ["
        + Thread.currentThread().getName()
        + "] "
        + record.getLoggerName()
        + ": "
        + message
        + throwable;
  }
}
