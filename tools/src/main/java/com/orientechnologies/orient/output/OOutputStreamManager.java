/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 *
 * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.output;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * It contains and manages an OutputStream at different and desired levels. The default OutputStream
 * is 'System.out', but it's possible to instantiate the class with a specific one by passing it to
 * the class Constructor. Levels: - 0 : no output - 3 : only error level is printed - 2 : from info
 * to error is printed - 1 : from debug to error is printed
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OOutputStreamManager {

  public PrintStream outputStream;
  private int level;
  public static final int BLANK_LEVEL = 0;
  public static final int DEBUG_LEVEL = 1;
  public static final int INFO_LEVEL = 2;
  public static final int WARNING_LEVEL = 3;
  public static final int ERROR_LEVEL = 4;

  public OOutputStreamManager(int level) {
    this.outputStream = System.out;
    this.level = level;
  }

  public OOutputStreamManager(PrintStream outputStream, int level) {
    this.outputStream = outputStream;
    this.level = level;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public synchronized int getLevel() {
    return level;
  }

  public synchronized void setLevel(int level) {
    this.level = level;
  }

  public synchronized void debug(String message) {
    if (!(this.level == BLANK_LEVEL) && message != null) {
      if (this.level <= DEBUG_LEVEL) this.outputStream.print(message);
    }
  }

  public synchronized void debug(String format, Object... args) {
    if (!(this.level == BLANK_LEVEL) && format != null) {
      if (this.level <= DEBUG_LEVEL) this.outputStream.printf(format, args);
    }
  }

  public synchronized void info(String message) {
    if (!(this.level == BLANK_LEVEL) && message != null) {
      if (this.level <= INFO_LEVEL) this.outputStream.print(message);
    }
  }

  public synchronized void info(String format, Object... args) {
    if (!(this.level == BLANK_LEVEL) && format != null) {
      if (this.level <= INFO_LEVEL) this.outputStream.printf(format, args);
    }
  }

  public synchronized void warn(String message) {
    if (!(this.level == BLANK_LEVEL) && message != null) {
      if (this.level <= WARNING_LEVEL) this.outputStream.print(message);
    }
  }

  public synchronized void warn(String format, Object... args) {
    if (!(this.level == BLANK_LEVEL) && format != null) {
      if (this.level <= WARNING_LEVEL) this.outputStream.printf(format, args);
    }
  }

  public synchronized void error(String message) {
    if (!(this.level == BLANK_LEVEL) && message != null) {
      if (this.level <= ERROR_LEVEL) this.outputStream.print("\nERROR: " + message);
    }
  }

  public synchronized void error(String format, Object... args) {
    if (!(this.level == BLANK_LEVEL) && format != null) {
      if (this.level <= ERROR_LEVEL) this.outputStream.printf("\nERROR: " + format, args);
    }
  }
}
