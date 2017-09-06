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

/**
 * Interface used by OrientDB plugins to handle application messages for different outputs.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public interface OPluginMessageHandler {

  int getLevel();

  void setLevel(int level);

  void debug(String message);

  void debug(String format, Object... args);

  void info(String message);

  void info(String format, Object... args);

  void warn(String message);

  void warn(String format, Object... args);

  void error(String message);

  void error(String format, Object... args);

}
