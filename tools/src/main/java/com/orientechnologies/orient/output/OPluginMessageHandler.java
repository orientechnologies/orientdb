/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.output;

/**
 * Interface used by OrientDB plugins to handle application messages for different outputs.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public interface OPluginMessageHandler {

  int getOutputManagerLevel();

  void setOutputManagerLevel(int outputManagerLevel);

  void debug(Object requester, String message);

  void debug(Object requester, String format, Object... args);

  void info(Object requester, String message);

  void info(Object requester, String format, Object... args);

  void warn(Object requester, String message);

  void warn(Object requester, String format, Object... args);

  void error(Object requester, String message);

  void error(Object requester, String format, Object... args);
}
