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
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;

/**
 * Generic interface for server-side commands.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OServerCommand {
  /**
   * Called before to execute. Useful to make checks.
   *
   * @param iResponse TODO
   */
  public boolean beforeExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception;

  /**
   * Called after to execute. Useful to free resources.
   *
   * @param iResponse TODO
   */
  public boolean afterExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception;

  /**
   * Executes the command requested.
   *
   * @return boolean value that indicates if this command is part of a chain
   */
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception;

  public String[] getNames();

  public void configure(OServer server);
}
