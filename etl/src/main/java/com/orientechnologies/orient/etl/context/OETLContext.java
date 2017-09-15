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

package com.orientechnologies.orient.etl.context;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.output.OPluginMessageHandler;

/**
 * OETLContext extends OBasicCommandContext, in order to handle the following additional elements:
 * - message handler for application messages
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */

public class OETLContext extends OBasicCommandContext {

  private OPluginMessageHandler messageHandler;

  /**
   * By default the OETLContext in initialized with a message handler with:
   * - verbosity level: 0 (NONE) --> nothing will be printed out
   * - output stream: null
   * - logging: OLogManager
   */
  public OETLContext() {
    this.messageHandler = new OETLMessageHandler(0);
  }

  public OPluginMessageHandler getMessageHandler() {
    return this.messageHandler;
  }

  public void setMessageHandler(OPluginMessageHandler messageHandler) {
    this.messageHandler = messageHandler;
  }

}
