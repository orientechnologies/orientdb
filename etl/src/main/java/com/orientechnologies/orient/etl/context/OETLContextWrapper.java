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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.output.OPluginMessageHandler;

/**
 * Singleton used as wrapper for the OETLContext.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OETLContextWrapper {

  private static OETLContextWrapper instance = null;
  private OCommandContext context;

  private OETLContextWrapper() {}

  public static OETLContextWrapper getInstance() {
    if (instance == null) {
      instance = new OETLContextWrapper();
    }
    return instance;
  }

  public OCommandContext getContext() {
    return context;
  }

  public void setContext(OCommandContext context) {
    this.context = context;
  }

  public OPluginMessageHandler getMessageHandler() {
    return ((OETLContext) this.context).getMessageHandler();
  }

  public void setMessageHandler(OPluginMessageHandler messageHandler) {
    ((OETLContext) this.context).setMessageHandler(messageHandler);
  }
}
