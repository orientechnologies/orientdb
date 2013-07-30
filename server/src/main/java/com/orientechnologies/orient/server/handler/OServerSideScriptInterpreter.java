/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

/**
 * Allow the execution of server-side scripting. This could be a security hole in your configuration if users have access to the
 * database and can execute any kind of code.
 * 
 * @author Luca
 * 
 */
public class OServerSideScriptInterpreter extends OServerHandlerAbstract {
  private boolean enabled = false;

  @Override
  public void config(final OServer iServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      }
    }
  }

  @Override
  public String getName() {
    return "script-interpreter";
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    OLogManager.instance().info(this,
        "Installing Script interpreter. WARN: authenticated clients can execute any kind of code into the server.");

    // REGISTER THE SECURE COMMAND SCRIPT
    OCommandManager.instance().registerExecutor(OCommandScript.class, OCommandExecutorScript.class);
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OCommandManager.instance().unregisterExecutor(OCommandScript.class);
  }
}
