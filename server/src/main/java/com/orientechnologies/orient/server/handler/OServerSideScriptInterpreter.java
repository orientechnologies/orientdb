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
package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Allow the execution of server-side scripting. This could be a security hole in your configuration if users have access to the
 * database and can execute any kind of code.
 * 
 * @author Luca
 * 
 */
public class OServerSideScriptInterpreter extends OServerPluginAbstract {
  protected boolean     enabled          = false;
  protected Set<String> allowedLanguages = new HashSet<String>();

  @Override
  public void config(final OServer iServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      } else if (param.name.equalsIgnoreCase("allowedLanguages")) {
        allowedLanguages = new HashSet<String>(Arrays.asList(param.value.toLowerCase(Locale.ENGLISH).split(",")));
      }
    }
  }

  @Override
  public String getName() {
    return "script-interpreter";
  }

  @Override
  public void startup() {
    OCommandManager.instance().unregisterExecutor(OCommandScript.class);

    if (!enabled)
      return;

    OCommandManager.instance().registerExecutor(OCommandScript.class, OCommandExecutorScript.class,
        new OCallable<Void, OCommandRequest>() {
          @Override
          public Void call(OCommandRequest iArgument) {
            final String language = ((OCommandScript) iArgument).getLanguage().toLowerCase(Locale.ENGLISH);

            if (!allowedLanguages.contains(language))
              throw new OSecurityException("Language '" + language + "' is not allowed to be executed");

            return null;
          }
        });

    OLogManager.instance().warn(this,
        "Authenticated clients can execute any kind of code into the server by using the following allowed languages: "
            + allowedLanguages);
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OCommandManager.instance().unregisterExecutor(OCommandScript.class);
  }
}
