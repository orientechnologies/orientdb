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
import com.orientechnologies.orient.core.command.OScriptInterceptor;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Allow the execution of server-side scripting. This could be a security hole in your configuration
 * if users have access to the database and can execute any kind of code.
 *
 * @author Luca
 */
public class OServerSideScriptInterpreter extends OServerPluginAbstract {
  protected boolean enabled = false;
  protected Set<String> allowedLanguages = new HashSet<String>();

  protected OScriptInterceptor interceptor;
  private OServer server;

  @Override
  public void config(final OServer iServer, OServerParameterConfiguration[] iParams) {

    this.server = iServer;
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      } else if (param.name.equalsIgnoreCase("allowedLanguages")) {
        allowedLanguages =
            new HashSet<>(Arrays.asList(param.value.toLowerCase(Locale.ENGLISH).split(",")));
      } else if (param.name.equalsIgnoreCase("allowedPackages")) {
        OrientDBInternal.extract(iServer.getContext())
            .getScriptManager()
            .addAllowedPackages(new HashSet<>(Arrays.asList(param.value.split(","))));
      }
    }
  }

  @Override
  public String getName() {
    return "script-interpreter";
  }

  @Override
  public void startup() {

    if (!enabled) return;

    OrientDBInternal.extract(server.getContext())
        .getScriptManager()
        .getCommandManager()
        .registerExecutor(
            OCommandScript.class,
            OCommandExecutorScript.class,
            iArgument -> {
              final String language =
                  ((OCommandScript) iArgument).getLanguage().toLowerCase(Locale.ENGLISH);

              checkLanguage(language);
              return null;
            });

    interceptor =
        (db, language, script, params) -> {
          checkLanguage(language);
        };

    OrientDBInternal.extract(server.getContext())
        .getScriptManager()
        .getCommandManager()
        .getScriptExecutors()
        .entrySet()
        .forEach(e -> e.getValue().registerInterceptor(interceptor));
    OLogManager.instance()
        .warn(
            this,
            "Authenticated clients can execute any kind of code into the server by using the following allowed languages: "
                + allowedLanguages);
  }

  @Override
  public void shutdown() {
    if (!enabled) return;

    if (interceptor != null) {
      OrientDBInternal.extract(server.getContext())
          .getScriptManager()
          .getCommandManager()
          .getScriptExecutors()
          .entrySet()
          .forEach(e -> e.getValue().unregisterInterceptor(interceptor));
    }

    OrientDBInternal.extract(server.getContext())
        .getScriptManager()
        .getCommandManager()
        .unregisterExecutor(OCommandScript.class);
  }

  private void checkLanguage(final String language) {
    if (allowedLanguages.contains(language)) return;

    if ("js".equals(language) && allowedLanguages.contains("javascript")) return;

    throw new OSecurityException("Language '" + language + "' is not allowed to be executed");
  }
}
