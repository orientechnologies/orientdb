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
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.command.OScriptInterceptor;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
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
public class OServerSideScriptInterpreter implements OServerPlugin {
  private static final OLogger logger =
      OLogManager.instance().logger(OServerSideScriptInterpreter.class);
  protected OScriptInterceptor interceptor;
  protected boolean enabled = true;
  private OrientDBInternal context;
  private OServerSideScriptInterpreterConfig config;
  
  
  @Override
  public void config(final OServer iServer, OServerParameterConfiguration[] iParams) {
    this.context = iServer.getDatabases();
    config = OServerSideScriptInterpreterConfig.fromParameters(iParams);
    context
    .getScriptManager()
    .addAllowedPackages(config.getAllowedPackages());
  }

  @Override
  public String getName() {
    return "script-interpreter";
  }

  @Override
  public void startup() {

    if (!enabled) return;

    interceptor =
        (db, language, script, params) -> {
          checkLanguage(language);
        };

    context
        .getScriptManager()
        .getScriptExecutors()
        .entrySet()
        .forEach(e -> e.getValue().registerInterceptor(interceptor));
    logger.warn(
        "Authenticated clients can execute any kind of code into the server by using the"
            + " following allowed languages: %s",
        config.getAllowedLanguages());
  }

  @Override
  public void shutdown() {
    if (!enabled) return;

    if (interceptor != null) {
      context
          .getScriptManager()
          .getScriptExecutors()
          .entrySet()
          .forEach(e -> e.getValue().unregisterInterceptor(interceptor));
    }
  }

  private void checkLanguage(final String language) {
    if (config.getAllowedLanguages().contains(language)) return;
    throw new OSecurityException("Language '" + language + "' is not allowed to be executed");
  }
}
