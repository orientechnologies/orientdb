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

package com.orientechnologies.tinkerpop.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.tinkerpop.command.OServerCommandPostCommandGremlinLoader;
import org.apache.tinkerpop.gremlin.orientdb.executor.OCommandGremlinExecutor;

public class OGraphServerHandler implements OServerPlugin {
  private static final OLogger logger = OLogManager.instance().logger(OGraphServerHandler.class);
  private OServer server;
  private OGraphServerHandlerConfig config;

  public OGraphServerHandler() {}

  public OGraphServerHandler(OServer context, OGraphServerHandlerConfig config) {
    this.config = config;
    this.server = context;
    OCommandGremlinExecutor executor =
        (OCommandGremlinExecutor)
            context.getDatabases().getScriptManager().getScriptExecutor("gremlin");
    logger.info("Installed GREMLIN language v.%s ", executor.getEngineVersion());
  }

  @Override
  public void config(final OServer server, OServerParameterConfiguration[] iParams) {
    config = OGraphServerHandlerConfig.fromParameters(iParams);
    OCommandGremlinExecutor executor =
        (OCommandGremlinExecutor)
            server.getDatabases().getScriptManager().getScriptExecutor("gremlin");
    logger.info("Installed GREMLIN language v.%s ", executor.getEngineVersion());

    this.server = server;
  }

  @Override
  public String getName() {
    return "graph";
  }

  @Override
  public void startup() {
    if (!config.isEnabled()) return;
    for (var listener : server.getHttpListeners()) {
      listener.registerStatelessCommand(new OServerCommandPostCommandGremlinLoader());
    }
  }

  @Override
  public void shutdown() {
    if (!config.isEnabled()) return;
  }
}
