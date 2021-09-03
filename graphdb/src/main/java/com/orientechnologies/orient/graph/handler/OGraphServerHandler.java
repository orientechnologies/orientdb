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

package com.orientechnologies.orient.graph.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlinExecutor;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.script.OScriptGraphOrientWrapper;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import javax.script.Bindings;
import javax.script.ScriptEngine;

public class OGraphServerHandler extends OServerPluginAbstract implements OScriptInjection {
  private boolean enabled = true;
  private int graphPoolMax;
  private OServer server;

  @Override
  public void config(final OServer server, OServerParameterConfiguration[] iParams) {
    graphPoolMax =
        server.getContextConfiguration().getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX);
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.equalsIgnoreCase("graph.pool.max"))
        graphPoolMax = Integer.parseInt(param.value);
    }

    if (OGremlinHelper.isGremlinAvailable()) {
      enabled = true;
      OLogManager.instance()
          .info(
              this,
              "Installed GREMLIN language v.%s - graph.pool.max=%d",
              OGremlinHelper.getEngineVersion(),
              graphPoolMax);

      OrientDBInternal.extract(server.getContext()).getScriptManager().registerInjection(this);

      OrientDBInternal.extract(server.getContext())
          .getScriptManager()
          .getCommandManager()
          .registerRequester("gremlin", OCommandGremlin.class);
      OrientDBInternal.extract(server.getContext())
          .getScriptManager()
          .getCommandManager()
          .registerExecutor(OCommandGremlin.class, OCommandGremlinExecutor.class);
    } else enabled = false;

    this.server = server;
  }

  @Override
  public String getName() {
    return "graph";
  }

  @Override
  public void startup() {

    if (!enabled) return;

    OGremlinHelper.global().setMaxGraphPool(graphPoolMax).create();
  }

  @Override
  public void shutdown() {
    if (!enabled) return;

    OGremlinHelper.global().destroy();
  }

  @Override
  public void bind(ScriptEngine engine, Bindings binding, ODatabaseDocument database) {
    Object scriptGraph = binding.get("orient");
    if (scriptGraph == null || !(scriptGraph instanceof OScriptGraphOrientWrapper))
      binding.put("orient", new OScriptGraphOrientWrapper());
  }

  @Override
  public void unbind(ScriptEngine engine, Bindings binding) {
    binding.put("orient", null);
  }

  @Override
  public void onAfterClientRequest(OClientConnection connection, byte requestType) {
    super.onAfterClientRequest(connection, requestType);

    OrientBaseGraph.clearInitStack();
  }
}
