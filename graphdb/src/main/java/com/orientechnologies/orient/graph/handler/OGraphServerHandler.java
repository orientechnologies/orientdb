/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.graph.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.script.OScriptGraphOrientWrapper;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import javax.script.Bindings;

public class OGraphServerHandler extends OServerPluginAbstract implements OScriptInjection {
  private boolean enabled      = true;
  private int     graphPoolMax = OGlobalConfiguration.DB_POOL_MAX.getValueAsInteger();

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.equalsIgnoreCase("graph.pool.max"))
        graphPoolMax = Integer.parseInt(param.value);
    }

    enabled = true;
    OLogManager.instance().info(this, "Installing GREMLIN language v.%s - graph.pool.max=%d", OGremlinHelper.getEngineVersion(),
        graphPoolMax);

    Orient.instance().getScriptManager().registerInjection(this);
  }

  @Override
  public String getName() {
    return "graph";
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    OGremlinHelper.global().setMaxGraphPool(graphPoolMax).create();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OGremlinHelper.global().destroy();
  }

  @Override
  public void bind(Bindings binding) {
    Object scriptGraph = binding.get("orient");
    if (scriptGraph == null || !(scriptGraph instanceof OScriptGraphOrientWrapper))
      binding.put("orient", new OScriptGraphOrientWrapper());
  }

  @Override
  public void unbind(Bindings binding) {
    binding.put("orient", null);
  }

  @Override
  public void onAfterClientRequest(OClientConnection connection, byte requestType) {
    super.onAfterClientRequest(connection, requestType);

    OrientBaseGraph.clearInitStack();
  }
}
