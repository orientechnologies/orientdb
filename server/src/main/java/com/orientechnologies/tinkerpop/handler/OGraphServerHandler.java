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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.tinkerpop.command.OServerCommandPostCommandGremlin;
import org.apache.tinkerpop.gremlin.orientdb.executor.OCommandGremlinExecutor;

public class OGraphServerHandler extends OServerPluginAbstract {
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

    OCommandGremlinExecutor executor =
        (OCommandGremlinExecutor)
            OrientDBInternal.extract(server.getContext())
                .getScriptManager()
                .getCommandManager()
                .getScriptExecutor("gremlin");
    enabled = true;
    OLogManager.instance()
        .info(
            this,
            "Installed GREMLIN language v.%s - graph.pool.max=%d",
            executor.getEngineVersion(),
            graphPoolMax);

    this.server = server;
  }

  @Override
  public String getName() {
    return "graph";
  }

  @Override
  public void startup() {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

    if (!enabled) return;
    if (listener != null) listener.registerStatelessCommand(new OServerCommandPostCommandGremlin());
  }

  @Override
  public void shutdown() {
    if (!enabled) return;
  }

  @Override
  public void onAfterClientRequest(OClientConnection connection, byte requestType) {
    super.onAfterClientRequest(connection, requestType);
  }
}
