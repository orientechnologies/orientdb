/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *        http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginConfigurable;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by Enrico Risa on 18/11/15.
 */
public class OServerCommandPluginManager extends OServerCommandDistributedScope {
  private static final String[] NAMES = { "GET|plugins", "GET|plugins/*", "PUT|plugins/*" };

  public OServerCommandPluginManager() {
    super(EnterprisePermissions.SERVER_PLUGINS.toString());
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: plugins");

    if (isLocalNode(iRequest)) {

      if ("GET".equalsIgnoreCase(iRequest.httpMethod)) {
        if (parts.length == 1) {
          doGetPlugins(iResponse);
        }
        if (parts.length == 2) {
          doGetPlugin(iResponse, parts[1]);
        }
      } else if ("PUT".equalsIgnoreCase(iRequest.httpMethod)) {
        if (parts.length == 2) {
          doUpdatePluginCfg(iRequest, iResponse, parts[1]);
        }
      }

    } else {
      proxyRequest(iRequest, iResponse);
    }
    return false;
  }

  private void doGetPlugin(OHttpResponse iResponse, String name) throws IOException {

    OServerPluginInfo pluginByName = server.getPluginManager().getPluginByName(name);

    if (pluginByName == null) {
      throw new IllegalArgumentException("Cannot find plugin with name: " + name);
    }
    OServerPlugin instance = pluginByName.getInstance();

    if (instance instanceof OServerPluginConfigurable) {

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, ((OServerPluginConfigurable) instance).getConfig()
          .toJSON("prettyPrint"), null);
    } else {
      throw new IllegalArgumentException("Plugin with name: " + name + " is not configurable.");
    }
  }

  private void doUpdatePluginCfg(OHttpRequest iRequest, OHttpResponse iResponse, String pluginName) throws IOException {

    OServerPluginInfo pluginByName = server.getPluginManager().getPluginByName(pluginName);

    if (pluginByName == null) {
      throw new IllegalArgumentException("Cannot find plugin with name: " + pluginName);
    }
    OServerPlugin instance = pluginByName.getInstance();

    if (instance instanceof OServerPluginConfigurable) {
      ODocument newCfg = new ODocument().fromJSON(iRequest.content);
      ((OServerPluginConfigurable) instance).changeConfig(newCfg);

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, newCfg.toJSON("prettyPrint"), null);

    } else {
      throw new IllegalArgumentException("Plugin with name: " + pluginName + " cannot be configured.");
    }
  }

  private void doGetPlugins(OHttpResponse iResponse) throws IOException {
    ODocument document = new ODocument();

    Collection<ODocument> documents = new ArrayList<ODocument>();
    document.field("plugins", documents);
    Collection<OServerPluginInfo> plugins = server.getPluginManager().getPlugins();
    for (OServerPluginInfo plugin : plugins) {
      OServerPlugin instance = plugin.getInstance();

      if (instance instanceof OServerPluginConfigurable) {
        ODocument p = new ODocument();
        p.field("name", plugin.getName());
        p.field("description", plugin.getDescription());
        p.field("startedOn", plugin.getLoadedOn());
        p.field("configuration", ((OServerPluginConfigurable) instance).getConfig());

        documents.add(p);
      }
    }

    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, document.toJSON("prettyPrint"), null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
