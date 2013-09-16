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
package com.orientechnologies.orient.monitor.http;

import java.net.URL;

import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.monitor.OMonitorUtils;
import com.orientechnologies.orient.monitor.OMonitoredServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetConnectionsCommand extends OServerCommandAbstract {
  private static final String[] NAMES = { "GET|connections/*" };

  private OMonitorPlugin        monitor;

  public OServerCommandGetConnectionsCommand(final OServerCommandConfiguration iConfiguration) {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OMonitorPlugin.class);

    final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: connections/monitor/<server>[/<database>]");

    iRequest.data.commandInfo = "Server information command";

    final String serverName = parts[2];
    final String dbName = parts.length > 3 ? parts[3] : "";

    try {
      final OMonitoredServer server = monitor.getMonitoredServer(serverName);

      final URL remoteUrl = new java.net.URL("http://" + server.getConfiguration().field("url") + "/connections/" + dbName);

      final String result = OMonitorUtils.fetchFromRemoteServer(server.getConfiguration(), remoteUrl);

      iResponse.writeResult(result);

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
