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
package com.orientechnologies.workbench.http;

import java.net.URL;
import java.net.URLEncoder;

import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;
import com.orientechnologies.workbench.OWorkbenchUtils;

public class OServerCommandGetExplainCommand extends OServerCommandAbstract {
  private static final String[] NAMES = { "GET|explainCommand/*" };

  private OWorkbenchPlugin      monitor;

  public OServerCommandGetExplainCommand() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    final String[] parts = checkSyntax(iRequest.url, 6,
        "Syntax error: explainCommand/monitor/<server>/<database>/<language>/<command>");

    iRequest.data.commandInfo = "Explain command";

    final String serverName = parts[2];
    final String databasee = parts[3];
    final String language = parts[4];
    final String command = parts[5];

    try {
      final OMonitoredServer server = monitor.getMonitoredServer(serverName);

      final URL remoteUrl = new java.net.URL("http://" + server.getConfiguration().field("url") + "/command/" + databasee + "/"
          + language + "/" + URLEncoder.encode("explain " + command, "UTF-8"));

      final String result = OWorkbenchUtils.fetchFromRemoteServer(server.getConfiguration(), remoteUrl, "POST");

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
