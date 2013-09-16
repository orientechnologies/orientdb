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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.monitor.OMonitoredServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandDeleteRealtimeMetrics extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "DELETE|metrics/*" };

  private OMonitorPlugin        monitor;

  public OServerCommandDeleteRealtimeMetrics(final OServerCommandConfiguration iConfiguration) {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OMonitorPlugin.class);

    final String[] parts = checkSyntax(iRequest.url, 5, "Syntax error: metrics/monitor/<server>/<type>/<names>");

    iRequest.data.commandInfo = "Reset metrics";

    try {

      final String serverName = parts[2];
      final String type = parts[3];
      final String[] metricNames = parts[4].split(",");

      final OMonitoredServer server = monitor.getMonitoredServer(serverName);
      if (server == null)
        throw new IllegalArgumentException("Invalid server '" + serverName + "'");

      final Map<String, Object> result = new HashMap<String, Object>();

      if ("realtime".equalsIgnoreCase(type))
        clearRealtimeMetrics(iResponse, null, metricNames, server, result);
      else if ("snapshot".equalsIgnoreCase(type))
        throw new UnsupportedOperationException("snapshot is not implemented yet");

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  protected void clearRealtimeMetrics(OHttpResponse iResponse, final String iMetricKind, final String[] metricNames,
      final OMonitoredServer server, final Map<String, Object> result) throws MalformedURLException, IOException,
      InterruptedException {
    for (String metricName : metricNames)
      server.getRealtime().reset(metricName);

    if (result != null)
      iResponse.writeResult(result, "indent:6");
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
