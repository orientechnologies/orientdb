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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;
import com.orientechnologies.workbench.OWorkbenchPurgeMetricLogHelper;

public class OServerCommandDeleteServer extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "DELETE|monitoredServer/*" };

  private OWorkbenchPlugin      monitor;

  public OServerCommandDeleteServer() {
  }

  public OServerCommandDeleteServer(final OServerCommandConfiguration iConfiguration) {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: monitoredServer/database/<name>");

    iRequest.data.commandInfo = "Reset metrics";

    try {

      final String serverName = parts[2];
      final OMonitoredServer server = monitor.getMonitoredServer(serverName);
      if (server == null)
        throw new IllegalArgumentException("Invalid server '" + serverName + "'");

      ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
      OWorkbenchPurgeMetricLogHelper.purgeLogsNow(server.getConfiguration(), db);
      OWorkbenchPurgeMetricLogHelper.purgeMetricNow(server.getConfiguration(), db);
      OWorkbenchPurgeMetricLogHelper.purgeConfigNow(server.getConfiguration(), db);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
      server.getConfiguration().delete();
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
      iResponse.writeResult(result, "indent:6", null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
