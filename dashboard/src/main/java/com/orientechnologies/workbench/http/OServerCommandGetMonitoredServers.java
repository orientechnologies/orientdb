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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;

public class OServerCommandGetMonitoredServers extends OServerCommandAbstract {
  private static final String[] NAMES = { "GET|monitoredServers/*" };

  private OWorkbenchPlugin      monitor;

  public OServerCommandGetMonitoredServers() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    iRequest.data.commandInfo = "Retrieve monitored servers";

    try {

      List<ODocument> results = new ArrayList<ODocument>();
      Set<Entry<String, OMonitoredServer>> monitoredServers = this.monitor.getMonitoredServers();

      for (Entry<String, OMonitoredServer> s : monitoredServers) {
        ODocument r = new ODocument();
        r.field("id", s.getValue().getConfiguration().getIdentity());
        r.field("name", s.getValue().getConfiguration().field("name"));
        Map<String, Object> metric = s.getValue().getRealtime().getInformation("system.databases");
        r.field("databases", metric.get("system.databases"));
        results.add(r);
      }

      iResponse.writeResult(results, "indent:6", null);

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
