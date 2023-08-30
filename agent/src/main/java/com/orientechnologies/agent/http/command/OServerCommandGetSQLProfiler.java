/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.util.Optional;

public class OServerCommandGetSQLProfiler extends OServerCommandDistributedScope {
  private static final String[] NAMES = {"GET|sqlProfiler/*"};
  private final OEnterpriseServer eeServer;

  public OServerCommandGetSQLProfiler(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_METRICS.toString(), server);

    this.eeServer = server;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: sqlProfiler/<command>");

    iRequest.getData().commandInfo = "Profiler information";

    try {

      if (isLocalNode(iRequest)) {
        final String db = parts[1];
        if ("GET".equalsIgnoreCase(iRequest.getHttpMethod())) {
          doGet(iRequest, iResponse, parts, db);
        }
      } else {
        proxyRequest(iRequest, null);
      }

    } catch (Exception e) {
      iResponse.send(
          OHttpUtils.STATUS_BADREQ_CODE,
          OHttpUtils.STATUS_BADREQ_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          e,
          null);
    }
    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts, String command)
      throws IOException, InterruptedException {
    String db = iRequest.getParameter("db");
    switch (command) {
      case "running":
        if (db != null) {
          iResponse.writeResult(
              eeServer.listQueries(
                  Optional.of((c -> c.getDatabase().getName().equalsIgnoreCase(db)))));
        } else {
          iResponse.writeResult(eeServer.listQueries(Optional.empty()));
        }

        break;
      case "stats":
        Optional database = Optional.ofNullable(db);
        iResponse.writeResult(eeServer.getQueryStats(database));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
