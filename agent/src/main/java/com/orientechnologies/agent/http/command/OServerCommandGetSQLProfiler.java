/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Optional;

public class OServerCommandGetSQLProfiler extends OServerCommandDistributedScope {
  private static final String[]          NAMES = { "GET|sqlProfiler/*" };
  private final        OEnterpriseServer eeServer;

  public OServerCommandGetSQLProfiler(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_METRICS.toString(), server);

    this.eeServer = server;
  }

  @Override
  void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse) {
    throw new UnsupportedOperationException();
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
        proxyRequest(iRequest, iResponse);
      }

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts, String command)
      throws IOException, InterruptedException {
    String db = iRequest.getParameter("db");
    switch (command) {
    case "running":

      if (db != null) {
        iResponse.writeResult(eeServer.listQueries(Optional.of((c -> c.getDatabase().getName().equalsIgnoreCase(db)))));
      } else {
        iResponse.writeResult(eeServer.listQueries(Optional.empty()));
      }

      break;
    case "stats":

      final StringWriter jsonBuffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(jsonBuffer);
      json.append(Orient.instance().getProfiler().toJSON("realtime", "db." + db + ".command"));
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);
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
