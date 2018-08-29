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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.StringWriter;

public class OServerCommandGetSQLProfiler extends OServerCommandDistributedScope {
  private static final String[] NAMES = { "GET|sqlProfiler/*", "POST|sqlProfiler/*" };

  public OServerCommandGetSQLProfiler() {
    super(EnterprisePermissions.SERVER_PROFILER.toString());
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: sqlProfiler/<command>/[<config>]|[<from>]");

    iRequest.data.commandInfo = "Profiler information";

    try {

      if (isLocalNode(iRequest)) {
        final String db = parts[1];
        if ("GET".equalsIgnoreCase(iRequest.httpMethod)) {
          final StringWriter jsonBuffer = new StringWriter();
          final OJSONWriter json = new OJSONWriter(jsonBuffer);
          json.append(Orient.instance().getProfiler().toJSON("realtime", "db." + db + ".command"));

          iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);
        } else if ("POST".equalsIgnoreCase(iRequest.httpMethod)) {
          Orient.instance().getProfiler().resetRealtime("db." + db + ".command");
          iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
        }
      } else {
        proxyRequest(iRequest, iResponse);
      }

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
