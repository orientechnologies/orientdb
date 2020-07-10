/*
 * Copyright 2010-2016 OrientDB LTD
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
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import com.orientechnologies.orient.server.security.OServerSecurity;
import java.io.IOException;

public class OServerCommandGetSecurityConfig extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = {"GET|security/config"};

  private OServerSecurity serverSecurity;

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public OServerCommandGetSecurityConfig(OServerSecurity serverSec) {
    super(EnterprisePermissions.SERVER_SECURITY.toString());

    serverSecurity = serverSec;
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws IOException {
    return authenticate(iRequest, iResponse, false);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    if (serverSecurity == null) {
      writeError(iResponse, "OServerCommandGetSecurityConfig.execute()", "ServerSecurity is null");
      return false;
    }

    try {
      ODocument configDoc = null;

      // If the content is null then we return the main security configuration.
      if (iRequest.getContent() == null) {
        configDoc = serverSecurity.getConfig();
      } else {
        // Convert the JSON content to an ODocument to make parsing it easier.
        final ODocument jsonParams = new ODocument().fromJSON(iRequest.getContent(), "noMap");

        if (jsonParams.containsField("module")) {
          final String compName = jsonParams.field("module");

          configDoc = serverSecurity.getComponentConfig(compName);
        }
      }

      if (configDoc != null) {
        final String json = configDoc.toJSON("alwaysFetchEmbedded");

        writeJSON(iResponse, json);
      } else {
        writeError(
            iResponse,
            "OServerCommandGetSecurityConfig.execute()",
            "Unable to retrieve configuration");
      }
    } catch (Exception ex) {
      writeError(
          iResponse, "OServerCommandGetSecurityConfig.execute()", "Exception: " + ex.getMessage());
    }

    return false;
  }

  protected void writeError(
      final OHttpResponse iResponse, final String method, final String reason) {
    try {
      OLogManager.instance().error(this, "%s %s", null, method, reason);

      final StringBuilder json = new StringBuilder();

      json.append("{ \"Status\" : \"Error\", \"Reason\" : \"");
      json.append(reason);
      json.append("\" }");

      iResponse.send(
          OHttpUtils.STATUS_INVALIDMETHOD_CODE,
          "Error",
          OHttpUtils.CONTENT_JSON,
          json.toString(),
          null);
    } catch (IOException ex) {
      OLogManager.instance().error(this, "OServerCommandGetSecurityConfig.writeJSON() ", ex);
    }
  }

  protected void writeJSON(final OHttpResponse iResponse, final String json) {
    try {
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, json, null);
    } catch (IOException ex) {
      OLogManager.instance().error(this, "OServerCommandGetSecurityConfig.writeJSON()", ex);
    }
  }
}
