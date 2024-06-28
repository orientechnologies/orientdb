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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.io.IOException;

public class OServerCommandGetSecurityConfig extends OServerCommandAuthenticatedServerAbstract {
  private static final OLogger logger =
      OLogManager.instance().logger(OServerCommandGetSecurityConfig.class);
  private static final String[] NAMES = {"GET|security/config"};

  private OSecuritySystem serverSecurity;

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public OServerCommandGetSecurityConfig(OSecuritySystem serverSec) {
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
      logger.error("%s %s", null, method, reason);

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
      logger.error("OServerCommandGetSecurityConfig.writeJSON() ", ex);
    }
  }

  protected void writeJSON(final OHttpResponse iResponse, final String json) {
    try {
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, json, null);
    } catch (IOException ex) {
      logger.error("OServerCommandGetSecurityConfig.writeJSON()", ex);
    }
  }
}
