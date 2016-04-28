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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import com.orientechnologies.orient.server.security.OServerSecurity;

import java.io.IOException;

public class OServerCommandPostSecurityReload extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "POST|security/reload" };

  private OServerSecurity       serverSecurity;

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public OServerCommandPostSecurityReload(OServerSecurity serverSec) {
    super("*");

    serverSecurity = serverSec;
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException {
    return authenticate(iRequest, iResponse, false);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    if (iRequest.content == null) {
      writeError(iResponse, "OServerCommandPostSecurityReload.execute()", "Request Content is null");
      return false;
    }

    if (serverSecurity == null) {
      writeError(iResponse, "OServerCommandPostSecurityReload.execute()", "ServerSecurity is null");
      return false;
    }

    try {
      // Convert the JSON content to an ODocument to make parsing it easier.
      final ODocument jsonParams = new ODocument().fromJSON(iRequest.content, "noMap");

      // "configFile" and "config"/"module" are mutually exclusive properties.
      if (jsonParams.containsField("configFile")) {
        final String configName = OSystemVariableResolver.resolveSystemVariables((String) jsonParams.field("configFile"));

        OLogManager.instance().info(this, "OServerCommandPostSecurityReload.execute() configName = %s", configName);

        serverSecurity.reload(configName);
      } else if (jsonParams.containsField("config")) {
        final ODocument jsonDoc = jsonParams.field("config");

        if (jsonParams.containsField("module")) {
          final String compName = jsonParams.field("module");

          serverSecurity.reloadComponent(compName, jsonDoc);
        } else {
          serverSecurity.reload(jsonDoc);
        }
      } else {
        writeError(iResponse, "OServerCommandPostSecurityReload.execute()", "/security/reload keyword is missing");
        return false;
      }
    } catch (Exception ex) {
      writeError(iResponse, "OServerCommandPostSecurityReload.execute()", "Exception: " + ex.getMessage());
      return false;
    }

    writeJSON(iResponse, "Configuration loaded successfully");

    return false;
  }

  protected void writeError(final OHttpResponse iResponse, final String method, final String reason) {
    try {
      OLogManager.instance().error(this, "%s %s", method, reason);

      final StringBuilder json = new StringBuilder();

      json.append("{ \"Status\" : \"Error\", \"Reason\" : \"");
      json.append(reason);
      json.append("\" }");

      iResponse.send(OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Error", OHttpUtils.CONTENT_JSON, json.toString(), null);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OServerCommandPostSecurityReload.WriteJSON() Exception: " + ex);
    }
  }

  protected void writeJSON(final OHttpResponse iResponse, final String json) {
    try {
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, json, null);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OServerCommandPostSecurityReload.WriteJSON() Exception: " + ex);
    }
  }
}
