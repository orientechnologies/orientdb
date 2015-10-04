/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class OServerCommandGetConnections extends OServerCommandAuthenticatedServerAbstract {
  protected final static DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final String[]     NAMES          = { "GET|connections/*" };

  public OServerCommandGetConnections() {
    super("server.connections");
  }

  public OServerCommandGetConnections(final String iName) {
    super(iName);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] args = checkSyntax(iRequest.url, 1, "Syntax error: connections[/<database>]");

    iRequest.data.commandInfo = "Server status";

    try {
      final StringWriter jsonBuffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(jsonBuffer);
      json.beginObject();

      final String databaseName = args.length > 1 && args[1].length() > 0 ? args[1] : null;

      writeConnections(json, databaseName);

      json.endObject();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);

    } finally {
    }
    return false;
  }

  protected void writeConnections(final OJSONWriter json, final String databaseName) throws IOException {
    json.beginCollection(1, true, "connections");

    final List<OClientConnection> conns = server.getClientConnectionManager().getConnections();
    for (OClientConnection c : conns) {
      final ONetworkProtocolData data = c.data;

      if (databaseName != null && !databaseName.equals((data.lastDatabase)))
        // SKIP IT
        continue;

      final String lastCommandOn;
      final String connectedOn;

      synchronized (dateTimeFormat) {
        lastCommandOn = dateTimeFormat.format(new Date(data.lastCommandReceived));
        connectedOn = dateTimeFormat.format(new Date(c.since));
      }

      json.beginObject(2);
      writeField(json, 2, "connectionId", c.id);
      writeField(json, 2, "remoteAddress", c.protocol.getChannel() != null ? c.protocol.getChannel().toString() : "Disconnected");
      writeField(json, 2, "db", data.lastDatabase != null ? data.lastDatabase : "-");
      writeField(json, 2, "user", data.lastUser != null ? data.lastUser : "-");
      writeField(json, 2, "totalRequests", data.totalRequests);
      writeField(json, 2, "commandInfo", data.commandInfo);
      writeField(json, 2, "commandDetail", data.commandDetail);
      writeField(json, 2, "lastCommandOn", lastCommandOn);
      writeField(json, 2, "lastCommandInfo", data.lastCommandInfo);
      writeField(json, 2, "lastCommandDetail", data.lastCommandDetail);
      writeField(json, 2, "lastExecutionTime", data.lastCommandExecutionTime);
      writeField(json, 2, "totalWorkingTime", data.totalCommandExecutionTime);
      writeField(json, 2, "connectedOn", connectedOn);
      writeField(json, 2, "protocol", c.protocol.getType());
      writeField(json, 2, "clientId", data.clientId);

      final StringBuilder driver = new StringBuilder(128);
      if (data.driverName != null) {
        driver.append(data.driverName);
        driver.append(" v");
        driver.append(data.driverVersion);
        driver.append(" Protocol v");
        driver.append(data.protocolVersion);
      }

      writeField(json, 2, "driver", driver.toString());
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private void writeField(final OJSONWriter json, final int iLevel, final String iAttributeName, final Object iAttributeValue)
      throws IOException {
    json.writeAttribute(iLevel, true, iAttributeName, iAttributeValue != null ? iAttributeValue.toString() : "-");
  }
}
