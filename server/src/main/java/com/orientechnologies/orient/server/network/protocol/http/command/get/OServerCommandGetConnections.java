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
import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.StringWriter;

public class OServerCommandGetConnections extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|connections/*" };

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

      OServerInfo.getConnections(server, json, databaseName);

      json.endObject();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);

    } finally {
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
