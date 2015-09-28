/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.IOException;
import java.util.List;

public class OServerCommandPostKillDbConnection extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "POST|dbconnection/*" };

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: dbconnection/<database>");

    doPost(iRequest, iResponse, urlParts[1], iRequest.content);

    return false;
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String db, String command) throws IOException {

    final List<OClientConnection> connections = server.getClientConnectionManager().getConnections();
    for (OClientConnection connection : connections) {
      if (connection.getProtocol() instanceof ONetworkProtocolHttpAbstract) {
        final ONetworkProtocolHttpAbstract http = (ONetworkProtocolHttpAbstract) connection.getProtocol();
        final OHttpRequest req = http.getRequest();

        if (req != null && req != iRequest && req.sessionId.equals(iRequest.sessionId)) {
          server.getClientConnectionManager().interrupt(connection.id);
        }
      }
    }
    iResponse.send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
        null, null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
