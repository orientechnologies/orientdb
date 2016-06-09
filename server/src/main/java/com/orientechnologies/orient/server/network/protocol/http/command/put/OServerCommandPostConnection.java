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
package com.orientechnologies.orient.server.network.protocol.http.command.put;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandPostConnection extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "POST|connection/*" };

  public OServerCommandPostConnection() {
    super("server.connection");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: connection/<command>/<id>");

    iRequest.data.commandInfo = "Interrupt command";
    iRequest.data.commandDetail = urlParts[1];

    if ("KILL".equalsIgnoreCase(urlParts[1]))
      server.getClientConnectionManager().kill(Integer.parseInt(urlParts[2]));
    else if ("INTERRUPT".equalsIgnoreCase(urlParts[1]))
      server.getClientConnectionManager().interrupt(Integer.parseInt(urlParts[2]));
    else
      throw new IllegalArgumentException("Connection command '" + urlParts[1] + "' is unknown. Supported are: kill, interrupt");

    iResponse.send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
        null, null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
