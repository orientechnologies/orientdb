/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandDeleteConnection extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "DELETE|connection/*" };

  public OServerCommandDeleteConnection() {
    super("server.connection.kill");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: connection/<id>");

    iRequest.data.commandInfo = "Kill connection";
    iRequest.data.commandDetail = urlParts[1];

    OClientConnectionManager.instance().disconnect(Integer.parseInt(urlParts[1]));

    iResponse.send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
        null, null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
