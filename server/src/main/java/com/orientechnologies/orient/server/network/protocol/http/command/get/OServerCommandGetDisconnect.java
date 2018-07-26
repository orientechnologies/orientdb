/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.IOException;

public class OServerCommandGetDisconnect extends OServerCommandAbstract {
  private static final String[] NAMES = { "GET|disconnect" };

  @Override
  public boolean beforeExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    super.beforeExecute(iRequest, iResponse);
    return true;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 1, "Syntax error: disconnect");

    iRequest.data.commandInfo = "Disconnect";
    iRequest.data.commandDetail = null;

    if (iRequest.sessionId != null) {
      server.getHttpSessionManager().removeSession(iRequest.sessionId);
      iRequest.sessionId = OServerCommandAuthenticatedDbAbstract.SESSIONID_UNAUTHORIZED;
      iResponse.setSessionId(iRequest.sessionId);
    }

    iResponse.keepAlive = false;

    if (isJsonResponse(iResponse)) {
      sendJsonError(iResponse, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "Logged out", null);
    } else {
      iResponse.send(OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, "Logged out",
          null);
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
