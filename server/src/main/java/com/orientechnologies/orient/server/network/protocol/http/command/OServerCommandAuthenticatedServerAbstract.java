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
package com.orientechnologies.orient.server.network.protocol.http.command;

import java.io.IOException;

import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

/**
 * Server based authenticated commands. Authenticates against the OrientDB server users found in configuration.
 *
 * @author Luca Garulli
 */
public abstract class OServerCommandAuthenticatedServerAbstract extends OServerCommandAbstract {

  private static final String SESSIONID_UNAUTHORIZED = "-";
  private static final String SESSIONID_LOGOUT       = "!";

  private final String        resource;
  protected String            serverUser;
  protected String            serverPassword;

  protected OServerCommandAuthenticatedServerAbstract(final String iRequiredResource) {
    resource = iRequiredResource;
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException {
    super.beforeExecute(iRequest, iResponse);
    return authenticate(iRequest, iResponse, true);
  }

  protected boolean authenticate(final OHttpRequest iRequest, final OHttpResponse iResponse, final boolean iAskForAuthentication)
      throws IOException {
    if (checkGuestAccess()) {
      // GUEST ACCESSES TO THE RESOURCE: OK ALSO WITHOUT AN AUTHENTICATION.
      iResponse.sessionId = null;
      return true;
    }

    if (iAskForAuthentication)
      if (iRequest.authorization == null || SESSIONID_LOGOUT.equals(iRequest.sessionId)) {
        // NO AUTHENTICATION AT ALL
        sendAuthorizationRequest(iRequest, iResponse);
        return false;
      }

    if (iRequest.authorization != null) {
      // GET CREDENTIALS
      final String[] authParts = iRequest.authorization.split(":");
      if (authParts.length != 2) {
        // NO USER : PASSWD
        sendAuthorizationRequest(iRequest, iResponse);
        return false;
      }

      serverUser = authParts[0];
      serverPassword = authParts[1];
      if (authParts.length == 2 && server.authenticate(serverUser, serverPassword, resource))
        // AUTHORIZED
        return true;
    }

    // NON AUTHORIZED FOR RESOURCE
    sendNotAuthorizedResponse(iRequest, iResponse);
    return false;
  }

  protected boolean checkGuestAccess() {
    return server.isAllowed(OServerConfiguration.SRV_ROOT_GUEST, resource);
  }

  protected void sendNotAuthorizedResponse(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException {
    sendAuthorizationRequest(iRequest, iResponse);
  }

  protected void sendAuthorizationRequest(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException {
    // UNAUTHORIZED
    iRequest.sessionId = SESSIONID_UNAUTHORIZED;
    if (isJsonResponse(iResponse)) {
      sendJsonError(iResponse, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", "WWW-Authenticate: Basic realm=\"OrientDB Server\"");
    } else {
      iResponse.send(OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", "WWW-Authenticate: Basic realm=\"OrientDB Server\"");
    }
  }
}
