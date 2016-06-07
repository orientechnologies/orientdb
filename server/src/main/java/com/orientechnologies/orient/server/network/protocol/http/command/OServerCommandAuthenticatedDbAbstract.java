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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.network.protocol.http.*;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;

/**
 * Database based authenticated command. Authenticates against the database taken as second parameter of the URL. The URL must be in
 * this format:
 *
 * <pre>
 * <command>/<database>[/...]
 * </pre>
 *
 * @author Luca Garulli
 *
 */
public abstract class OServerCommandAuthenticatedDbAbstract extends OServerCommandAbstract {

  public static final char       DBNAME_DIR_SEPARATOR   = '$';
  public static final String     SESSIONID_UNAUTHORIZED = "-";
  public static final String     SESSIONID_LOGOUT       = "!";
  private volatile OTokenHandler tokenHandler;

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    super.beforeExecute(iRequest, iResponse);

    init();

    final String[] urlParts = iRequest.url.substring(1).split("/");
    if (urlParts.length < 2)
      throw new OHttpRequestException("Syntax error in URL. Expected is: <command>/<database>[/...]");

    iRequest.databaseName = URLDecoder.decode(urlParts[1],"UTF-8");
    if (iRequest.bearerTokenRaw != null) {
      // Bearer authentication
      try {
        iRequest.bearerToken = tokenHandler.parseWebToken(iRequest.bearerTokenRaw.getBytes());
      } catch (Exception e) {
        // TODO: Catch all expected exceptions correctly!
        OLogManager.instance().warn(this, "Bearer token parsing failed", e);
      }

      if (iRequest.bearerToken == null || iRequest.bearerToken.getIsVerified() == false) {
        // Token parsing or verification failed - for now fail silently.
        sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
        return false;
      }

      // CHECK THE REQUEST VALIDITY
      tokenHandler.validateToken(iRequest.bearerToken, urlParts[0], urlParts[1]);
      if (iRequest.bearerToken.getIsValid() == false) {

        // SECURITY PROBLEM: CROSS DATABASE REQUEST!
        OLogManager.instance().warn(this, "Token '%s' is not valid for database '%s'", iRequest.bearerTokenRaw,
            iRequest.databaseName);
        sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
        return false;

      }

      return iRequest.bearerToken.getIsValid();
    } else {
      // HTTP basic authentication
      final List<String> authenticationParts = iRequest.authorization != null ? OStringSerializerHelper.split(
          iRequest.authorization, ':') : null;

      OHttpSession currentSession;
      if (iRequest.sessionId != null && iRequest.sessionId.length() > 1) {
        currentSession = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);
        if (currentSession != null && authenticationParts != null) {
          if (!currentSession.getUserName().equals(authenticationParts.get(0))) {
            // CHANGED USER, INVALIDATE THE SESSION
            currentSession = null;
          }
        }
      } else
        currentSession = null;

      if (currentSession == null) {
        // NO SESSION
        if (iRequest.authorization == null || SESSIONID_LOGOUT.equals(iRequest.sessionId)) {
          iResponse.setSessionId(SESSIONID_UNAUTHORIZED);
          sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
          return false;
        } else
          return authenticate(iRequest, iResponse, authenticationParts, iRequest.databaseName);

      } else {
        // CHECK THE SESSION VALIDITY
        if (!currentSession.getDatabaseName().equals(iRequest.databaseName)) {

          // SECURITY PROBLEM: CROSS DATABASE REQUEST!
          OLogManager.instance().warn(this,
              "Session %s is trying to access to the database '%s', but has been authenticated against the database '%s'",
              iRequest.sessionId, iRequest.databaseName, currentSession.getDatabaseName());
          OHttpSessionManager.getInstance().removeSession(iRequest.sessionId);
          sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
          return false;

        } else if (authenticationParts != null && !currentSession.getUserName().equals(authenticationParts.get(0))) {

          // SECURITY PROBLEM: CROSS DATABASE REQUEST!
          OLogManager.instance().warn(this,
              "Session %s is trying to access to the database '%s' with user '%s', but has been authenticated with user '%s'",
              iRequest.sessionId, iRequest.databaseName, authenticationParts.get(0), currentSession.getUserName());
          OHttpSessionManager.getInstance().removeSession(iRequest.sessionId);
          sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
          return false;
        }

        return true;
      }
    }
  }

  @Override
  public boolean afterExecute(final OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    return true;
  }

  protected boolean authenticate(final OHttpRequest iRequest, final OHttpResponse iResponse,
      final List<String> iAuthenticationParts, final String iDatabaseName) throws IOException {
    ODatabaseDocument db = null;
    try {
      db = (ODatabaseDocument) server.openDatabase(iDatabaseName, iAuthenticationParts.get(0), iAuthenticationParts.get(1));
      // if (db.getUser() == null)
      // // MAYBE A PREVIOUS ROOT REALM? UN AUTHORIZE
      // return false;

      // Set user rid after authentication
      iRequest.data.currentUserId = db.getUser() == null ? "<server user>" : db.getUser().getIdentity().toString();

      // AUTHENTICATED: CREATE THE SESSION
      iRequest.sessionId = OHttpSessionManager.getInstance().createSession(iDatabaseName, iAuthenticationParts.get(0),
          iAuthenticationParts.get(1));
      iResponse.sessionId = iRequest.sessionId;
      return true;

    } catch (OSecurityAccessException e) {
      // WRONG USER/PASSWD
    } catch (OLockException e) {
      OLogManager.instance().error(this, "Cannot access to the database '" + iDatabaseName + "'", ODatabaseException.class, e);
    } finally {
      if (db == null) {
        // WRONG USER/PASSWD
        sendAuthorizationRequest(iRequest, iResponse, iDatabaseName);
      } else {
        db.close();
      }
    }
    return false;
  }

  protected void sendAuthorizationRequest(final OHttpRequest iRequest, final OHttpResponse iResponse, final String iDatabaseName)
      throws IOException {
    // UNAUTHORIZED
    iRequest.sessionId = SESSIONID_UNAUTHORIZED;

    // Defaults to "WWW-Authenticate: Basic".
    String header = server.getSecurity().getAuthenticationHeader(iDatabaseName);

    if (isJsonResponse(iResponse)) {
      sendJsonError(iResponse, OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", header);
    } else {
      iResponse.send(OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", header);
    }
  }

  protected ODatabaseDocumentInternal getProfiledDatabaseInstance(final OHttpRequest iRequest) throws InterruptedException {
    if (iRequest.bearerToken != null) {
      return getProfiledDatabaseInstanceToken(iRequest);
    } else {
      return getProfiledDatabaseInstanceBasic(iRequest);
    }
  }

  protected ODatabaseDocumentInternal getProfiledDatabaseInstanceToken(final OHttpRequest iRequest) throws InterruptedException {
    // after authentication, if current login user is different compare with current DB user, reset DB user to login user
    ODatabaseDocumentInternal localDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (localDatabase == null) {
      localDatabase = (ODatabaseDocumentTx) server.openDatabase(iRequest.databaseName, iRequest.bearerToken);
    } else {
      ORID currentUserId = iRequest.bearerToken.getUserId();
      if (currentUserId != null && localDatabase != null && localDatabase.getUser() != null) {
        if (!currentUserId.equals(localDatabase.getUser().getDocument().getIdentity())) {
          ODocument userDoc = localDatabase.load(currentUserId);
          localDatabase.setUser(new OUser(userDoc));
        }
      }
    }

    iRequest.data.lastDatabase = localDatabase.getName();
    iRequest.data.lastUser = localDatabase.getUser() != null ? localDatabase.getUser().getName() : null;
    return (ODatabaseDocumentTx) localDatabase.getDatabaseOwner();
  }

  protected ODatabaseDocumentInternal getProfiledDatabaseInstanceBasic(final OHttpRequest iRequest) throws InterruptedException {
    final OHttpSession session = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);

    if (session == null)
      throw new OSecurityAccessException(iRequest.databaseName, "No session active");

    // after authentication, if current login user is different compare with current DB user, reset DB user to login user
    ODatabaseDocumentInternal localDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    if (localDatabase == null) {
      localDatabase = (ODatabaseDocumentTx) server.openDatabase(iRequest.databaseName, session.getUserName(),
          session.getUserPassword());
    } else {

      String currentUserId = iRequest.data.currentUserId;
      if (currentUserId != null && currentUserId.length() > 0 && localDatabase != null && localDatabase.getUser() != null) {
        if (!currentUserId.equals(localDatabase.getUser().getIdentity().toString())) {
          ODocument userDoc = localDatabase.load(new ORecordId(currentUserId));
          localDatabase.setUser(new OUser(userDoc));
        }
      }
    }

    iRequest.data.lastDatabase = localDatabase.getName();
    iRequest.data.lastUser = localDatabase.getUser() != null ? localDatabase.getUser().getName() : null;
    return (ODatabaseDocumentTx) localDatabase.getDatabaseOwner();
  }

  private void init() {
    if (tokenHandler == null && OGlobalConfiguration.NETWORK_HTTP_USE_TOKEN.getValueAsBoolean()) {
      tokenHandler = server.getTokenHandler();
    }
  }
}
