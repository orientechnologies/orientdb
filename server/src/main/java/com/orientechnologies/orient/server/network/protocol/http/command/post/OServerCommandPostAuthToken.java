package com.orientechnologies.orient.server.network.protocol.http.command.post;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

/**
 * Created by emrul on 14/09/14.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class OServerCommandPostAuthToken extends OServerCommandAbstract {
  private static final String[]  NAMES           = { "POST|token/*" };
  private static final String    RESPONSE_FORMAT = "indent:-1,attribSameRow";
  private volatile OTokenHandler tokenHandler;

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private void init() {

    if (tokenHandler == null && OGlobalConfiguration.NETWORK_HTTP_USE_TOKEN.getValueAsBoolean()) {
      tokenHandler = server.getTokenHandler();
    }
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    init();
    String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: token/<database>");
    iRequest.databaseName = urlParts[1];

    iRequest.data.commandInfo = "Generate authentication token";

    // Parameter names consistent with 4.3.2 (Access Token Request) of RFC 6749
    Map<String, String> content = iRequest.getUrlEncodedContent();
    String signedToken = "";// signedJWT.serialize();

    String grantType = content.get("grant_type").toLowerCase();
    String username = content.get("username");
    String password = content.get("password");
    String authenticatedRid;
    ODocument result;

    if (grantType.equals("password")) {
      authenticatedRid = authenticate(username, password, iRequest.databaseName);
      if (authenticatedRid == null) {
        sendAuthorizationRequest(iRequest, iResponse, iRequest.databaseName);
      } else {
        // Generate and return a JWT access token

        ODatabaseDocument db = null;
        OSecurityUser user = null;
        try {
          db = (ODatabaseDocument) server.openDatabase(iRequest.databaseName, username, password);
          user = db.getUser();

          if (user != null) {
            byte[] tokenBytes = tokenHandler.getSignedWebToken(db, user);
            signedToken = new String(tokenBytes);
          } else {
            // Server user (not supported yet!)
          }

        } catch (OSecurityAccessException e) {
          // WRONG USER/PASSWD
        } catch (OLockException e) {
          OLogManager.instance().error(this, "Cannot access to the database '" + iRequest.databaseName + "'",
              ODatabaseException.class, e);
        } finally {
          if (db != null) {
            db.close();
          }
        }

        // 4.1.4 (Access Token Response) of RFC 6749
        result = new ODocument().field("access_token", signedToken).field("expires_in", 3600);

        iResponse.writeRecord(result, RESPONSE_FORMAT, null);
      }
    } else {
      result = new ODocument().field("error", "unsupported_grant_type");
      sendError(iRequest, iResponse, result);
    }

    return false;
  }

  // Return user rid if authentication successful.
  // If user is server user (doesn't have a rid) then '<server user>' is returned.
  // null is returned in all other cases and means authentication was unsuccessful.
  protected String authenticate(final String username, final String password, final String iDatabaseName) throws IOException {
    ODatabaseDocument db = null;
    String userRid = null;
    try {
      db = (ODatabaseDocument) server.openDatabase(iDatabaseName, username, password);

      userRid = (db.getUser() == null ? "<server user>" : db.getUser().getDocument().getIdentity().toString());
    } catch (OSecurityAccessException e) {
      // WRONG USER/PASSWD
    } catch (OLockException e) {
      OLogManager.instance().error(this, "Cannot access to the database '" + iDatabaseName + "'", ODatabaseException.class, e);
    } finally {
      if (db != null) {
        db.close();
      }
    }
    return userRid;
  }

  protected void sendError(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODocument error) throws IOException {
    iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_JSON, error.toJSON(),
        null);
  }

  protected void sendAuthorizationRequest(final OHttpRequest iRequest, final OHttpResponse iResponse, final String iDatabaseName)
      throws IOException {

    // Defaults to "WWW-Authenticate: Basic".
    String header = server.getSecurity().getAuthenticationHeader(iDatabaseName);

    if (isJsonResponse(iResponse)) {
      sendJsonError(iResponse, OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", header);
    } else {
      iResponse.send(OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.", header);
    }

  }
}
