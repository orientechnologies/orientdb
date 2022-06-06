package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.metadata.security.auth.OTokenAuthInfo;
import com.orientechnologies.orient.core.metadata.security.auth.OUserPasswordAuthInfo;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.security.OTokenSign;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;

public class ODatabaseUserAuthenticator extends OSecurityAuthenticatorAbstract {
  private OTokenSign tokenSign;

  @Override
  public void config(ODocument jsonConfig, OSecuritySystem security) {
    super.config(jsonConfig, security);
    tokenSign = security.getTokenSign();
  }

  @Override
  public OSecurityUser authenticate(ODatabaseSession session, OAuthenticationInfo info) {
    if (info instanceof OUserPasswordAuthInfo) {
      return authenticate(
          session,
          ((OUserPasswordAuthInfo) info).getUser(),
          ((OUserPasswordAuthInfo) info).getPassword());
    } else if (info instanceof OTokenAuthInfo) {
      OParsedToken token = ((OTokenAuthInfo) info).getToken();

      if (tokenSign != null && !tokenSign.verifyTokenSign(token)) {
        throw new OTokenSecurityException("The token provided is expired");
      }
      if (token.getToken().getIsValid() != true) {
        throw new OSecurityAccessException(session.getName(), "Token not valid");
      }

      OUser user = token.getToken().getUser((ODatabaseDocumentInternal) session);
      if (user == null && token.getToken().getUserName() != null) {
        OSecurityShared databaseSecurity =
            (OSecurityShared)
                ((ODatabaseDocumentInternal) session).getSharedContext().getSecurity();
        user = databaseSecurity.getUserInternal(session, token.getToken().getUserName());
      }
      return user;
    }
    return super.authenticate(session, info);
  }

  @Override
  public OSecurityUser authenticate(ODatabaseSession session, String username, String password) {
    if (session == null) {
      return null;
    }

    String dbName = session.getName();
    OSecurityShared databaseSecurity =
        (OSecurityShared) ((ODatabaseDocumentInternal) session).getSharedContext().getSecurity();
    OUser user = databaseSecurity.getUserInternal(session, username);
    if (user == null) {
      return null;
    }
    if (user.getAccountStatus() != OSecurityUser.STATUSES.ACTIVE)
      throw new OSecurityAccessException(dbName, "User '" + username + "' is not active");

    // CHECK USER & PASSWORD
    if (!user.checkPassword(password)) {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      throw new OSecurityAccessException(
          dbName, "User or password not valid for database: '" + dbName + "'");
    }

    return user;
  }

  @Override
  public OSecurityUser getUser(String username) {
    return null;
  }

  @Override
  public boolean isAuthorized(String username, String resource) {
    return false;
  }

  @Override
  public boolean isSingleSignOnSupported() {
    return false;
  }
}
