/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import com.orientechnologies.orient.core.Orient;

/**
 * OSecurity implementation that extends OSecurityShared but uses an external security plugin.
 *
 * @author S. Colin Leister
 */
public class OSecurityExternal extends OSecurityShared {
  @Override
  public OUser authenticate(ODatabaseSession session, final String iUsername, final String iUserPassword) {
    OUser user = null;
    final String dbName = session.getName();

    if (!(((ODatabaseDocumentInternal) session).getStorage() instanceof OStorageProxy)) {
      if (Orient.instance().getSecurity() == null)
        throw new OSecurityAccessException(dbName, "External Security System is null!");

      // Uses the external authenticator.
      // username is returned if authentication is successful, otherwise null.
      String username = Orient.instance().getSecurity().authenticate(iUsername, iUserPassword);

      if (username != null) {
        user = getUser(session, username);

        if (user == null)
          throw new OSecurityAccessException(dbName,
              "User or password not valid for username: " + username + ", database: '" + dbName + "'");

        if (user.getAccountStatus() != OSecurityUser.STATUSES.ACTIVE)
          throw new OSecurityAccessException(dbName, "User '" + username + "' is not active");
      } else {
        // Will use the local database to authenticate.
        if (Orient.instance().getSecurity().isDefaultAllowed()) {
          user = super.authenticate(session, iUsername, iUserPassword);
        } else {
          // WAIT A BIT TO AVOID BRUTE FORCE
          try {
            Thread.sleep(200);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
          }

          throw new OSecurityAccessException(dbName,
              "User or password not valid for username: " + iUsername + ", database: '" + dbName + "'");
        }
      }
    }

    return user;
  }

  @Override
  public OUser getUser(ODatabaseSession session, final String username) {
    OUser user = null;

    if (Orient.instance().getSecurity() != null) {
      // See if there's a system user first.
      user = Orient.instance().getSecurity().getSystemUser(username, session.getName());
    }

    // If not found, try the local database.
    if (user == null)
      user = super.getUser(session, username);

    return user;
  }
}
