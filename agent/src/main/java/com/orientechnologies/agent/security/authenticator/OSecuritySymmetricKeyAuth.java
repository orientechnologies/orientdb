/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.agent.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.security.OImmutableUser;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.server.security.authenticator.ODefaultPasswordAuthenticator;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a symmetric key authenticator derived from ODefaultPasswordAuthenticator. This is used
 * in security.json.
 *
 * @author S. Colin Leister
 */
public class OSecuritySymmetricKeyAuth extends ODefaultPasswordAuthenticator {
  // OSecurityComponent
  // Called once the Server is running.
  private Map<String, OSecuritySymmetricKeyUser> symmetricKeys = new HashMap<>();

  @Override
  public void active() {
    OLogManager.instance().info(this, "OSecuritySymmetricKeyAuth is active");
  }

  // Derived implementations can override this method to provide new server user implementations.
  @Override
  protected OSecurityUser createServerUser(final ODocument userDoc) {
    OSecurityUser userCfg = null;

    try {
      OSecuritySymmetricKeyUser user = new OSecuritySymmetricKeyUser(userDoc);
      symmetricKeys.put(user.getName(), user);
      OSecurityRole role = OSecurityShared.createRole(null, user);
      userCfg =
          new OImmutableUser(
              user.getName(), user.getPassword(), OSecurityUser.SECURITY_USER_TYPE, role);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "createServerUser()", ex);
    }

    return userCfg;
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  public OSecurityUser authenticate(
      ODatabaseSession session, final String username, final String password) {

    OSecuritySymmetricKeyUser user = symmetricKeys.get(username);

    if (user != null) {
      try {

        OSymmetricKey sk = OSymmetricKey.fromConfig(user);

        String decryptedUsername = sk.decryptAsString(password);

        if (OSecurityManager.checkPassword(username, decryptedUsername)) {
          OSecurityUser serverUser = getUser(username);
          return serverUser;
        }
      } catch (Exception ex) {
        OLogManager.instance().error(this, "authenticate()", ex);
      }
    }

    return null;
  }
}
