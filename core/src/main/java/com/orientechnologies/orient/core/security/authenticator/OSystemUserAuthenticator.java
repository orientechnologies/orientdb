/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;

/**
 * Provides a default password authenticator.
 *
 * @author S. Colin Leister
 */
public class OSystemUserAuthenticator extends OSecurityAuthenticatorAbstract {

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().info(this, "OSystemUserAuthenticator is active");
  }

  // OSecurityComponent
  // Called on removal of the authenticator.
  public void dispose() {}

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  // This will authenticate username using the system database.
  public OSecurityUser authenticate(
      ODatabaseSession session, final String username, final String password) {

    try {
      if (getSecurity() != null) {
        // dbName parameter is null because we don't need to filter any roles for this.
        OSecurityUser user = getSecurity().getSystemUser(username, null);

        if (user != null && user.getAccountStatus() == OSecurityUser.STATUSES.ACTIVE) {
          if (user.checkPassword(password)) return user;
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "authenticate()", ex);
    }

    return null;
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  // Checks to see if a
  public boolean isAuthorized(final String username, final String resource) {
    if (username == null || resource == null) return false;

    try {
      if (getSecurity() != null) {
        OSecurityUser user = getSecurity().getSystemUser(username, null);

        if (user != null && user.getAccountStatus() == OSecurityUser.STATUSES.ACTIVE) {
          OSecurityRole role = null;

          ORule.ResourceGeneric rg = ORule.mapLegacyResourceToGenericResource(resource);

          if (rg != null) {
            String specificResource = ORule.mapLegacyResourceToSpecificResource(resource);

            if (specificResource == null || specificResource.equals("*")) {
              specificResource = null;
            }

            role = user.checkIfAllowed(rg, specificResource, ORole.PERMISSION_EXECUTE);
          }

          return role != null;
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "isAuthorized()", ex);
    }

    return false;
  }

  // OSecurityAuthenticator
  public OSecurityUser getUser(final String username) {
    OSecurityUser userCfg = null;

    try {
      if (getSecurity() != null) {
        userCfg = getSecurity().getSystemUser(username, null);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "getUser()", ex);
    }

    return userCfg;
  }
}
