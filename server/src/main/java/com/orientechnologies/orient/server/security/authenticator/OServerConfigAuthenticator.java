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
package com.orientechnologies.orient.server.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorAbstract;

/**
 * Provides an OSecurityAuthenticator for the users listed in orientdb-server-config.xml.
 *
 * @author S. Colin Leister
 */
public class OServerConfigAuthenticator extends OSecurityAuthenticatorAbstract {
  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().info(this, "OServerConfigAuthenticator is active");
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  public String authenticate(final String username, final String password) {
    String principal = null;

    try {
      if (getServerConfig() != null) {
        OServerUserConfiguration userCfg = null;

        // This will throw an IllegalArgumentException if username is null or empty.
        // However, a null or empty username is possible with some security implementations.
        if (username != null && !username.isEmpty())
          userCfg = getServerConfig().getUser(username);

        if (userCfg != null && userCfg.password != null) {
          if (OSecurityManager.instance().checkPassword(password, userCfg.password)) {
            principal = userCfg.name;
          }
        }
      } else {
        OLogManager.instance().error(this, "OServerConfigAuthenticator.authenticate() ServerConfig is null", null);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OServerConfigAuthenticator.authenticate()", ex);
    }

    return principal;
  }

  // OSecurityAuthenticator
  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig) {
    super.config(oServer, serverCfg, jsonConfig);
  }

  // OSecurityAuthenticator
  public OServerUserConfiguration getUser(final String username) {
    OServerUserConfiguration userCfg = null;

    if (getServerConfig() != null) {
      userCfg = getServerConfig().getUser(username);
    }

    return userCfg;
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  public boolean isAuthorized(final String username, final String resource) {
    if (username == null || resource == null)
      return false;

    if (getServerConfig() != null) {
      // getUser() will throw an IllegalArgumentException if username is null or empty.
      // However, a null or empty username is possible with some security implementations.
      if (!username.isEmpty()) {
        OServerUserConfiguration userCfg = getServerConfig().getUser(username);

        if (userCfg != null) {
          // Total Access
          if (userCfg.resources.equals("*"))
            return true;

          String[] resourceParts = userCfg.resources.split(",");

          for (String r : resourceParts) {
            if (r.equalsIgnoreCase(resource))
              return true;
          }
        }
      }
    } else {
      OLogManager.instance().error(this, "OServerConfigAuthenticator.isAuthorized() ServerConfig is null", null);
    }

    return false;
  }

  // Server configuration users are never case sensitive.
  @Override
  protected boolean isCaseSensitive() {
    return false;
  }
}
