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
package com.orientechnologies.orient.server.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorAbstract;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides a default password authenticator.
 * 
 * @author S. Colin Leister
 * 
 */
public class ODefaultPasswordAuthenticator extends OSecurityAuthenticatorAbstract {
  // Holds a map of the users specified in the security.json file.
  private ConcurrentHashMap<String, OServerUserConfiguration> usersMap = new ConcurrentHashMap<String, OServerUserConfiguration>();

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().info(this, "ODefaultPasswordAuthenticator is active");
  }

  // OSecurityComponent
  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig) {
    super.config(oServer, serverCfg, jsonConfig);

    try {
      if (jsonConfig.containsField("users")) {
        List<ODocument> usersList = jsonConfig.field("users");

        for (ODocument userDoc : usersList) {
        	
          OServerUserConfiguration userCfg = createServerUser(userDoc);
         
          if (userCfg != null) {
            String checkName = userCfg.name;
    
            if (!isCaseSensitive()) checkName = checkName.toLowerCase(Locale.ENGLISH);
          	
          	usersMap.put(checkName, userCfg);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "config()", ex);
    }
  }
  
  // Derived implementations can override this method to provide new server user implementations.
  protected OServerUserConfiguration createServerUser(final ODocument userDoc) {
    OServerUserConfiguration userCfg = null;
    
    if (userDoc.containsField("username") && userDoc.containsField("resources")) {
      final String user = userDoc.field("username");
      final String resources = userDoc.field("resources");
      String password = userDoc.field("password");    
    
      if (password == null) password = "";
    
      userCfg = new OServerUserConfiguration(user, password, resources);
    }
   
    return userCfg;
  }

  // OSecurityComponent
  // Called on removal of the authenticator.
  public void dispose() {
    synchronized (usersMap) {
      usersMap.clear();
      usersMap = null;
    }
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  public String authenticate(final String username, final String password) {
    String principal = null;

    try {
      OServerUserConfiguration user = getUser(username);

      if (isPasswordValid(user)) {
        if (OSecurityManager.instance().checkPassword(password, user.password)) {
          principal = user.name;
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultPasswordAuthenticator.authenticate()", ex);
    }

    return principal;
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  public boolean isAuthorized(final String username, final String resource) {
    if (username == null || resource == null)
      return false;

    OServerUserConfiguration userCfg = getUser(username);

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

    return false;
  }

  // OSecurityAuthenticator
  public OServerUserConfiguration getUser(final String username) {
    OServerUserConfiguration userCfg = null;

    synchronized (usersMap) {
      if (username != null) {
        String checkName = username;

        if (!isCaseSensitive())
          checkName = username.toLowerCase(Locale.ENGLISH);

        if (usersMap.containsKey(checkName)) {
          userCfg = usersMap.get(checkName);
        }
      }
    }

    return userCfg;
  }
}