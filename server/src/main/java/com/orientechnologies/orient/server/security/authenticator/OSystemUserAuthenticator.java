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
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorAbstract;

/**
 * Provides a default password authenticator.
 * 
 * @author S. Colin Leister
 * 
 */
public class OSystemUserAuthenticator extends OSecurityAuthenticatorAbstract {

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().info(this, "OSystemUserAuthenticator is active");
  }

  // OSecurityComponent
  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig) {
    super.config(oServer, serverCfg, jsonConfig);

    try {
    } catch (Exception ex) {
      OLogManager.instance().error(this, "config() Exception: %s", ex.getMessage());
    }
  }

  // OSecurityComponent
  // Called on removal of the authenticator.
  public void dispose() {
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  // This will authenticate username using the system database.
  public String authenticate(final String username, final String password) {
    String principal = null;

    try {
      if(getServer() != null) {	
    	  // dbName parameter is null because we don't need to filter any roles for this.
    	  OUser user = getServer().getSecurity().getSystemUser(username, null);

    	  if(user != null && user.getAccountStatus() == OSecurityUser.STATUSES.ACTIVE) {
          if (user.checkPassword(password)) principal = username;
    	  }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "authenticate() Exception: %s", ex.getMessage());
    }

    return principal;
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  // Checks to see if a 
  public boolean isAuthorized(final String username, final String resource) {
    if (username == null || resource == null)
      return false;

    try {
      if(getServer() != null) {
    	  OUser user = getServer().getSecurity().getSystemUser(username, null);
    	  
    	  if(user != null && user.getAccountStatus() == OSecurityUser.STATUSES.ACTIVE) {
          ORole role = null;

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
      OLogManager.instance().error(this, "isAuthorized() Exception: %s", ex.getMessage());
    }

    return false;
  }

  // OSecurityAuthenticator
  public OServerUserConfiguration getUser(final String username) {
    OServerUserConfiguration userCfg = null;

    try {
      if(getServer() != null) {
    	  OUser user = getServer().getSecurity().getSystemUser(username, null);
    		
    	  if(user != null && user.getAccountStatus() == OSecurityUser.STATUSES.ACTIVE) {
          userCfg = new OServerUserConfiguration(user.getName(), "", "");
    	  }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "getUser() Exception: %s", ex.getMessage());
    }

    return userCfg;
  }
}