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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.security.authenticator.ODefaultPasswordAuthenticator;


import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides a symmetric key authenticator derived from ODefaultPasswordAuthenticator.
 * This is used in security.json.
 * 
 * @author S. Colin Leister
 * 
 */
public class OSecuritySymmetricKeyAuth extends ODefaultPasswordAuthenticator {
  // OSecurityComponent
  // Called once the Server is running.
  @Override
  public void active() {
    OLogManager.instance().info(this, "OSecuritySymmetricKeyAuth is active");
  }

  // Derived implementations can override this method to provide new server user implementations.
  @Override
  protected OServerUserConfiguration createServerUser(final ODocument userDoc) {
    OServerUserConfiguration userCfg = null;
    
    try {
    	userCfg = new OSecuritySymmetricKeyUser(userDoc);
    } catch (Exception ex) {
    	OLogManager.instance().error(this, "createServerUser() Exception: %s", ex.getMessage());
    }

    return userCfg;
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  public String authenticate(final String username, final String password) {
    String principal = null;

    OServerUserConfiguration serverUser = getUser(username);
    
    if(serverUser != null && serverUser instanceof OSecuritySymmetricKeyUser) {
      try {
        OSecuritySymmetricKeyUser user = (OSecuritySymmetricKeyUser)serverUser;

        OSymmetricKey sk = OSymmetricKey.fromConfig(user);

        String decryptedUsername = sk.decryptAsString(password);
        
        if (OSecurityManager.instance().checkPassword(username, decryptedUsername)) {
          principal = username; // user.name;
        }        
      } catch (Exception ex) {
        OLogManager.instance().error(this, "authenticate() Exception: %s", ex.getMessage());
      }      
    }
  
    return principal;
  }
}