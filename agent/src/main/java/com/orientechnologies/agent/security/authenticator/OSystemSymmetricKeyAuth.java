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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKey;
import com.orientechnologies.orient.core.security.symmetrickey.OUserSymmetricKeyConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
//import com.orientechnologies.orient.server.security.OSecurityAuthenticatorAbstract;
import com.orientechnologies.orient.server.security.authenticator.OSystemUserAuthenticator;


/**
 * Provides an OSystem user symmetric key authenticator, derived from OSystemUserAuthenticator.
 * This is used in security.json.
 * 
 * @author S. Colin Leister
 * 
 */
public class OSystemSymmetricKeyAuth extends OSystemUserAuthenticator {

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().info(this, "OSystemSymmetricKeyAuth is active");
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
          OUserSymmetricKeyConfig userConfig = new OUserSymmetricKeyConfig(user);
    	  	   
          OSymmetricKey sk = OSymmetricKey.fromConfig(userConfig);

          String decryptedUsername = sk.decryptAsString(password);
            
          if(OSecurityManager.instance().checkPassword(username, decryptedUsername)) {
            principal = username;
          }
    	  }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "authenticate() Exception: %s", ex.getMessage());
    }

    return principal;
  }
}