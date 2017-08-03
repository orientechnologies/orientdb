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
package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

/**
 * Provides an interface for the server-specific security features. Extends OSecuritySystem.
 * 
 * @author S. Colin Leister
 * 
 */
public interface OServerSecurity extends OSecuritySystem {
  OAuditingService getAuditing();

  /**
   * Returns the authenticator based on name, if one exists.
   */
  OSecurityAuthenticator getAuthenticator(final String authName);

  /**
   * Returns the first authenticator in the list, which is the primary authenticator.
   */
  OSecurityAuthenticator getPrimaryAuthenticator();

  OSyslog getSyslog();

  /**
   * Some authenticators support maintaining a list of users and associated resources (and sometimes passwords).
   */
  OServerUserConfiguration getUser(final String username);

  ODatabase<?> openDatabase(final String dbName);
  
  void onAfterDynamicPlugins();
}
