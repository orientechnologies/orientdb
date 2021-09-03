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
package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;

/**
 * Provides an interface for creating security authenticators.
 *
 * @author S. Colin Leister
 */
public interface OSecurityAuthenticator extends OSecurityComponent {
  // Returns the actual username if successful, null otherwise.
  // Some token-based authentication (e.g., SPNEGO tokens have the user's name embedded in the
  // service ticket).
  OSecurityUser authenticate(
      ODatabaseSession session, final String username, final String password);

  OSecurityUser authenticate(ODatabaseSession session, OAuthenticationInfo authenticationInfo);

  String getAuthenticationHeader(final String databaseName);

  default Map<String, String> getAuthenticationHeaders(String databaseName) {
    return new HashMap<>();
  }

  Subject getClientSubject();

  // Returns the name of this OSecurityAuthenticator.
  String getName();

  OSecurityUser getUser(final String username);

  boolean isAuthorized(final String username, final String resource);

  boolean isSingleSignOnSupported();
}
