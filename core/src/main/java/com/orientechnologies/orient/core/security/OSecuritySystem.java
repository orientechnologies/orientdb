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

import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides a basic interface for a modular security system.
 *
 * @author S. Colin Leister
 */
public interface OSecuritySystem {
  void shutdown();

  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  boolean isDefaultAllowed();

  // Returns the actual username if successful, null otherwise.
  // Some token-based authentication (e.g., SPNEGO tokens have the user's name embedded in the service ticket).
  String authenticate(final String username, final String password);

  // Used for generating the appropriate HTTP authentication mechanism. The chain of authenticators is used for this.
  String getAuthenticationHeader(final String databaseName);

  default Map<String, String> getAuthenticationHeaders(final String databaseName) {
    return new HashMap<>();
  }

  ODocument getConfig();

  ODocument getComponentConfig(final String name);

  /**
   * Returns the "System User" associated with 'username' from the system database. If not found, returns null. dbName is used to
   * filter the assigned roles. It may be null.
   */
  OUser getSystemUser(final String username, final String dbName);

  // Walks through the list of Authenticators.
  boolean isAuthorized(final String username, final String resource);

  boolean isEnabled();

  // Indicates if passwords should be stored when creating new users.
  boolean arePasswordsStored();

  // Indicates if the primary security mechanism supports single sign-on.
  boolean isSingleSignOnSupported();

  /**
   * Logs to the auditing service, if installed.
   *
   * @param dbName   May be null or empty.
   * @param username May be null or empty.
   */
  void log(final OAuditingOperation operation, final String dbName, final String username, final String message);

  void registerSecurityClass(final Class<?> cls);

  void reload(final String cfgPath);

  void reload(final ODocument jsonConfig);

  void reloadComponent(final String name, final ODocument jsonConfig);

  /**
   * Called each time one of the security classes (OUser, ORole, OServerRole) is modified.
   */
  void securityRecordChange(final String dbURL, final ODocument record);

  void unregisterSecurityClass(final Class<?> cls);

  // If a password validator is registered with the security system, it will be called to validate
  // the specified password. An OInvalidPasswordException is thrown if the password does not meet
  // the password validator's requirements.
  void validatePassword(final String password) throws OInvalidPasswordException;
}
