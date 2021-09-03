/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.security.symmetrickey;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicyImpl;
import com.orientechnologies.orient.core.metadata.security.OSecurityResourceProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a symmetric key specific authentication. Implements an OSecurity interface that
 * delegates to the specified OSecurity object.
 *
 * <p>This is used with embedded (non-server) databases, like so:
 * db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSymmetricKeySecurity.class);
 *
 * @author S. Colin Leister
 */
public class OSymmetricKeySecurity implements OSecurityInternal {
  private OSecurityInternal delegate;

  public OSymmetricKeySecurity(final OSecurityInternal iDelegate) {
    this.delegate = iDelegate;
  }

  @Override
  public OSecurityUser securityAuthenticate(
      ODatabaseSession session, String userName, String password) {
    return authenticate(session, userName, password);
  }

  public OUser authenticate(
      ODatabaseSession session, final String username, final String password) {
    if (delegate == null)
      throw new OSecurityAccessException(
          "OSymmetricKeySecurity.authenticate() Delegate is null for username: " + username);

    if (session == null)
      throw new OSecurityAccessException(
          "OSymmetricKeySecurity.authenticate() Database is null for username: " + username);

    final String dbName = session.getName();

    OUser user = delegate.getUser(session, username);

    if (user == null)
      throw new OSecurityAccessException(
          dbName,
          "OSymmetricKeySecurity.authenticate() Username or Key is invalid for username: "
              + username);

    if (user.getAccountStatus() != OSecurityUser.STATUSES.ACTIVE)
      throw new OSecurityAccessException(
          dbName, "OSymmetricKeySecurity.authenticate() User '" + username + "' is not active");

    try {
      OUserSymmetricKeyConfig userConfig = new OUserSymmetricKeyConfig(user.getDocument());

      OSymmetricKey sk = OSymmetricKey.fromConfig(userConfig);

      String decryptedUsername = sk.decryptAsString(password);

      if (OSecurityManager.checkPassword(username, decryptedUsername)) return user;
    } catch (Exception ex) {
      throw OException.wrapException(
          new OSecurityAccessException(
              dbName,
              "OSymmetricKeySecurity.authenticate() Exception for session: "
                  + dbName
                  + ", username: "
                  + username
                  + " "
                  + ex.getMessage()),
          ex);
    }

    throw new OSecurityAccessException(
        dbName,
        "OSymmetricKeySecurity.authenticate() Username or Key is invalid for session: "
            + dbName
            + ", username: "
            + username);
  }

  @Override
  public boolean isAllowed(
      ODatabaseSession session,
      final Set<OIdentifiable> iAllowAll,
      final Set<OIdentifiable> iAllowOperation) {
    return delegate.isAllowed(session, iAllowAll, iAllowOperation);
  }

  @Override
  public OIdentifiable allowUser(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName) {
    return delegate.allowUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable allowRole(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName) {
    return delegate.allowRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public OIdentifiable denyUser(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName) {
    return delegate.denyUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable denyRole(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName) {
    return delegate.denyRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public OIdentifiable allowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return delegate.allowIdentity(session, iDocument, iAllowFieldName, iId);
  }

  @Override
  public OIdentifiable disallowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return delegate.disallowIdentity(session, iDocument, iAllowFieldName, iId);
  }

  public OUser create(ODatabaseSession session) {
    return delegate.create(session);
  }

  public void load(ODatabaseSession session) {
    delegate.load(session);
  }

  public OUser authenticate(ODatabaseSession session, final OToken authToken) {
    return null;
  }

  public OUser getUser(ODatabaseSession session, final String iUserName) {
    return delegate.getUser(session, iUserName);
  }

  public OUser getUser(ODatabaseSession session, final ORID iUserId) {
    return delegate.getUser(session, iUserId);
  }

  public OUser createUser(
      ODatabaseSession session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public OUser createUser(
      ODatabaseSession session,
      final String iUserName,
      final String iUserPassword,
      final ORole... iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public ORole getRole(ODatabaseSession session, final String iRoleName) {
    return delegate.getRole(session, iRoleName);
  }

  public ORole getRole(ODatabaseSession session, final OIdentifiable iRole) {
    return delegate.getRole(session, iRole);
  }

  public ORole createRole(
      ODatabaseSession session,
      final String iRoleName,
      final OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iAllowMode);
  }

  public ORole createRole(
      ODatabaseSession session,
      final String iRoleName,
      final ORole iParent,
      final OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iParent, iAllowMode);
  }

  public List<ODocument> getAllUsers(ODatabaseSession session) {
    return delegate.getAllUsers(session);
  }

  public List<ODocument> getAllRoles(ODatabaseSession session) {
    return delegate.getAllRoles(session);
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(
      ODatabaseSession session, OSecurityRole role) {
    return delegate.getSecurityPolicies(session, role);
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(
      ODatabaseSession session, OSecurityRole role, String resource) {
    return delegate.getSecurityPolicy(session, role, resource);
  }

  @Override
  public void setSecurityPolicy(
      ODatabaseSession session, OSecurityRole role, String resource, OSecurityPolicyImpl policy) {
    delegate.setSecurityPolicy(session, role, resource, policy);
  }

  @Override
  public OSecurityPolicyImpl createSecurityPolicy(ODatabaseSession session, String name) {
    return delegate.createSecurityPolicy(session, name);
  }

  @Override
  public OSecurityPolicyImpl getSecurityPolicy(ODatabaseSession session, String name) {
    return delegate.getSecurityPolicy(session, name);
  }

  @Override
  public void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicyImpl policy) {
    delegate.saveSecurityPolicy(session, policy);
  }

  @Override
  public void deleteSecurityPolicy(ODatabaseSession session, String name) {
    delegate.deleteSecurityPolicy(session, name);
  }

  @Override
  public void removeSecurityPolicy(ODatabaseSession session, ORole role, String resource) {
    delegate.removeSecurityPolicy(session, role, resource);
  }

  public String toString() {
    return delegate.toString();
  }

  public boolean dropUser(ODatabaseSession session, final String iUserName) {
    return delegate.dropUser(session, iUserName);
  }

  public boolean dropRole(ODatabaseSession session, final String iRoleName) {
    return delegate.dropRole(session, iRoleName);
  }

  public void createClassTrigger(ODatabaseSession session) {
    delegate.createClassTrigger(session);
  }

  @Override
  public long getVersion(ODatabaseSession session) {
    return delegate.getVersion(session);
  }

  @Override
  public void incrementVersion(ODatabaseSession session) {
    delegate.incrementVersion(session);
  }

  @Override
  public Set<String> getFilteredProperties(ODatabaseSession session, ODocument document) {
    return delegate.getFilteredProperties(session, document);
  }

  @Override
  public boolean isAllowedWrite(ODatabaseSession session, ODocument document, String propertyName) {
    return delegate.isAllowedWrite(session, document, propertyName);
  }

  @Override
  public boolean canCreate(ODatabaseSession session, ORecord record) {
    return delegate.canCreate(session, record);
  }

  @Override
  public boolean canRead(ODatabaseSession session, ORecord record) {
    return delegate.canRead(session, record);
  }

  @Override
  public boolean canUpdate(ODatabaseSession session, ORecord record) {
    return delegate.canUpdate(session, record);
  }

  @Override
  public boolean canDelete(ODatabaseSession session, ORecord record) {
    return delegate.canDelete(session, record);
  }

  @Override
  public boolean canExecute(ODatabaseSession session, OFunction function) {
    return delegate.canExecute(session, function);
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(ODatabaseSession session, String resource) {
    return delegate.isReadRestrictedBySecurityPolicy(session, resource);
  }

  @Override
  public Set<OSecurityResourceProperty> getAllFilteredProperties(
      ODatabaseDocumentInternal database) {
    return delegate.getAllFilteredProperties(database);
  }

  @Override
  public OSecurityUser securityAuthenticate(
      ODatabaseSession session, OAuthenticationInfo authenticationInfo) {
    return delegate.securityAuthenticate(session, authenticationInfo);
  }

  @Override
  public void close() {}
}
