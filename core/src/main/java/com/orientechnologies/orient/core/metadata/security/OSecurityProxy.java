/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Set;

/**
 * Proxy class for user management
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSecurityProxy implements OSecurity {
  private ODatabaseSession session;
  private OSecurityInternal security;

  public OSecurityProxy(OSecurityInternal security, ODatabaseDocumentInternal session) {
    this.security = security;
    this.session = session;
  }

  @Override
  public boolean isAllowed(
      final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation) {
    return security.isAllowed(session, iAllowAll, iAllowOperation);
  }

  @Override
  public OIdentifiable allowUser(
      ODocument iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return security.allowUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable allowRole(
      ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return security.allowRole(session, iDocument, iOperationType, iRoleName);
  }

  @Override
  public OIdentifiable denyUser(
      ODocument iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return security.denyUser(session, iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable denyRole(
      ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return security.denyRole(session, iDocument, iOperationType, iRoleName);
  }

  public OUser authenticate(final String iUsername, final String iUserPassword) {
    return security.authenticate(session, iUsername, iUserPassword);
  }

  public OUser authenticate(final OToken authToken) {
    return security.authenticate(session, authToken);
  }

  public OUser getUser(final String iUserName) {
    return security.getUser(session, iUserName);
  }

  public OUser getUser(final ORID iUserId) {
    return security.getUser(session, iUserId);
  }

  public OUser createUser(
      final String iUserName, final String iUserPassword, final String... iRoles) {
    return security.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public OUser createUser(
      final String iUserName, final String iUserPassword, final ORole... iRoles) {
    return security.createUser(session, iUserName, iUserPassword, iRoles);
  }

  public ORole getRole(final String iRoleName) {
    return security.getRole(session, iRoleName);
  }

  public ORole getRole(final OIdentifiable iRole) {
    return security.getRole(session, iRole);
  }

  public ORole createRole(final String iRoleName, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return security.createRole(session, iRoleName, iAllowMode);
  }

  public ORole createRole(
      final String iRoleName, final ORole iParent, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return security.createRole(session, iRoleName, iParent, iAllowMode);
  }

  public List<ODocument> getAllUsers() {
    return security.getAllUsers(session);
  }

  public List<ODocument> getAllRoles() {
    return security.getAllRoles(session);
  }

  public String toString() {
    return security.toString();
  }

  public boolean dropUser(final String iUserName) {
    return security.dropUser(session, iUserName);
  }

  public boolean dropRole(final String iRoleName) {
    return security.dropRole(session, iRoleName);
  }
}
