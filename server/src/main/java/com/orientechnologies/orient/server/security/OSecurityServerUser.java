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
package com.orientechnologies.orient.server.security;

import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Security injected by server. This implementation is still able to change use to a database user.
 * 
 * @author Luca Garulli
 * 
 */
public class OSecurityServerUser extends OProxedResource<OSecurity> implements OSecurity {
  public OSecurityServerUser(final OSecurity iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation) {
    return delegate.isAllowed(iAllowAll, iAllowOperation);
  }

  @Override
  public OIdentifiable allowUser(ODocument iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return delegate.allowUser(iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable allowRole(ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return delegate.allowRole(iDocument, iOperationType, iRoleName);
  }

  @Override
  public OIdentifiable denyUser(ODocument iDocument, ORestrictedOperation iOperationType, String iUserName) {
    return delegate.denyUser(iDocument, iOperationType, iUserName);
  }

  @Override
  public OIdentifiable denyRole(ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName) {
    return delegate.denyRole(iDocument, iOperationType, iRoleName);
  }

  public OIdentifiable allowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    return delegate.allowUser(iDocument, iAllowFieldName, iUserName);
  }

  public OIdentifiable allowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    return delegate.allowRole(iDocument, iAllowFieldName, iRoleName);
  }

  @Override
  public OIdentifiable allowIdentity(ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return delegate.allowIdentity(iDocument, iAllowFieldName, iId);
  }

  public OIdentifiable disallowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    return delegate.disallowUser(iDocument, iAllowFieldName, iUserName);
  }

  public OIdentifiable disallowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    return delegate.disallowRole(iDocument, iAllowFieldName, iRoleName);
  }

  @Override
  public OIdentifiable disallowIdentity(ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return delegate.disallowIdentity(iDocument, iAllowFieldName, iId);
  }

  public OUser create() {
    return delegate.create();
  }

  public void load() {
    delegate.load();
  }

  public void close(boolean onDelete) {
    if (delegate != null)
      delegate.close(false);
  }

  public OUser authenticate(final String iUsername, final String iUserPassword) {
    return null;
  }

  public OUser authenticate(final OToken authToken) {
    return null;
  }

  public OUser getUser(final String iUserName) {
    return delegate.getUser(iUserName);
  }

  public OUser getUser(final ORID iUserId) {
    return delegate.getUser(iUserId);
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final String... iRoles) {
    return delegate.createUser(iUserName, iUserPassword, iRoles);
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final ORole... iRoles) {
    return delegate.createUser(iUserName, iUserPassword, iRoles);
  }

  public ORole getRole(final String iRoleName) {
    return delegate.getRole(iRoleName);
  }

  public ORole getRole(final OIdentifiable iRole) {
    return delegate.getRole(iRole);
  }

  public ORole createRole(final String iRoleName, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(iRoleName, iAllowMode);
  }

  public ORole createRole(final String iRoleName, final ORole iParent, final OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(iRoleName, iParent, iAllowMode);
  }

  public List<ODocument> getAllUsers() {
    return delegate.getAllUsers();
  }

  public List<ODocument> getAllRoles() {
    return delegate.getAllRoles();
  }

  public String toString() {
    return delegate.toString();
  }

  public boolean dropUser(final String iUserName) {
    return delegate.dropUser(iUserName);
  }

  public boolean dropRole(final String iRoleName) {
    return delegate.dropRole(iRoleName);
  }

  public void createClassTrigger() {
    delegate.createClassTrigger();
  }

  @Override
  public OSecurity getUnderlying() {
    return delegate;
  }

  @Override
  public long getVersion() {
    return delegate.getVersion();
  }

  @Override
  public void incrementVersion() {
    delegate.incrementVersion();
  }
}
