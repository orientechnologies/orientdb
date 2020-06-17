package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OSecurityRemote implements OSecurityInternal {

  private OSecurityInternal delegate;

  public OSecurityRemote(OSecurityInternal delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isAllowed(
      ODatabaseSession session, Set<OIdentifiable> iAllowAll, Set<OIdentifiable> iAllowOperation) {
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

  @Override
  public OUser authenticate(ODatabaseSession session, String iUsername, String iUserPassword) {
    return delegate.authenticate(session, iUsername, iUserPassword);
  }

  @Override
  public OUser createUser(
      ODatabaseSession session, String iUserName, String iUserPassword, String[] iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  @Override
  public OUser createUser(
      ODatabaseSession session, String iUserName, String iUserPassword, ORole[] iRoles) {
    return delegate.createUser(session, iUserName, iUserPassword, iRoles);
  }

  @Override
  public OUser authenticate(ODatabaseSession session, OToken authToken) {
    return delegate.authenticate(session, authToken);
  }

  @Override
  public ORole createRole(
      ODatabaseSession session,
      String iRoleName,
      ORole iParent,
      OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iParent, iAllowMode);
  }

  @Override
  public ORole createRole(
      ODatabaseSession session, String iRoleName, OSecurityRole.ALLOW_MODES iAllowMode) {
    return delegate.createRole(session, iRoleName, iAllowMode);
  }

  @Override
  public OUser getUser(ODatabaseSession session, String iUserName) {
    return delegate.getUser(session, iUserName);
  }

  @Override
  public OUser getUser(ODatabaseSession session, ORID userId) {
    return delegate.getUser(session, userId);
  }

  @Override
  public ORole getRole(ODatabaseSession session, String iRoleName) {
    return delegate.getRole(session, iRoleName);
  }

  @Override
  public ORole getRole(ODatabaseSession session, OIdentifiable iRoleRid) {
    return delegate.getRole(session, iRoleRid);
  }

  @Override
  public List<ODocument> getAllUsers(ODatabaseSession session) {
    return delegate.getAllUsers(session);
  }

  @Override
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
      ODatabaseSession session, OSecurityRole role, String resource, OSecurityPolicy policy) {
    delegate.setSecurityPolicy(session, role, resource, policy);
  }

  @Override
  public OSecurityPolicy createSecurityPolicy(ODatabaseSession session, String name) {
    return delegate.createSecurityPolicy(session, name);
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(ODatabaseSession session, String name) {
    return delegate.getSecurityPolicy(session, name);
  }

  @Override
  public void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicy policy) {
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

  @Override
  public boolean dropUser(ODatabaseSession session, String iUserName) {
    return delegate.dropUser(session, iUserName);
  }

  @Override
  public boolean dropRole(ODatabaseSession session, String iRoleName) {
    return delegate.dropRole(session, iRoleName);
  }

  @Override
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
  public OUser create(ODatabaseSession session) {
    return delegate.create(session);
  }

  @Override
  public void load(ODatabaseSession session) {
    delegate.load(session);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Set<String> getFilteredProperties(ODatabaseSession session, ODocument document) {
    return Collections.emptySet();
  }

  @Override
  public boolean isAllowedWrite(ODatabaseSession session, ODocument document, String propertyName) {
    return true;
  }

  @Override
  public boolean canCreate(ODatabaseSession session, ORecord record) {
    return true;
  }

  @Override
  public boolean canRead(ODatabaseSession session, ORecord record) {
    return true;
  }

  @Override
  public boolean canUpdate(ODatabaseSession session, ORecord record) {
    return true;
  }

  @Override
  public boolean canDelete(ODatabaseSession session, ORecord record) {
    return true;
  }

  @Override
  public boolean canExecute(ODatabaseSession session, OFunction function) {
    return true;
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(ODatabaseSession session, String resource) {
    return false;
  }

  @Override
  public Set<OSecurityResourceProperty> getAllFilteredProperties(
      ODatabaseDocumentInternal database) {
    return Collections.EMPTY_SET;
  }
}
