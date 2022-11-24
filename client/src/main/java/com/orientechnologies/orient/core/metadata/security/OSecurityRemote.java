package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OSecurityRemote implements OSecurityInternal {

  public OSecurityRemote() {}

  @Override
  public boolean isAllowed(
      ODatabaseSession session, Set<OIdentifiable> iAllowAll, Set<OIdentifiable> iAllowOperation) {
    return true;
  }

  @Override
  public OIdentifiable allowRole(
      final ODatabaseSession session,
      final ODocument iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    final ORID role = getRoleRID(session, iRoleName);
    if (role == null) throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return allowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public OIdentifiable allowUser(
      final ODatabaseSession session,
      final ODocument iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    final ORID user = getUserRID(session, iUserName);
    if (user == null) throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return allowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public OIdentifiable denyUser(
      final ODatabaseSession session,
      final ODocument iDocument,
      final ORestrictedOperation iOperation,
      final String iUserName) {
    final ORID user = getUserRID(session, iUserName);
    if (user == null) throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public OIdentifiable denyRole(
      final ODatabaseSession session,
      final ODocument iDocument,
      final ORestrictedOperation iOperation,
      final String iRoleName) {
    final ORID role = getRoleRID(session, iRoleName);
    if (role == null) throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public OIdentifiable allowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new ORecordLazySet(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);

    return iId;
  }

  public ORID getRoleRID(final ODatabaseSession session, final String iRoleName) {
    if (iRoleName == null) return null;

    try (final OResultSet result =
        session.query("select @rid as rid from ORole where name = ? limit 1", iRoleName)) {

      if (result.hasNext()) return result.next().getProperty("rid");
    }
    return null;
  }

  public ORID getUserRID(final ODatabaseSession session, final String userName) {
    try (OResultSet result =
        session.query("select @rid as rid from OUser where name = ? limit 1", userName)) {

      if (result.hasNext()) return result.next().getProperty("rid");
    }

    return null;
  }

  @Override
  public OIdentifiable disallowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null) field.remove(iId);
    return iId;
  }

  @Override
  public OUser authenticate(ODatabaseSession session, String iUsername, String iUserPassword) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OUser createUser(
      final ODatabaseSession session,
      final String iUserName,
      final String iUserPassword,
      final String... iRoles) {
    final OUser user = new OUser(iUserName, iUserPassword);

    if (iRoles != null)
      for (String r : iRoles) {
        user.addRole(r);
      }

    return user.save();
  }

  @Override
  public OUser createUser(
      final ODatabaseSession session,
      final String userName,
      final String userPassword,
      final ORole... roles) {
    final OUser user = new OUser(userName, userPassword);

    if (roles != null)
      for (ORole r : roles) {
        user.addRole(r);
      }

    return user.save();
  }

  @Override
  public OUser authenticate(ODatabaseSession session, OToken authToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ORole createRole(
      final ODatabaseSession session, final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
    return createRole(session, iRoleName, null, iAllowMode);
  }

  @Override
  public ORole createRole(
      final ODatabaseSession session,
      final String iRoleName,
      final ORole iParent,
      final ORole.ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(iRoleName, iParent, iAllowMode);
    return role.save();
  }

  @Override
  public OUser getUser(final ODatabaseSession session, final String iUserName) {
    try (OResultSet result = session.query("select from OUser where name = ? limit 1", iUserName)) {
      if (result.hasNext()) return new OUser((ODocument) result.next().getElement().get());
    }
    return null;
  }

  public OUser getUser(final ODatabaseSession session, final ORID iRecordId) {
    if (iRecordId == null) return null;

    ODocument result;
    result = session.load(iRecordId, "roles:1");
    if (!result.getClassName().equals(OUser.CLASS_NAME)) {
      result = null;
    }
    return new OUser(result);
  }

  public ORole getRole(final ODatabaseSession session, final OIdentifiable iRole) {
    final ODocument doc = session.load(iRole.getIdentity());
    if (doc != null) {
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
      if (clazz != null && clazz.isOrole()) {
        return new ORole(doc);
      }
    }

    return null;
  }

  public ORole getRole(final ODatabaseSession session, final String iRoleName) {
    if (iRoleName == null) return null;

    try (final OResultSet result =
        session.query("select from ORole where name = ? limit 1", iRoleName)) {
      if (result.hasNext()) return new ORole((ODocument) result.next().getElement().get());
    }

    return null;
  }

  public List<ODocument> getAllUsers(final ODatabaseSession session) {
    try (OResultSet rs = session.query("select from OUser")) {
      return rs.stream().map((e) -> (ODocument) e.getElement().get()).collect(Collectors.toList());
    }
  }

  public List<ODocument> getAllRoles(final ODatabaseSession session) {
    try (OResultSet rs = session.query("select from ORole")) {
      return rs.stream().map((e) -> (ODocument) e.getElement().get()).collect(Collectors.toList());
    }
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(
      ODatabaseSession session, OSecurityRole role) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(
      ODatabaseSession session, OSecurityRole role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSecurityPolicy(
      ODatabaseSession session, OSecurityRole role, String resource, OSecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicyImpl createSecurityPolicy(ODatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityPolicyImpl getSecurityPolicy(ODatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicyImpl policy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSecurityPolicy(ODatabaseSession session, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSecurityPolicy(ODatabaseSession session, ORole role, String resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropUser(final ODatabaseSession session, final String iUserName) {
    final Number removed =
        session.command("delete from OUser where name = ?", iUserName).next().getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public boolean dropRole(final ODatabaseSession session, final String iRoleName) {
    final Number removed =
        session
            .command("delete from ORole where name = '" + iRoleName + "'")
            .next()
            .getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  @Override
  public void createClassTrigger(ODatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getVersion(ODatabaseSession session) {
    return 0;
  }

  @Override
  public void incrementVersion(ODatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OUser create(ODatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void load(ODatabaseSession session) {}

  @Override
  public void close() {}

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

  @Override
  public OSecurityUser securityAuthenticate(
      ODatabaseSession session, String userName, String password) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSecurityUser securityAuthenticate(
      ODatabaseSession session, OAuthenticationInfo authenticationInfo) {
    throw new UnsupportedOperationException();
  }
}
