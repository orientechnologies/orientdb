package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;
import java.util.Set;

public interface OSecurityInternal {
  boolean isAllowed(ODatabaseSession session, Set<OIdentifiable> iAllowAll, Set<OIdentifiable> iAllowOperation);

  OIdentifiable allowUser(ODatabaseSession session, ODocument iDocument, ORestrictedOperation iOperationType, String iUserName);

  OIdentifiable allowRole(ODatabaseSession session, ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName);

  OIdentifiable denyUser(ODatabaseSession session, ODocument iDocument, ORestrictedOperation iOperationType, String iUserName);

  OIdentifiable denyRole(ODatabaseSession session, ODocument iDocument, ORestrictedOperation iOperationType, String iRoleName);

  OIdentifiable allowIdentity(ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId);

  OIdentifiable disallowIdentity(ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId);

  OUser authenticate(ODatabaseSession session, String iUsername, String iUserPassword);

  OUser createUser(ODatabaseSession session, String iUserName, String iUserPassword, String[] iRoles);

  OUser createUser(ODatabaseSession session, String iUserName, String iUserPassword, ORole[] iRoles);

  OUser authenticate(ODatabaseSession session, OToken authToken);

  ORole createRole(ODatabaseSession session, String iRoleName, ORole iParent, OSecurityRole.ALLOW_MODES iAllowMode);

  ORole createRole(ODatabaseSession session, String iRoleName, OSecurityRole.ALLOW_MODES iAllowMode);

  OUser getUser(ODatabaseSession session, String iUserName);

  OUser getUser(ODatabaseSession session, ORID userId);

  ORole getRole(ODatabaseSession session, String iRoleName);

  ORole getRole(ODatabaseSession session, OIdentifiable iRoleRid);

  List<ODocument> getAllUsers(ODatabaseSession session);

  List<ODocument> getAllRoles(ODatabaseSession session);

  boolean dropUser(ODatabaseSession session, String iUserName);

  boolean dropRole(ODatabaseSession session, String iRoleName);

  void createClassTrigger(ODatabaseSession session);

  long getVersion(ODatabaseSession session);

  void incrementVersion(ODatabaseSession session);

  OUser create(ODatabaseSession session);

  void load(ODatabaseSession session);

  void close();

  Set<String> getFilteredProperties(ODocument document);

  boolean isAllowedWrite(ODocument document, String name);
}
