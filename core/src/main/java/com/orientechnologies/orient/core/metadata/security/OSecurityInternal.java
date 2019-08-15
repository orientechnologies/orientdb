package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;
import java.util.Map;
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

  Map<String, OSecurityPolicy> getSecurityPolicies(ODatabaseSession session, ORole role);

  OSecurityPolicy getSecurityPolicy(ODatabaseSession session, ORole role, String resource);

  void setSecurityPolicy(ODatabaseSession session, ORole role, String resource, OSecurityPolicy policy);

  /**
   * creates and saves an empty security policy
   * @param session the session to a DB where the policy has to be created
   * @param name the policy name
   * @return
   */
  OSecurityPolicy createSecurityPolicy(ODatabaseSession session, String name);

  OSecurityPolicy getSecurityPolicy(ODatabaseSession session, String name);

  void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicy policy);

  void deleteSecurityPolicy(ODatabaseSession session, String name);

  void removeSecurityPolicy(ODatabaseSession session, ORole role, String resource);

  boolean dropUser(ODatabaseSession session, String iUserName);

  boolean dropRole(ODatabaseSession session, String iRoleName);

  void createClassTrigger(ODatabaseSession session);

  long getVersion(ODatabaseSession session);

  void incrementVersion(ODatabaseSession session);

  OUser create(ODatabaseSession session);

  void load(ODatabaseSession session);

  void close();

  /**
   * For property-level security. Returns the list of the properties that are hidden (ie. not allowed to be read) for current session, regarding a specific document
   * @param session the db session
   * @param document the document to filter
   * @return the list of the properties that are hidden (ie. not allowed to be read) on current document for current session
   */
  Set<String> getFilteredProperties(ODatabaseSession session, ODocument document);

  /**
   * For property-level security
   * @param session
   * @param document current document to check for proeprty-level security
   * @param propertyName the property to check for write access
   * @return
   */
  boolean isAllowedWrite(ODatabaseSession session, ODocument document, String propertyName);

  boolean canCreate(ODatabaseSession session, ORecord record);

  boolean canRead(ODatabaseSession session, ORecord record);

  boolean canUpdate(ODatabaseSession session, ORecord record);

  boolean canDelete(ODatabaseSession session, ORecord record);

  boolean canExecute(ODatabaseSession session, OFunction function);

}
