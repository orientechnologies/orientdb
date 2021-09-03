package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface OSecurityInternal {
  boolean isAllowed(
      ODatabaseSession session, Set<OIdentifiable> iAllowAll, Set<OIdentifiable> iAllowOperation);

  @Deprecated
  OIdentifiable allowUser(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  OIdentifiable allowRole(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  OIdentifiable denyUser(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iUserName);

  @Deprecated
  OIdentifiable denyRole(
      ODatabaseSession session,
      ODocument iDocument,
      ORestrictedOperation iOperationType,
      String iRoleName);

  @Deprecated
  OIdentifiable allowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId);

  @Deprecated
  OIdentifiable disallowIdentity(
      ODatabaseSession session, ODocument iDocument, String iAllowFieldName, OIdentifiable iId);

  OUser authenticate(ODatabaseSession session, String iUsername, String iUserPassword);

  OUser createUser(
      ODatabaseSession session, String iUserName, String iUserPassword, String[] iRoles);

  OUser createUser(
      ODatabaseSession session, String iUserName, String iUserPassword, ORole[] iRoles);

  OUser authenticate(ODatabaseSession session, OToken authToken);

  ORole createRole(
      ODatabaseSession session,
      String iRoleName,
      ORole iParent,
      OSecurityRole.ALLOW_MODES iAllowMode);

  ORole createRole(
      ODatabaseSession session, String iRoleName, OSecurityRole.ALLOW_MODES iAllowMode);

  OUser getUser(ODatabaseSession session, String iUserName);

  OUser getUser(ODatabaseSession session, ORID userId);

  ORole getRole(ODatabaseSession session, String iRoleName);

  ORole getRole(ODatabaseSession session, OIdentifiable iRoleRid);

  List<ODocument> getAllUsers(ODatabaseSession session);

  List<ODocument> getAllRoles(ODatabaseSession session);

  Map<String, OSecurityPolicy> getSecurityPolicies(ODatabaseSession session, OSecurityRole role);

  /**
   * Returns the security policy policy assigned to a role for a specific resource (not recursive on
   * superclasses, nor on role hierarchy)
   *
   * @param session an active DB session
   * @param role the role
   * @param resource the string representation of the security resource, eg. "database.class.Person"
   * @return
   */
  OSecurityPolicy getSecurityPolicy(ODatabaseSession session, OSecurityRole role, String resource);

  /**
   * Sets a security policy for a specific resource on a role
   *
   * @param session a valid db session to perform the operation (that has permissions to do it)
   * @param role The role
   * @param resource the string representation of the security resource, eg. "database.class.Person"
   * @param policy The security policy
   */
  void setSecurityPolicy(
      ODatabaseSession session, OSecurityRole role, String resource, OSecurityPolicyImpl policy);

  /**
   * creates and saves an empty security policy
   *
   * @param session the session to a DB where the policy has to be created
   * @param name the policy name
   * @return
   */
  OSecurityPolicyImpl createSecurityPolicy(ODatabaseSession session, String name);

  OSecurityPolicyImpl getSecurityPolicy(ODatabaseSession session, String name);

  void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicyImpl policy);

  void deleteSecurityPolicy(ODatabaseSession session, String name);

  /**
   * Removes security policy bound to a role for a specific resource
   *
   * @param session A valid db session to perform the operation
   * @param role the role
   * @param resource the string representation of the security resource, eg. "database.class.Person"
   */
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
   * For property-level security. Returns the list of the properties that are hidden (ie. not
   * allowed to be read) for current session, regarding a specific document
   *
   * @param session the db session
   * @param document the document to filter
   * @return the list of the properties that are hidden (ie. not allowed to be read) on current
   *     document for current session
   */
  Set<String> getFilteredProperties(ODatabaseSession session, ODocument document);

  /**
   * For property-level security
   *
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

  /**
   * checks if for current session a resource is restricted by security resources (ie. READ policies
   * exist, with predicate different from "TRUE", to access the given resource
   *
   * @param session The session to check for the existece of policies
   * @param resource a resource string, eg. "database.class.Person"
   * @return true if a restriction of any type exists for this session and this resource. False
   *     otherwise
   */
  boolean isReadRestrictedBySecurityPolicy(ODatabaseSession session, String resource);

  /**
   * returns the list of all the filtered properties (for any role defined in the db)
   *
   * @param database
   * @return
   */
  Set<OSecurityResourceProperty> getAllFilteredProperties(ODatabaseDocumentInternal database);

  OSecurityUser securityAuthenticate(ODatabaseSession session, String userName, String password);

  OSecurityUser securityAuthenticate(
      ODatabaseSession session, OAuthenticationInfo authenticationInfo);
}
