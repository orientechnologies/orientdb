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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.ONullOutputListener;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser.STATUSES;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Shared security class. It's shared by all the database instances that point to the same storage.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSecurityShared implements OSecurityInternal {
  private final AtomicLong version = new AtomicLong();

  public static final String RESTRICTED_CLASSNAME = "ORestricted";
  public static final String IDENTITY_CLASSNAME = "OIdentity";

  protected Map<String, Map<String, OBooleanExpression>> securityPredicateCache = new ConcurrentHashMap<>();

  /**
   * set of all the security resources defined on properties (used for optimizations)
   */
  protected Set<OSecurityResourceProperty> filteredProperties;

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_ALL_FIELD = ORestrictedOperation.ALLOW_ALL.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_READ_FIELD = ORestrictedOperation.ALLOW_READ.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_UPDATE_FIELD = ORestrictedOperation.ALLOW_UPDATE.getFieldName();

  /**
   * Uses the ORestrictedOperation ENUM instead.
   */
  @Deprecated
  public static final String ALLOW_DELETE_FIELD = ORestrictedOperation.ALLOW_DELETE.getFieldName();

  public static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  public static final String ONCREATE_FIELD = "onCreate.fields";

  public static final Set<String> ALLOW_FIELDS = Collections.unmodifiableSet(new HashSet<String>() {
    {
      add(ORestrictedOperation.ALLOW_ALL.getFieldName());
      add(ORestrictedOperation.ALLOW_READ.getFieldName());
      add(ORestrictedOperation.ALLOW_UPDATE.getFieldName());
      add(ORestrictedOperation.ALLOW_DELETE.getFieldName());
    }
  });

  public OSecurityShared() {
  }

  @Override
  public OIdentifiable allowRole(final ODatabaseSession session, final ODocument iDocument, final ORestrictedOperation iOperation,
                                 final String iRoleName) {
    final ORID role = getRoleRID(session, iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return allowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  @Override
  public OIdentifiable allowUser(final ODatabaseSession session, final ODocument iDocument, final ORestrictedOperation iOperation,
                                 final String iUserName) {
    final ORID user = getUserRID(session, iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return allowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  public OIdentifiable allowIdentity(final ODatabaseSession session, final ODocument iDocument, final String iAllowFieldName,
                                     final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new ORecordLazySet(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);

    return iId;
  }

  @Override
  public OIdentifiable denyUser(final ODatabaseSession session, final ODocument iDocument, final ORestrictedOperation iOperation,
                                final String iUserName) {
    final ORID user = getUserRID(session, iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), user);
  }

  @Override
  public OIdentifiable denyRole(final ODatabaseSession session, final ODocument iDocument, final ORestrictedOperation iOperation,
                                final String iRoleName) {
    final ORID role = getRoleRID(session, iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return disallowIdentity(session, iDocument, iOperation.getFieldName(), role);
  }

  public OIdentifiable disallowIdentity(final ODatabaseSession session, final ODocument iDocument, final String iAllowFieldName,
                                        final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null)
      field.remove(iId);
    return iId;
  }

  @Override
  public boolean isAllowed(final ODatabaseSession session, final Set<OIdentifiable> iAllowAll,
                           final Set<OIdentifiable> iAllowOperation) {
    if ((iAllowAll == null || iAllowAll.isEmpty()) && (iAllowOperation == null || iAllowOperation.isEmpty()))
      // NO AUTHORIZATION: CAN'T ACCESS
      return false;

    final OSecurityUser currentUser = ODatabaseRecordThreadLocal.instance().get().getUser();
    if (currentUser != null) {
      // CHECK IF CURRENT USER IS ENLISTED
      if (iAllowAll == null || (iAllowAll != null && !iAllowAll.contains(currentUser.getIdentity()))) {
        // CHECK AGAINST SPECIFIC _ALLOW OPERATION
        if (iAllowOperation != null && iAllowOperation.contains(currentUser.getIdentity()))
          return true;

        // CHECK IF AT LEAST ONE OF THE USER'S ROLES IS ENLISTED
        for (OSecurityRole r : currentUser.getRoles()) {
          // CHECK AGAINST GENERIC _ALLOW
          if (iAllowAll != null && iAllowAll.contains(r.getIdentity()))
            return true;
          // CHECK AGAINST SPECIFIC _ALLOW OPERATION
          if (iAllowOperation != null && iAllowOperation.contains(r.getIdentity()))
            return true;
          // CHECK inherited permissions from parent roles, fixes #1980: Record Level Security: permissions don't follow role's
          // inheritance
          OSecurityRole parentRole = r.getParentRole();
          while (parentRole != null) {
            if (iAllowAll != null && iAllowAll.contains(parentRole.getIdentity()))
              return true;
            if (iAllowOperation != null && iAllowOperation.contains(parentRole.getIdentity()))
              return true;
            parentRole = parentRole.getParentRole();
          }
        }
        return false;
      }
    }
    return true;
  }

  public OUser authenticate(final ODatabaseSession session, final String iUserName, final String iUserPassword) {
    final String dbName = session.getName();
    final OUser user = getUser(session, iUserName);
    if (user == null)
      throw new OSecurityAccessException(dbName, "User or password not valid for database: '" + dbName + "'");

    if (user.getAccountStatus() != OSecurityUser.STATUSES.ACTIVE)
      throw new OSecurityAccessException(dbName, "User '" + iUserName + "' is not active");

    if (!(((ODatabaseDocumentInternal) session).getStorage() instanceof OStorageProxy)) {
      // CHECK USER & PASSWORD
      if (!user.checkPassword(iUserPassword)) {
        // WAIT A BIT TO AVOID BRUTE FORCE
        try {
          Thread.sleep(200);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
        }
        throw new OSecurityAccessException(dbName, "User or password not valid for database: '" + dbName + "'");
      }
    }

    return user;
  }

  // Token MUST be validated before being passed to this method.
  public OUser authenticate(final ODatabaseSession session, final OToken authToken) {
    final String dbName = session.getName();
    if (authToken.getIsValid() != true) {
      throw new OSecurityAccessException(dbName, "Token not valid");
    }

    OUser user = authToken.getUser((ODatabaseDocumentInternal) session);
    if (user == null && authToken.getUserName() != null) {
      // Token handler may not support returning an OUser so let's get username (subject) and query:
      user = getUser(session, authToken.getUserName());
    }

    if (user == null) {
      throw new OSecurityAccessException(dbName, "Authentication failed, could not load user from token");
    }
    if (user.getAccountStatus() != STATUSES.ACTIVE)
      throw new OSecurityAccessException(dbName, "User '" + user.getName() + "' is not active");

    return user;
  }

  public OUser getUser(final ODatabaseSession session, final ORID iRecordId) {
    if (iRecordId == null)
      return null;

    ODocument result;
    result = session.load(iRecordId, "roles:1");
    if (!result.getClassName().equals(OUser.CLASS_NAME)) {
      result = null;
    }
    return new OUser(result);
  }

  public OUser createUser(final ODatabaseSession session, final String iUserName, final String iUserPassword,
                          final String... iRoles) {
    final OUser user = new OUser(iUserName, iUserPassword);

    if (iRoles != null)
      for (String r : iRoles) {
        user.addRole(r);
      }

    return user.save();
  }

  public OUser createUser(final ODatabaseSession session, final String userName, final String userPassword, final ORole... roles) {
    final OUser user = new OUser(userName, userPassword);

    if (roles != null)
      for (ORole r : roles) {
        user.addRole(r);
      }

    return user.save();
  }

  public boolean dropUser(final ODatabaseSession session, final String iUserName) {
    final Number removed = session.command("delete from OUser where name = ?", iUserName).next().getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  public ORole getRole(final ODatabaseSession session, final OIdentifiable iRole) {
    final ODocument doc = iRole.getRecord();
    if (doc != null) {
      OClass clazz = doc.getSchemaClass();
      if (clazz != null && clazz.isSubClassOf("ORole")) {
        return new ORole(doc);
      }
    }

    return null;
  }

  public ORole getRole(final ODatabaseSession session, final String iRoleName) {
    if (iRoleName == null)
      return null;

    final OResultSet result = session.query("select from ORole where name = ? limit 1", iRoleName);

    if (result.hasNext())
      return new ORole((ODocument) result.next().getElement().get());

    return null;
  }

  public ORID getRoleRID(final ODatabaseSession session, final String iRoleName) {
    if (iRoleName == null)
      return null;

    final OResultSet result = session.query("select @rid as rid from ORole where name = ? limit 1", iRoleName);

    if (result.hasNext())
      return result.next().getProperty("rid");

    return null;
  }

  public ORole createRole(final ODatabaseSession session, final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
    return createRole(session, iRoleName, null, iAllowMode);
  }

  public ORole createRole(final ODatabaseSession session, final String iRoleName, final ORole iParent,
                          final ORole.ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(iRoleName, iParent, iAllowMode);
    return role.save();
  }

  public boolean dropRole(final ODatabaseSession session, final String iRoleName) {
    final Number removed = session.command("delete from ORole where name = '" + iRoleName + "'").next().getProperty("count");

    return removed != null && removed.intValue() > 0;
  }

  public List<ODocument> getAllUsers(final ODatabaseSession session) {
    return session.query("select from OUser").stream().map((e) -> (ODocument) e.getElement().get()).collect(Collectors.toList());
  }

  public List<ODocument> getAllRoles(final ODatabaseSession session) {
    return session.query("select from ORole").stream().map((e) -> (ODocument) e.getElement().get()).collect(Collectors.toList());
  }

  @Override
  public Map<String, OSecurityPolicy> getSecurityPolicies(ODatabaseSession session, OSecurityRole role) {
    Map<String, OSecurityPolicy> result = new HashMap<>();

//    OElement roleDoc = session.reload(role.getDocument(), null, false);
    OElement roleDoc = role.getDocument();
    if (roleDoc == null) {
      return result;
    }
    Map<String, OIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      return result;
    }
    policies.entrySet().forEach(x -> result.put(x.getKey(), new OSecurityPolicy(x.getValue().getRecord())));
    return result;
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(ODatabaseSession session, OSecurityRole role, String resource) {
    resource = normalizeSecurityResource(session, resource);
    OElement roleDoc = session.reload(role.getDocument(), null, false);
    if (roleDoc == null) {
      return null;
    }
    Map<String, OIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      return null;
    }
    OIdentifiable entry = policies.get(resource);
    if (entry == null) {
      return null;
    }
    return new OSecurityPolicy(entry.getRecord());
  }

  public void setSecurityPolicyWithBitmask(ODatabaseSession session, OSecurityRole role, String resource, int legacyPolicy) {
    String policyName = "default_" + legacyPolicy;
    OSecurityPolicy policy = getSecurityPolicy(session, policyName);
    if (policy == null) {
      policy = createSecurityPolicy(session, policyName);
      policy.setCreateRule((legacyPolicy & ORole.PERMISSION_CREATE) > 0 ? "true" : "false");
      policy.setReadRule((legacyPolicy & ORole.PERMISSION_READ) > 0 ? "true" : "false");
      policy.setBeforeUpdateRule((legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
      policy.setAfterUpdateRule((legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
      policy.setDeleteRule((legacyPolicy & ORole.PERMISSION_DELETE) > 0 ? "true" : "false");
      policy.setExecuteRule((legacyPolicy & ORole.PERMISSION_EXECUTE) > 0 ? "true" : "false");
      saveSecurityPolicy(session, policy);
    }
    setSecurityPolicy(session, role, resource, policy);
  }

  @Override
  public void setSecurityPolicy(ODatabaseSession session, OSecurityRole role, String resource, OSecurityPolicy policy) {
    resource = normalizeSecurityResource(session, resource);
    OElement roleDoc = session.load(role.getDocument().getIdentity());
    if (roleDoc == null) {
      return;
    }
    validatePolicyWithIndexes(session, resource);
    Map<String, OIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      policies = new HashMap<>();
      roleDoc.setProperty("policies", policies);
    }
    policies.put(resource, policy.getElement());
    roleDoc = session.save(roleDoc);
    if (role instanceof ORole) {
      ((ORole) role).reload();
    }
    updateAllFilteredProperties((ODatabaseDocumentInternal) session);
  }

  private void validatePolicyWithIndexes(ODatabaseSession session, String resource) throws IllegalArgumentException {
    OSecurityResource res = OSecurityResource.getInstance(resource);
    if (res instanceof OSecurityResourceProperty) {
      String clazzName = ((OSecurityResourceProperty) res).getClassName();
      OClass clazz = session.getClass(clazzName);
      if (clazz == null) {
        return;
      }
      Set<OClass> allClasses = new HashSet<>();
      allClasses.add(clazz);
      allClasses.addAll(clazz.getAllSubclasses());
      allClasses.addAll(clazz.getAllSuperClasses());
      for (OClass c : allClasses) {
        for (OIndex index : c.getIndexes()) {
          List<String> indexFields = index.getDefinition().getFields();
          if (indexFields.size() > 1 && indexFields.contains(((OSecurityResourceProperty) res).getPropertyName())) {
            throw new IllegalArgumentException("Cannot bind security policy on " + resource + " because of existing composite indexes: " + index.getName());
          }
        }
      }
    }
  }

  @Override
  public OSecurityPolicy createSecurityPolicy(ODatabaseSession session, String name) {
    OElement elem = session.newElement(OSecurityPolicy.class.getSimpleName());
    elem.setProperty("name", name);
    OSecurityPolicy policy = new OSecurityPolicy(elem);
    saveSecurityPolicy(session, policy);
    return policy;
  }

  @Override
  public OSecurityPolicy getSecurityPolicy(ODatabaseSession session, String name) {
    try (OResultSet rs = session.query("SELECT FROM " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", name)) {
      if (rs.hasNext()) {
        OResult result = rs.next();
        return new OSecurityPolicy(result.getElement().get());
      }
    }
    return null;
  }

  @Override
  public void saveSecurityPolicy(ODatabaseSession session, OSecurityPolicy policy) {
    session.save(policy.getElement(), OSecurityPolicy.class.getSimpleName().toLowerCase(Locale.ENGLISH));
  }

  @Override
  public void deleteSecurityPolicy(ODatabaseSession session, String name) {
    session.command("DELETE FROM " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", name);
  }

  @Override
  public void removeSecurityPolicy(ODatabaseSession session, ORole role, String resource) {
    resource = normalizeSecurityResource(session, resource);
    OElement roleDoc = session.reload(role.getDocument(), null, false);
    if (roleDoc == null) {
      return;
    }
    Map<String, OIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      return;
    }
    policies.remove(resource);
    roleDoc.save();
    role.reload();
    updateAllFilteredProperties((ODatabaseDocumentInternal) session);
  }

  private String normalizeSecurityResource(ODatabaseSession session, String resource) {
    return resource; //TODO
  }

  public OUser create(final ODatabaseSession session) {
    if (!session.getMetadata().getSchema().getClasses().isEmpty())
      return null;

    final OUser adminUser = createMetadata(session);

    final ORole readerRole = createRole(session, "reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
    setSecurityPolicyWithBitmask(session, readerRole, "database.class.*.*", ORole.PERMISSION_ALL);

    readerRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);

    readerRole.save();

    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.DATABASE.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.SCHEMA.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + "." + OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".orole", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".ouser", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLASS.getLegacyName() + ".*", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLASS.getLegacyName() + ".OUser", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".*", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.COMMAND.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.RECORD_HOOK.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.FUNCTION.getLegacyName() + ".*", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, readerRole, ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName(), ORole.PERMISSION_NONE);


    // This will return the global value if a local storage context configuration value does not exist.
    boolean createDefUsers = ((ODatabaseDocumentInternal) session).getStorage().getConfiguration().getContextConfiguration()
            .getValueAsBoolean(OGlobalConfiguration.CREATE_DEFAULT_USERS);

    if (createDefUsers)
      createUser(session, "reader", "reader", new String[]{readerRole.getName()});

    final ORole writerRole = createRole(session, "writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
    setSecurityPolicyWithBitmask(session, writerRole, "database.class.*.*", ORole.PERMISSION_ALL);

    writerRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    writerRole
            .addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, OSequence.CLASS_NAME, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OTriggered", ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OSchedule", ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, OSecurityResource.class.getSimpleName(), ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
    writerRole.save();

    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.DATABASE.getLegacyName(), ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.SCHEMA.getLegacyName(), ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + "." + OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".orole", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".ouser", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLASS.getLegacyName() + ".*", ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLASS.getLegacyName() + ".OUser", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLUSTER.getLegacyName() + ".*", ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.COMMAND.getLegacyName(), ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.RECORD_HOOK.getLegacyName(), ORole.PERMISSION_ALL);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.FUNCTION.getLegacyName() + ".*", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.CLASS.getLegacyName() + "." + OSequence.CLASS_NAME, ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName() + ".OTriggered", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName() + ".OSchedule", ORole.PERMISSION_READ);
    setSecurityPolicyWithBitmask(session, writerRole, ORule.ResourceGeneric.SYSTEM_CLUSTERS.getLegacyName(), ORole.PERMISSION_NONE);

    if (createDefUsers)
      createUser(session, "writer", "writer", new String[]{writerRole.getName()});

    return adminUser;
  }

  /**
   * Repairs the security structure if broken by creating the ADMIN role and user with default password.
   *
   * @return
   */

  public OUser createMetadata(final ODatabaseSession session) {

    OClass identityClass = session.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME); // SINCE 1.2.0
    if (identityClass == null)
      identityClass = session.getMetadata().getSchema().createAbstractClass(OIdentity.CLASS_NAME);

    createOrUpdateOSecurityPolicyClass(session);

    OClass roleClass = createOrUpdateORoleClass(session, identityClass);

    createOrUpdateOUserClass(session, identityClass, roleClass);

    // CREATE ROLES AND USERS
    ORole adminRole = getRole(session, ORole.ADMIN);
    if (adminRole == null) {
      adminRole = createRole(session, ORole.ADMIN, ORole.ALLOW_MODES.DENY_ALL_BUT);
      setSecurityPolicyWithBitmask(session, adminRole, "*", ORole.PERMISSION_ALL);
      adminRole.addRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.ALL, null, ORole.PERMISSION_ALL).save();
//      adminRole.addRule(ORule.ResourceGeneric.ALL_CLASSES, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL).save();
//      adminRole.addRule(ORule.ResourceGeneric.ALL_CLUSTERS, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.COMMAND_GREMLIN, null, ORole.PERMISSION_ALL).save();
      adminRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_ALL).save();
    }

    OUser adminUser = getUser(session, OUser.ADMIN);

    if (adminUser == null) {
      // This will return the global value if a local storage context configuration value does not exist.
      boolean createDefUsers = ((ODatabaseDocumentInternal) session).getStorage().getConfiguration().getContextConfiguration()
              .getValueAsBoolean(OGlobalConfiguration.CREATE_DEFAULT_USERS);

      if (createDefUsers) {
        adminUser = createUser(session, OUser.ADMIN, OUser.ADMIN, adminRole);
      }
    }

    // SINCE 1.2.0
    createOrUpdateORestrictedClass(session);

    return adminUser;
  }

  private void createOrUpdateORestrictedClass(final ODatabaseDocument database) {
    OClass restrictedClass = database.getMetadata().getSchema().getClass(RESTRICTED_CLASSNAME);
    boolean unsafe = false;
    if (restrictedClass == null) {
      restrictedClass = database.getMetadata().getSchema().createAbstractClass(RESTRICTED_CLASSNAME);
      unsafe = true;
    }
    if (!restrictedClass.existsProperty(ALLOW_ALL_FIELD))
      restrictedClass
              .createProperty(ALLOW_ALL_FIELD, OType.LINKSET, database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME),
                      unsafe);
    if (!restrictedClass.existsProperty(ALLOW_READ_FIELD))
      restrictedClass
              .createProperty(ALLOW_READ_FIELD, OType.LINKSET, database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME),
                      unsafe);
    if (!restrictedClass.existsProperty(ALLOW_UPDATE_FIELD))
      restrictedClass
              .createProperty(ALLOW_UPDATE_FIELD, OType.LINKSET, database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME),
                      unsafe);
    if (!restrictedClass.existsProperty(ALLOW_DELETE_FIELD))
      restrictedClass
              .createProperty(ALLOW_DELETE_FIELD, OType.LINKSET, database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME),
                      unsafe);
  }

  private void createOrUpdateOUserClass(final ODatabaseDocument database, OClass identityClass, OClass roleClass) {
    boolean unsafe = false;
    OClass userClass = database.getMetadata().getSchema().getClass("OUser");
    if (userClass == null) {
      userClass = database.getMetadata().getSchema().createClass("OUser", identityClass);
      unsafe = true;
    } else if (!userClass.getSuperClasses().contains(identityClass))
      // MIGRATE AUTOMATICALLY TO 1.2.0
      userClass.setSuperClasses(Arrays.asList(identityClass));

    if (!userClass.existsProperty("name")) {
      ((OClassImpl) userClass).createProperty("name", OType.STRING, (OType) null, unsafe).setMandatory(true).setNotNull(true)
              .setCollate("ci").setMin("1").setRegexp("\\S+(.*\\S+)*");
      userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final OProperty name = userClass.getProperty("name");
      if (name.getAllIndexes().isEmpty())
        userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }
    if (!userClass.existsProperty(OUser.PASSWORD_FIELD))
      userClass.createProperty(OUser.PASSWORD_FIELD, OType.STRING, (OType) null, unsafe).setMandatory(true).setNotNull(true);
    if (!userClass.existsProperty("roles"))
      userClass.createProperty("roles", OType.LINKSET, roleClass, unsafe);
    if (!userClass.existsProperty("status"))
      userClass.createProperty("status", OType.STRING, (OType) null, unsafe).setMandatory(true).setNotNull(true);
  }

  private OClass createOrUpdateOSecurityPolicyClass(final ODatabaseDocument database) {
    OClass policyClass = database.getMetadata().getSchema().getClass("OSecurityPolicy");
    boolean unsafe = false;
    if (policyClass == null) {
      policyClass = database.getMetadata().getSchema().createClass("OSecurityPolicy");
      unsafe = true;
    }

    if (!policyClass.existsProperty("name")) {
      policyClass.createProperty("name", OType.STRING, (OType) null, unsafe).setMandatory(true).setNotNull(true).setCollate("ci");
      policyClass.createIndex("OSecurityPolicy.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final OProperty name = policyClass.getProperty("name");
      if (name.getAllIndexes().isEmpty())
        policyClass.createIndex("OSecurityPolicy.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }

    if (!policyClass.existsProperty("create"))
      policyClass.createProperty("create", OType.STRING, (OType) null, unsafe);
    if (!policyClass.existsProperty("read"))
      policyClass.createProperty("read", OType.STRING, (OType) null, unsafe);
    if (!policyClass.existsProperty("beforeUpdate"))
      policyClass.createProperty("beforeUpdate", OType.STRING, (OType) null, unsafe);
    if (!policyClass.existsProperty("afterUpdate"))
      policyClass.createProperty("afterUpdate", OType.STRING, (OType) null, unsafe);
    if (!policyClass.existsProperty("delete"))
      policyClass.createProperty("delete", OType.STRING, (OType) null, unsafe);
    if (!policyClass.existsProperty("execute"))
      policyClass.createProperty("execute", OType.STRING, (OType) null, unsafe);

    if (!policyClass.existsProperty("active"))
      policyClass.createProperty("active", OType.BOOLEAN, (OType) null, unsafe);

    return policyClass;
  }

  private OClass createOrUpdateORoleClass(final ODatabaseDocument database, OClass identityClass) {
    OClass roleClass = database.getMetadata().getSchema().getClass("ORole");
    boolean unsafe = false;
    if (roleClass == null) {
      roleClass = database.getMetadata().getSchema().createClass("ORole", identityClass);
      unsafe = true;
    } else if (!roleClass.getSuperClasses().contains(identityClass))
      // MIGRATE AUTOMATICALLY TO 1.2.0
      roleClass.setSuperClasses(Arrays.asList(identityClass));

    if (!roleClass.existsProperty("name")) {
      roleClass.createProperty("name", OType.STRING, (OType) null, unsafe).setMandatory(true).setNotNull(true).setCollate("ci");
      roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final OProperty name = roleClass.getProperty("name");
      if (name.getAllIndexes().isEmpty())
        roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }

    if (!roleClass.existsProperty("mode"))
      roleClass.createProperty("mode", OType.BYTE, (OType) null, unsafe);

    if (!roleClass.existsProperty("rules"))
      roleClass.createProperty("rules", OType.EMBEDDEDMAP, OType.BYTE, unsafe);
    if (!roleClass.existsProperty("inheritedRole"))
      roleClass.createProperty("inheritedRole", OType.LINK, roleClass, unsafe);

    if (!roleClass.existsProperty("policies"))
      roleClass.createProperty("policies", OType.LINKMAP, database.getClass("OSecurityPolicy"), unsafe);

    return roleClass;
  }

  public void load(ODatabaseSession session) {
    final OClass userClass = session.getMetadata().getSchema().getClass("OUser");
    if (userClass != null) {
      // @COMPATIBILITY <1.3.0
      if (!userClass.existsProperty("status")) {
        userClass.createProperty("status", OType.STRING).setMandatory(true).setNotNull(true);
      }
      OProperty p = userClass.getProperty("name");
      if (p == null)
        p = userClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true).setMin("1")
                .setRegexp("\\S+(.*\\S+)*");

      if (userClass.getInvolvedIndexes("name") == null)
        p.createIndex(INDEX_TYPE.UNIQUE);

      // ROLE
      final OClass roleClass = session.getMetadata().getSchema().getClass("ORole");

      final OProperty rules = roleClass.getProperty("rules");
      if (rules != null && !OType.EMBEDDEDMAP.equals(rules.getType())) {
        roleClass.dropProperty("rules");
      }

      if (!roleClass.existsProperty("inheritedRole")) {
        roleClass.createProperty("inheritedRole", OType.LINK, roleClass);
      }

      p = roleClass.getProperty("name");
      if (p == null)
        p = roleClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true);

      if (roleClass.getInvolvedIndexes("name") == null)
        p.createIndex(INDEX_TYPE.UNIQUE);

      //TODO create OSecurityPolicy
      //TODO migrate ORole to use security policies
    }
  }

  public void createClassTrigger(ODatabaseSession session) {
    OClass classTrigger = session.getMetadata().getSchema().getClass(OClassTrigger.CLASSNAME);
    if (classTrigger == null)
      classTrigger = session.getMetadata().getSchema().createAbstractClass(OClassTrigger.CLASSNAME);
  }

  @Override
  public OUser getUser(final ODatabaseSession session, final String iUserName) {
    return (OUser) OScenarioThreadLocal.executeAsDistributed(() -> {
      try (OResultSet result = session.query("select from OUser where name = ? limit 1", iUserName)) {
        if (result.hasNext())
          return new OUser((ODocument) result.next().getElement().get());

      }
      return null;
    });
  }

  public ORID getUserRID(final ODatabaseSession session, final String userName) {
    return (ORID) OScenarioThreadLocal.executeAsDistributed(() -> {
      try (OResultSet result = session.query("select @rid as rid from OUser where name = ? limit 1", userName)) {

        if (result.hasNext())
          return result.next().getProperty("rid");
      }

      return null;
    });
  }

  @Override
  public void close() {
  }

  @Override
  public long getVersion(final ODatabaseSession session) {
    return version.get();
  }

  @Override
  public void incrementVersion(final ODatabaseSession session) {
    version.incrementAndGet();
    securityPredicateCache.clear();
    updateAllFilteredProperties((ODatabaseDocumentInternal) session);
  }

  @Override
  public Set<String> getFilteredProperties(ODatabaseSession session, ODocument document) {
    if (session.getUser() == null) {
      return Collections.emptySet();
    }
    if (OSecurityPolicy.class.getSimpleName().equalsIgnoreCase(document.getClassName())) {
      return Collections.emptySet();
    }
    if (document.getClassName() == null) {
      return Collections.emptySet();
    }
    Set<String> props = document.getPropertyNames();
    Set<String> result = new HashSet<>();
    OClass schemaType = ((OElement) document).getSchemaType().orElse(null);
    if (schemaType == null) {
      return Collections.emptySet();
    }
    for (String prop : props) {
      OBooleanExpression predicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + schemaType.getName() + "`.`" + prop + "`", OSecurityPolicy.Scope.READ);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document)) {
        result.add(prop);
      }
    }
    return result;
//    return Collections.emptySet();
  }

  @Override
  public boolean isAllowedWrite(ODatabaseSession session, ODocument document, String propertyName) {
//    return true; //TODO

    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }

    OClass clazz = ((OElement) document).getSchemaType().orElse(null);
    if (clazz == null) {
      return true;
    }

    if (document.getIdentity().isNew()) {
      OBooleanExpression predicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + clazz.getName() + "`.`" + propertyName + "`", OSecurityPolicy.Scope.CREATE);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document);
    } else {

      OBooleanExpression readPredicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + clazz.getName() + "`.`" + propertyName + "`", OSecurityPolicy.Scope.READ);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, readPredicate, document)) {
        return false;
      }

      OBooleanExpression beforePredicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + clazz.getName() + "`.`" + propertyName + "`", OSecurityPolicy.Scope.BEFORE_UPDATE);
      OResultInternal originalRecord = calculateOriginalValue(document);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, beforePredicate, originalRecord)) {
        return false;
      }

      OBooleanExpression predicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + clazz.getName() + "`.`" + propertyName + "`", OSecurityPolicy.Scope.AFTER_UPDATE);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, document);
    }

  }

  @Override
  public boolean canCreate(ODatabaseSession session, ORecord record) {
    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }

    if (record instanceof OElement) {
      String className = record instanceof ODocument ? ((ODocument) record).getClassName() : ((OElement) record).getSchemaType().map(x -> x.getName()).orElse(null);
      OBooleanExpression predicate = className == null ? null : OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + className + "`", OSecurityPolicy.Scope.CREATE);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  @Override
  public boolean canRead(ODatabaseSession session, ORecord record) {
    //TODO what about server users?
    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }
    if (record instanceof OElement) {
      if (OSecurityPolicy.class.getSimpleName().equalsIgnoreCase(((ODocument) record).getClassName())) {
        return true;
      }
      if (((ODocument) record).getClassName() == null) {
        return true;
      }
    }

    if (record instanceof OElement) {
      OBooleanExpression predicate = ((OElement) record).getSchemaType()
              .map(x -> OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + x.getName() + "`", OSecurityPolicy.Scope.READ)).orElse(null);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  @Override
  public boolean canUpdate(ODatabaseSession session, ORecord record) {
    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }
    if (record instanceof OElement) {
      OBooleanExpression beforePredicate = ((OElement) record).getSchemaType()
              .map(x -> OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + x.getName() + "`", OSecurityPolicy.Scope.BEFORE_UPDATE)).orElse(null);
      OResultInternal originalRecord = calculateOriginalValue(record);
      if (!OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, beforePredicate, originalRecord)) {
        return false;
      }

      OBooleanExpression predicate = ((OElement) record).getSchemaType()
              .map(x -> OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + x.getName() + "`", OSecurityPolicy.Scope.AFTER_UPDATE)).orElse(null);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }

  private OResultInternal calculateOriginalValue(ORecord record) {
    return OLiveQueryHookV2.calculateBefore(record.getRecord());
  }

  @Override
  public boolean canDelete(ODatabaseSession session, ORecord record) {
    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }
    if (record instanceof OElement) {
      OBooleanExpression predicate = ((OElement) record).getSchemaType()
              .map(x -> OSecurityEngine.getPredicateForSecurityResource(session, this, "database.class.`" + x.getName() + "`", OSecurityPolicy.Scope.DELETE)).orElse(null);
      return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, record);
    }
    return true;
  }


  @Override
  public boolean canExecute(ODatabaseSession session, OFunction function) {
    if (session.getUser() == null) {
      //executeNoAuth
      return true;
    }
    OBooleanExpression predicate = OSecurityEngine.getPredicateForSecurityResource(session, this, "database.function." + function.getName(), OSecurityPolicy.Scope.EXECUTE);
    return OSecurityEngine.evaluateSecuirtyPolicyPredicate(session, predicate, function.getDocument());
  }


  protected OBooleanExpression getPredicateFromCache(String roleName, String key) {
    Map<String, OBooleanExpression> roleMap = this.securityPredicateCache.get(roleName);
    if (roleMap == null) {
      return null;
    }
    OBooleanExpression result = roleMap.get(key.toLowerCase(Locale.ENGLISH));
    if (result != null) {
      return result.copy();
    }
    return null;
  }

  protected void putPredicateInCache(String roleName, String key, OBooleanExpression predicate) {
    if (predicate.isCacheable()) {
      Map<String, OBooleanExpression> roleMap = this.securityPredicateCache.get(roleName);
      if (roleMap == null) {
        roleMap = new ConcurrentHashMap<>();
        this.securityPredicateCache.put(roleName, roleMap);
      }

      roleMap.put(key.toLowerCase(Locale.ENGLISH), predicate);
    }
  }

  @Override
  public boolean isReadRestrictedBySecurityPolicy(ODatabaseSession session, String resource) {
    if (session.getUser() == null) {
      //executeNoAuth
      return false;
    }
    OBooleanExpression predicate = OSecurityEngine.getPredicateForSecurityResource(session, this, resource, OSecurityPolicy.Scope.READ);
    if (predicate == null || OBooleanExpression.TRUE.equals(predicate)) {
      return false;
    }
    return true;
  }

  @Override
  public synchronized Set<OSecurityResourceProperty> getAllFilteredProperties(ODatabaseDocumentInternal database) {
    if (filteredProperties == null) {
      updateAllFilteredProperties(database);
    }
    if (filteredProperties == null) {
      return Collections.emptySet();
    }
    return new HashSet<>(filteredProperties);
  }

  protected void updateAllFilteredProperties(ODatabaseDocumentInternal session) {
    try {
      Set<OSecurityResourceProperty> result;
      if (session.getUser() == null) {
        result = calculateAllFilteredProperties(session);
      } else {
        result = session.getSharedContext().getOrientDB()
                .executeNoAuthorization(session.getName(), (db -> calculateAllFilteredProperties(db))).get();
      }
      synchronized (this) {
        filteredProperties = result;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected Set<OSecurityResourceProperty> calculateAllFilteredProperties(ODatabaseSession session) {
    Set<OSecurityResourceProperty> result = new HashSet<>();
    if (session.getClass("ORole") == null) {
      return Collections.emptySet();
    }
    OResultSet rs = session.query("select policies from ORole");
    while (rs.hasNext()) {
      OResult item = rs.next();
      Map<String, OIdentifiable> policies = item.getProperty("policies");
      if (policies != null) {
        for (Map.Entry<String, OIdentifiable> policyEntry : policies.entrySet()) {
          try {
            OSecurityResource res = OSecurityResource.getInstance(policyEntry.getKey());
            if (res instanceof OSecurityResourceProperty) {
              OSecurityPolicy policy = new OSecurityPolicy(policyEntry.getValue().getRecord());
              String readRule = policy.getReadRule();
              if (readRule != null && !readRule.trim().equalsIgnoreCase("true")) {
                result.add((OSecurityResourceProperty) res);
              }
            }
          } catch (Exception e) {
          }
        }
      }
    }
    rs.close();
    return result;
  }
}
