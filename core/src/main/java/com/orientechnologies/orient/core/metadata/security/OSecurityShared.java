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
import com.orientechnologies.orient.core.index.ONullOutputListener;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser.STATUSES;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import java.util.*;
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
  public static final String IDENTITY_CLASSNAME   = "OIdentity";

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
  public static final String ONCREATE_FIELD         = "onCreate.fields";

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

    final OResultSet result = session.query("select rid from index:ORole.name where key = ? limit 1", iRoleName);

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

  public OUser create(final ODatabaseSession session) {
    if (!session.getMetadata().getSchema().getClasses().isEmpty())
      return null;

    final OUser adminUser = createMetadata(session);

    final ORole readerRole = createRole(session, "reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
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

    // This will return the global value if a local storage context configuration value does not exist.
    boolean createDefUsers = ((ODatabaseDocumentInternal) session).getStorage().getConfiguration().getContextConfiguration()
        .getValueAsBoolean(OGlobalConfiguration.CREATE_DEFAULT_USERS);

    if (createDefUsers)
      createUser(session, "reader", "reader", new String[] { readerRole.getName() });

    final ORole writerRole = createRole(session, "writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
    writerRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    writerRole
        .addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, OSequence.CLASS_NAME, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OTriggered", ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OSchedule", ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
    writerRole.save();

    if (createDefUsers)
      createUser(session, "writer", "writer", new String[] { writerRole.getName() });

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

    OClass roleClass = createOrUpdateORoleClass(session, identityClass);

    createOrUpdateOUserClass(session, identityClass, roleClass);

    // CREATE ROLES AND USERS
    ORole adminRole = getRole(session, ORole.ADMIN);
    if (adminRole == null) {
      adminRole = createRole(session, ORole.ADMIN, ORole.ALLOW_MODES.ALLOW_ALL_BUT);
      adminRole.addRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL).save();
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

  public ORID getUserRID(final ODatabaseSession session, final String iUserName) {
    return (ORID) OScenarioThreadLocal.executeAsDistributed(() -> {
      try (OResultSet result = session.query("select rid from index:OUser.name where key = ? limit 1", iUserName)) {

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
  }

  @Override
  public Set<String> getFilteredProperties(ODocument document) {
    return null;
  }

  @Override
  public boolean isAllowedWrite(ODocument document, String name) {
    return true;
  }
}
