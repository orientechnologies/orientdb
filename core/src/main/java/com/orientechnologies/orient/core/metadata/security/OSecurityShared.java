/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.security;

import java.util.List;
import java.util.Set;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.ONullOutputListener;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OUser.STATUSES;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Shared security class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSecurityShared extends OSharedResourceAdaptive implements OSecurity, OCloseable {
  public static final String RESTRICTED_CLASSNAME   = "ORestricted";
  public static final String IDENTITY_CLASSNAME     = "OIdentity";
  public static final String ALLOW_ALL_FIELD        = "_allow";
  public static final String ALLOW_READ_FIELD       = "_allowRead";
  public static final String ALLOW_UPDATE_FIELD     = "_allowUpdate";
  public static final String ALLOW_DELETE_FIELD     = "_allowDelete";
  public static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  public static final String ONCREATE_FIELD         = "onCreate.fields";

  public OSecurityShared() {
  }

  public OIdentifiable allowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    final OUser user = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getUser(iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return allowIdentity(iDocument, iAllowFieldName, user.getDocument().getIdentity());
  }

  public OIdentifiable allowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    final ORole role = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getRole(iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return allowIdentity(iDocument, iAllowFieldName, role.getDocument().getIdentity());
  }

  public OIdentifiable allowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new OMVRBTreeRIDSet(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);
    return iId;
  }

  public OIdentifiable disallowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    final OUser user = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getUser(iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return disallowIdentity(iDocument, iAllowFieldName, user.getDocument().getIdentity());
  }

  public OIdentifiable disallowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    final ORole role = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getRole(iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return disallowIdentity(iDocument, iAllowFieldName, role.getDocument().getIdentity());
  }

  public OIdentifiable disallowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null)
      field.remove(iId);
    return iId;
  }

  public boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation) {
    if (iAllowAll == null || iAllowAll.isEmpty())
      return true;

    final OUser currentUser = ODatabaseRecordThreadLocal.INSTANCE.get().getUser();
    if (currentUser != null) {
      // CHECK IF CURRENT USER IS ENLISTED
      if (!iAllowAll.contains(currentUser.getDocument().getIdentity())) {
        // CHECK AGAINST SPECIFIC _ALLOW OPERATION
        if (iAllowOperation != null && iAllowOperation.contains(currentUser.getDocument().getIdentity()))
          return true;

        // CHECK IF AT LEAST ONE OF THE USER'S ROLES IS ENLISTED
        for (ORole r : currentUser.getRoles()) {
          // CHECK AGAINST GENERIC _ALLOW
          if (iAllowAll.contains(r.getDocument().getIdentity()))
            return true;
          // CHECK AGAINST SPECIFIC _ALLOW OPERATION
          if (iAllowOperation != null && iAllowOperation.contains(r.getDocument().getIdentity()))
            return true;
        }
        return false;
      }
    }
    return true;
  }

  public OUser authenticate(final String iUserName, final String iUserPassword) {
    acquireExclusiveLock();
    try {

      final String dbName = getDatabase().getName();

      final OUser user = getUser(iUserName);
      if (user == null)
        throw new OSecurityAccessException(dbName, "User or password not valid for database: '" + dbName + "'");

      if (user.getAccountStatus() != STATUSES.ACTIVE)
        throw new OSecurityAccessException(dbName, "User '" + iUserName + "' is not active");

      if (!(getDatabase().getStorage() instanceof OStorageProxy)) {
        // CHECK USER & PASSWORD
        if (!user.checkPassword(iUserPassword)) {
          // WAIT A BIT TO AVOID BRUTE FORCE
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          throw new OSecurityAccessException(dbName, "User or password not valid for database: '" + dbName + "'");
        }
      }

      return user;

    } finally {
      releaseExclusiveLock();
    }
  }

  public OUser getUser(final String iUserName) {
    acquireExclusiveLock();
    try {

      final List<ODocument> result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from OUser where name = '" + iUserName + "' limit 1").setFetchPlan("*:-1"))
          .execute();

      if (result != null && !result.isEmpty())
        return new OUser(result.get(0));

      return null;

    } finally {
      releaseExclusiveLock();
    }
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final String... iRoles) {
    acquireExclusiveLock();
    try {

      final OUser user = new OUser(iUserName, iUserPassword);

      if (iRoles != null)
        for (String r : iRoles) {
          user.addRole(r);
        }

      return user.save();

    } finally {
      releaseExclusiveLock();
    }
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final ORole... iRoles) {
    acquireExclusiveLock();
    try {

      final OUser user = new OUser(iUserName, iUserPassword);

      if (iRoles != null)
        for (ORole r : iRoles) {
          user.addRole(r);
        }

      return user.save();

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean dropUser(final String iUserName) {
    acquireExclusiveLock();
    try {

      final Number removed = getDatabase().<OCommandRequest> command(
          new OCommandSQL("delete from OUser where name = '" + iUserName + "'")).execute();

      return removed != null && removed.intValue() > 0;

    } finally {
      releaseExclusiveLock();
    }
  }

  public ORole getRole(final OIdentifiable iRole) {
    acquireExclusiveLock();
    try {

      final ODocument doc = iRole.getRecord();
      if ("ORole".equals(doc.getClassName()))
        return new ORole(doc);

      return null;

    } finally {
      releaseExclusiveLock();
    }
  }

  public ORole getRole(final String iRoleName) {
    acquireExclusiveLock();
    try {

      final List<ODocument> result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from ORole where name = '" + iRoleName + "' limit 1").setFetchPlan("*:-1"))
          .execute();

      if (result != null && !result.isEmpty())
        return new ORole(result.get(0));

      return null;

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Failed to get role : " + iRoleName + " " + ex.getMessage());
      return null;
    } finally {
      releaseExclusiveLock();
    }
  }

  public ORole createRole(final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
    return createRole(iRoleName, null, iAllowMode);
  }

  public ORole createRole(final String iRoleName, final ORole iParent, final ORole.ALLOW_MODES iAllowMode) {
    acquireExclusiveLock();
    try {

      final ORole role = new ORole(iRoleName, iParent, iAllowMode);
      return role.save();

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean dropRole(final String iRoleName) {
    acquireExclusiveLock();
    try {

      final Number removed = getDatabase().<OCommandRequest> command(
          new OCommandSQL("delete from ORole where name = '" + iRoleName + "'")).execute();

      return removed != null && removed.intValue() > 0;

    } finally {
      releaseExclusiveLock();
    }
  }

  public List<ODocument> getAllUsers() {
    acquireExclusiveLock();
    try {

      return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from OUser")).execute();

    } finally {
      releaseExclusiveLock();
    }
  }

  public List<ODocument> getAllRoles() {
    acquireExclusiveLock();
    try {

      return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from ORole")).execute();

    } finally {
      releaseExclusiveLock();
    }
  }

  public OUser create() {
    acquireExclusiveLock();
    try {

      if (!getDatabase().getMetadata().getSchema().getClasses().isEmpty())
        return null;

      final OUser adminUser = createMetadata();

      final ORole readerRole = createRole("reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
      readerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.CLUSTER + "." + OMetadata.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".orole", ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".ouser", ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
      readerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_READ);
      readerRole.save();
      createUser("reader", "reader", new String[] { readerRole.getName() });

      final ORole writerRole = createRole("writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
      writerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
      writerRole.addRule(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE
          + ORole.PERMISSION_UPDATE);
      writerRole.addRule(ODatabaseSecurityResources.CLUSTER + "." + OMetadata.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
      writerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".orole", ORole.PERMISSION_READ);
      writerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".ouser", ORole.PERMISSION_READ);
      writerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_ALL);
      writerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_ALL);
      writerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_ALL);
      writerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_ALL);
      writerRole.save();
      createUser("writer", "writer", new String[] { writerRole.getName() });

      return adminUser;

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Repairs the security structure if broken by creating the ADMIN role and user with default password.
   * 
   * @return
   */
  public OUser repair() {
    acquireExclusiveLock();
    try {

      getDatabase().getMetadata().getIndexManager().dropIndex("OUser.name");
      getDatabase().getMetadata().getIndexManager().dropIndex("ORole.name");

      return createMetadata();

    } finally {
      releaseExclusiveLock();
    }
  }

  public OUser createMetadata() {
    final ODatabaseRecord database = getDatabase();

    OClass identityClass = database.getMetadata().getSchema().getClass(IDENTITY_CLASSNAME); // SINCE 1.2.0
    if (identityClass == null)
      identityClass = database.getMetadata().getSchema().createAbstractClass(IDENTITY_CLASSNAME);

    OClass roleClass = database.getMetadata().getSchema().getClass("ORole");
    if (roleClass == null)
      roleClass = database.getMetadata().getSchema().createClass("ORole", identityClass);
    else if (roleClass.getSuperClass() == null)
      // MIGRATE AUTOMATICALLY TO 1.2.0
      roleClass.setSuperClass(identityClass);

    if (!roleClass.existsProperty("name")) {
      roleClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true);
      roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final Set<OIndex<?>> indexes = roleClass.getInvolvedIndexes("name");
      if (indexes.isEmpty())
        roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }

    if (!roleClass.existsProperty("mode"))
      roleClass.createProperty("mode", OType.BYTE);
    if (!roleClass.existsProperty("rules"))
      roleClass.createProperty("rules", OType.EMBEDDEDMAP, OType.BYTE);
    if (!roleClass.existsProperty("inheritedRole"))
      roleClass.createProperty("inheritedRole", OType.LINK, roleClass);

    OClass userClass = database.getMetadata().getSchema().getClass("OUser");
    if (userClass == null)
      userClass = database.getMetadata().getSchema().createClass("OUser", identityClass);
    else if (userClass.getSuperClass() == null)
      // MIGRATE AUTOMATICALLY TO 1.2.0
      userClass.setSuperClass(identityClass);

    if (!userClass.existsProperty("name")) {
      userClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true);
      userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }
    if (!userClass.existsProperty("password"))
      userClass.createProperty("password", OType.STRING).setMandatory(true).setNotNull(true);
    if (!userClass.existsProperty("roles"))
      userClass.createProperty("roles", OType.LINKSET, roleClass);
    if (!userClass.existsProperty("status"))
      userClass.createProperty("status", OType.STRING).setMandatory(true).setNotNull(true);

    // CREATE ROLES AND USERS
    ORole adminRole = getRole(ORole.ADMIN);
    if (adminRole == null)
      adminRole = createRole(ORole.ADMIN, ORole.ALLOW_MODES.ALLOW_ALL_BUT);

    OUser adminUser = getUser(OUser.ADMIN);
    if (adminUser == null)
      adminUser = createUser(OUser.ADMIN, OUser.ADMIN, adminRole);

    // SINCE 1.2.0
    OClass restrictedClass = database.getMetadata().getSchema().getClass(RESTRICTED_CLASSNAME);
    if (restrictedClass == null)
      restrictedClass = database.getMetadata().getSchema().createAbstractClass(RESTRICTED_CLASSNAME);
    if (!restrictedClass.existsProperty(ALLOW_ALL_FIELD))
      restrictedClass.createProperty(ALLOW_ALL_FIELD, OType.LINKSET, database.getMetadata().getSchema()
          .getClass(IDENTITY_CLASSNAME));
    if (!restrictedClass.existsProperty(ALLOW_READ_FIELD))
      restrictedClass.createProperty(ALLOW_READ_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(IDENTITY_CLASSNAME));
    if (!restrictedClass.existsProperty(ALLOW_UPDATE_FIELD))
      restrictedClass.createProperty(ALLOW_UPDATE_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(IDENTITY_CLASSNAME));
    if (!restrictedClass.existsProperty(ALLOW_DELETE_FIELD))
      restrictedClass.createProperty(ALLOW_DELETE_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(IDENTITY_CLASSNAME));

    return adminUser;
  }

  public void close() {
  }

  public void load() {
    final OClass userClass = getDatabase().getMetadata().getSchema().getClass("OUser");
    if (userClass != null) {
      // @COMPATIBILITY <1.3.0
      if (!userClass.existsProperty("status")) {
        userClass.createProperty("status", OType.STRING).setMandatory(true).setNotNull(true);
      }
      OProperty p = userClass.getProperty("name");
      if (p == null)
        p = userClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true);

      if (userClass.getInvolvedIndexes("name") == null)
        p.createIndex(INDEX_TYPE.UNIQUE);

      // ROLE
      final OClass roleClass = getDatabase().getMetadata().getSchema().getClass("ORole");
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

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public void createClassTrigger() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    OClass classTrigger = db.getMetadata().getSchema().getClass(OClassTrigger.CLASSNAME);
    if (classTrigger == null)
      classTrigger = db.getMetadata().getSchema().createAbstractClass(OClassTrigger.CLASSNAME);
  }
}
