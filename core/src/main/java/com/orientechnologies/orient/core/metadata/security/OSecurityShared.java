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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.ONullOutputListener;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared security class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSecurityShared implements OSecurity, OCloseable {
  public static final String                             RESTRICTED_CLASSNAME   = "ORestricted";
  public static final String                             IDENTITY_CLASSNAME     = "OIdentity";
  public static final String                             ALLOW_ALL_FIELD        = "_allow";
  public static final String                             ALLOW_READ_FIELD       = "_allowRead";
  public static final String                             ALLOW_UPDATE_FIELD     = "_allowUpdate";
  public static final String                             ALLOW_DELETE_FIELD     = "_allowDelete";
  public static final String                             ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  public static final String                             ONCREATE_FIELD         = "onCreate.fields";

  protected final ConcurrentLinkedHashMap<String, OUser> cachedUsers;
  protected final ConcurrentLinkedHashMap<String, ORole> cachedRoles;

  public OSecurityShared() {
    final int maxCachedUsers = OGlobalConfiguration.SECURITY_MAX_CACHED_USERS.getValueAsInteger();
    if (maxCachedUsers > 0)
      cachedUsers = new ConcurrentLinkedHashMap.Builder<String, OUser>().maximumWeightedCapacity(maxCachedUsers).build();
    else
      cachedUsers = null;

    final int maxCachedRoles = OGlobalConfiguration.SECURITY_MAX_CACHED_ROLES.getValueAsInteger();
    if (maxCachedRoles > 0)
      cachedRoles = new ConcurrentLinkedHashMap.Builder<String, ORole>().maximumWeightedCapacity(maxCachedRoles).build();
    else
      cachedRoles = null;
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
          // CHECK inherited permissions from parent roles, fixes #1980: Record Level Security: permissions don't follow role's
          // inheritance
          ORole parentRole = r.getParentRole();
          while (parentRole != null) {
            if (iAllowAll.contains(parentRole.getDocument().getIdentity()))
              return true;
            if (iAllowOperation != null && iAllowOperation.contains(parentRole.getDocument().getIdentity()))
              return true;
            parentRole = parentRole.getParentRole();
          }
        }
        return false;
      }
    }
    return true;
  }

  public OUser authenticate(final String iUserName, final String iUserPassword) {
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
  }

  @Override
  public OSecurity uncacheUsersAndRoles() {
    if (cachedRoles != null)
      cachedRoles.clear();

    if (cachedUsers != null)
      cachedUsers.clear();
    return this;
  }

  public OUser getUser(final String iUserName) {
    return getUser(iUserName, true);
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final String... iRoles) {
    final OUser user = new OUser(iUserName, iUserPassword);

    if (iRoles != null)
      for (String r : iRoles) {
        user.addRole(r);
      }

    cacheUser(user);

    return user.save();
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final ORole... iRoles) {
    final OUser user = new OUser(iUserName, iUserPassword);

    if (iRoles != null)
      for (ORole r : iRoles) {
        user.addRole(r);
      }

    cacheUser(user);

    return user.save();
  }

  public boolean dropUser(final String iUserName) {
    uncacheUser(iUserName);

    final Number removed = getDatabase().<OCommandRequest> command(
        new OCommandSQL("delete from OUser where name = '" + iUserName + "'")).execute();

    return removed != null && removed.intValue() > 0;
  }

  public ORole getRole(final OIdentifiable iRole) {
    final ODocument doc = iRole.getRecord();
    if ("ORole".equals(doc.getClassName()))
      return new ORole(doc);

    return null;
  }

  public ORole getRole(final String iRoleName) {
    return getRole(iRoleName, true);
  }

  public ORole getRole(final String iRoleName, final boolean iAllowRepair) {
    if (cachedRoles != null) {
      final ORole role = cachedRoles.get(iRoleName);
      if (role != null)
        return role;
    }

    List<ODocument> result;

    try {
      result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from ORole where name = '" + iRoleName + "' limit 1")).execute();

      if (iRoleName.equalsIgnoreCase("admin") && result.isEmpty()) {
        if (iAllowRepair)
          repair();
        result = getDatabase().<OCommandRequest> command(
            new OSQLSynchQuery<ODocument>("select from ORole where name = '" + iRoleName + "' limit 1")).execute();
      }
    } catch (Exception e) {
      if (iAllowRepair)
        repair();
      result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from ORole where name = '" + iRoleName + "' limit 1").setFetchPlan("roles:1"))
          .execute();
    }

    if (result != null && !result.isEmpty())
      return cacheRole(new ORole(result.get(0)));

    return null;

  }

  public ORole createRole(final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
    return createRole(iRoleName, null, iAllowMode);
  }

  public ORole createRole(final String iRoleName, final ORole iParent, final ORole.ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(iRoleName, iParent, iAllowMode);
    cacheRole(role);
    return role.save();
  }

  public boolean dropRole(final String iRoleName) {
    uncacheRole(iRoleName);

    final Number removed = getDatabase().<OCommandRequest> command(
        new OCommandSQL("delete from ORole where name = '" + iRoleName + "'")).execute();

    return removed != null && removed.intValue() > 0;
  }

  public List<ODocument> getAllUsers() {
    try {
      return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from OUser")).execute();
    } catch (Exception e) {
      repair();
      return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from OUser")).execute();
    }
  }

  public List<ODocument> getAllRoles() {
    return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from ORole")).execute();
  }

  public OUser create() {
    if (!getDatabase().getMetadata().getSchema().getClasses().isEmpty())
      return null;

    final OUser adminUser = createMetadata();

    final ORole readerRole = createRole("reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
    readerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.CLUSTER + "." + OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".orole", ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".ouser", ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_READ);
    readerRole.addRule(ODatabaseSecurityResources.FUNCTION + ".*", ORole.PERMISSION_READ);
    readerRole.save();
    createUser("reader", "reader", new String[] { readerRole.getName() });

    final ORole writerRole = createRole("writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
    writerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
    writerRole
        .addRule(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE + ORole.PERMISSION_UPDATE);
    writerRole.addRule(ODatabaseSecurityResources.CLUSTER + "." + OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    writerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".orole", ORole.PERMISSION_READ);
    writerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".ouser", ORole.PERMISSION_READ);
    writerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_ALL);
    writerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_ALL);
    writerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_ALL);
    writerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_ALL);
    readerRole.addRule(ODatabaseSecurityResources.FUNCTION + ".*", ORole.PERMISSION_READ);
    writerRole.save();
    createUser("writer", "writer", new String[] { writerRole.getName() });

    return adminUser;
  }

  /**
   * Repairs the security structure if broken by creating the ADMIN role and user with default password.
   * 
   * @return
   */
  public OUser repair() {
    OLogManager.instance().warn(this, "Repairing security structures...");

    try {
      if (cachedUsers != null)
        cachedUsers.clear();
      getDatabase().getMetadata().getIndexManager().dropIndex("OUser.name");

      if (cachedRoles != null)
        cachedRoles.clear();
      getDatabase().getMetadata().getIndexManager().dropIndex("ORole.name");

      return createMetadata();

    } finally {
      OLogManager.instance().warn(this, "Repair completed");
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
      roleClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true).setCollate("ci");
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
      userClass.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true).setCollate("ci");
      userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }
    if (!userClass.existsProperty("password"))
      userClass.createProperty("password", OType.STRING).setMandatory(true).setNotNull(true);
    if (!userClass.existsProperty("roles"))
      userClass.createProperty("roles", OType.LINKSET, roleClass);
    if (!userClass.existsProperty("status"))
      userClass.createProperty("status", OType.STRING).setMandatory(true).setNotNull(true);

    // CREATE ROLES AND USERS
    ORole adminRole = getRole(ORole.ADMIN, false);
    if (adminRole == null) {
      adminRole = createRole(ORole.ADMIN, ORole.ALLOW_MODES.ALLOW_ALL_BUT);
      adminRole.addRule(ODatabaseSecurityResources.BYPASS_RESTRICTED, ORole.PERMISSION_ALL).save();
    }

    OUser adminUser = getUser(OUser.ADMIN, false);
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

  public void close(boolean onDelete) {
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

  public void createClassTrigger() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    OClass classTrigger = db.getMetadata().getSchema().getClass(OClassTrigger.CLASSNAME);
    if (classTrigger == null)
      classTrigger = db.getMetadata().getSchema().createAbstractClass(OClassTrigger.CLASSNAME);
  }

  @Override
  public OSecurity getUnderlying() {
    return this;
  }

  protected OUser getUser(final String iUserName, final boolean iAllowRepair) {

    if (cachedUsers != null) {
      final OUser user = cachedUsers.get(iUserName);
      if (user != null)
        return user;
    }

    List<ODocument> result;
    try {
      result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from OUser where name = '" + iUserName + "' limit 1").setFetchPlan("roles:1"))
          .execute();

      if (iUserName.equalsIgnoreCase("admin") && result.isEmpty()) {
        if (iAllowRepair)
          repair();
        result = getDatabase().<OCommandRequest> command(
            new OSQLSynchQuery<ODocument>("select from OUser where name = '" + iUserName + "' limit 1").setFetchPlan("roles:1"))
            .execute();
      }
    } catch (Exception e) {
      if (iAllowRepair)
        repair();
      result = getDatabase().<OCommandRequest> command(
          new OSQLSynchQuery<ODocument>("select from OUser where name = '" + iUserName + "' limit 1").setFetchPlan("roles:1"))
          .execute();
    }

    if (result != null && !result.isEmpty())
      return cacheUser(new OUser(result.get(0)));

    return null;

  }

  protected OUser cacheUser(final OUser user) {
    if (cachedUsers != null)
      cachedUsers.put(user.getName(), user);
    return user;
  }

  protected void uncacheUser(final String iName) {
    if (cachedUsers != null)
      cachedUsers.remove(iName);
  }

  protected void uncacheUsersOfRole(final String iName) {
    if (cachedUsers != null) {
      for (final Iterator<Map.Entry<String, OUser>> it = cachedUsers.entrySet().iterator(); it.hasNext();) {
        if (it.next().getValue().hasRole(iName, true))
          it.remove();
      }
    }
  }

  protected ORole cacheRole(final ORole iRole) {
    if (cachedRoles != null)
      cachedRoles.put(iRole.getName(), iRole);
    return iRole;
  }

  protected void uncacheRole(final String iName) {
    if (cachedRoles != null) {
      cachedRoles.remove(iName);
      uncacheUsersOfRole(iName);
    }
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
