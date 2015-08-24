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
package com.orientechnologies.orient.core.metadata.security;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorageProxy;

/**
 * Shared security class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSecurityShared implements OSecurity, OCloseable {
  private final AtomicLong        version      = new AtomicLong();

  @SuppressWarnings("serial")
  public static final Set<String> ALLOW_FIELDS = new HashSet<String>() {
                                                 {
                                                   add(ALLOW_ALL_FIELD);
                                                   add(ALLOW_DELETE_FIELD);
                                                   add(ALLOW_READ_FIELD);
                                                   add(ALLOW_UPDATE_FIELD);
                                                 }
                                               };

  public OSecurityShared() {
  }

  public OIdentifiable allowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    final ORID user = getUserRID(iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return allowIdentity(iDocument, iAllowFieldName, user);
  }

  public OIdentifiable allowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    final ORID role = getRoleRID(iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return allowIdentity(iDocument, iAllowFieldName, role);
  }

  public OIdentifiable allowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field == null) {
      field = new ORecordLazySet(iDocument);
      iDocument.field(iAllowFieldName, field);
    }
    field.add(iId);

    return iId;
  }

  public OIdentifiable disallowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName) {
    final ORID user = getUserRID(iUserName);
    if (user == null)
      throw new IllegalArgumentException("User '" + iUserName + "' not found");

    return disallowIdentity(iDocument, iAllowFieldName, user);
  }

  public OIdentifiable disallowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName) {
    final ORID role = getRoleRID(iRoleName);
    if (role == null)
      throw new IllegalArgumentException("Role '" + iRoleName + "' not found");

    return disallowIdentity(iDocument, iAllowFieldName, role);
  }

  public OIdentifiable disallowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId) {
    Set<OIdentifiable> field = iDocument.field(iAllowFieldName);
    if (field != null)
      field.remove(iId);
    return iId;
  }

  public boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation) {
    if ((iAllowAll == null || iAllowAll.isEmpty()) && (iAllowOperation == null || iAllowOperation.isEmpty()))
      // NO AUTHORIZATION: CAN'T ACCESS
      return false;

    final OSecurityUser currentUser = ODatabaseRecordThreadLocal.INSTANCE.get().getUser();
    if (currentUser != null) {
      // CHECK IF CURRENT USER IS ENLISTED
      if (!iAllowAll.contains(currentUser.getIdentity())) {
        // CHECK AGAINST SPECIFIC _ALLOW OPERATION
        if (iAllowOperation != null && iAllowOperation.contains(currentUser.getIdentity()))
          return true;

        // CHECK IF AT LEAST ONE OF THE USER'S ROLES IS ENLISTED
        for (OSecurityRole r : currentUser.getRoles()) {
          // CHECK AGAINST GENERIC _ALLOW
          if (iAllowAll.contains(r.getIdentity()))
            return true;
          // CHECK AGAINST SPECIFIC _ALLOW OPERATION
          if (iAllowOperation != null && iAllowOperation.contains(r.getIdentity()))
            return true;
          // CHECK inherited permissions from parent roles, fixes #1980: Record Level Security: permissions don't follow role's
          // inheritance
          OSecurityRole parentRole = r.getParentRole();
          while (parentRole != null) {
            if (iAllowAll.contains(parentRole.getIdentity()))
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

  public OUser authenticate(final String iUserName, final String iUserPassword) {
    final String dbName = getDatabase().getName();
    final OUser user = getUser(iUserName);
    if (user == null)
      throw new OSecurityAccessException(dbName, "User or password not valid for database: '" + dbName + "'");

    if (user.getAccountStatus() != OSecurityUser.STATUSES.ACTIVE)
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

  // Token MUST be validated before being passed to this method.
  public OUser authenticate(final OToken authToken) {
    final String dbName = getDatabase().getName();
    if (authToken.getIsValid() != true) {
      throw new OSecurityAccessException(dbName, "Token not valid");
    }

    OUser user = authToken.getUser(getDatabase());
    if (user == null && authToken.getUserName() != null) {
      // Token handler may not support returning an OUser so let's get username (subject) and query:
      user = getUser(authToken.getUserName());
    }

    if (user == null) {
      throw new OSecurityAccessException(dbName, "Authentication failed, could not load user from token");
    }
    if (user.getAccountStatus() != STATUSES.ACTIVE)
      throw new OSecurityAccessException(dbName, "User '" + user.getName() + "' is not active");

    return user;
  }

  public OUser getUser(final ORID iRecordId) {
    if (iRecordId == null)
      return null;

    ODocument result;
    result = getDatabase().load(iRecordId, "roles:1");
    if (!result.getClassName().equals(OUser.CLASS_NAME)) {
      result = null;
    }
    return new OUser(result);
  }

  public OUser createUser(final String iUserName, final String iUserPassword, final String... iRoles) {
    final OUser user = new OUser(iUserName, iUserPassword);

    if (iRoles != null)
      for (String r : iRoles) {
        user.addRole(r);
      }

    return user.save();
  }

  public OUser createUser(final String userName, final String userPassword, final ORole... roles) {
    final OUser user = new OUser(userName, userPassword);

    if (roles != null)
      for (ORole r : roles) {
        user.addRole(r);
      }

    return user.save();
  }

  public boolean dropUser(final String iUserName) {
    final Number removed = getDatabase().<OCommandRequest> command(new OCommandSQL("delete from OUser where name = ?")).execute(
        iUserName);

    return removed != null && removed.intValue() > 0;
  }

  public ORole getRole(final OIdentifiable iRole) {
    final ODocument doc = iRole.getRecord();
    if (doc != null && "ORole".equals(doc.getClassName()))
      return new ORole(doc);

    return null;
  }

  public ORole getRole(final String iRoleName) {
    if (iRoleName == null)
      return null;

    final List<ODocument> result = getDatabase().<OCommandRequest> command(
        new OSQLSynchQuery<ODocument>("select from ORole where name = ? limit 1")).execute(iRoleName);

    if (result != null && !result.isEmpty())
      return new ORole(result.get(0));

    return null;
  }

  public ORID getRoleRID(final String iRoleName) {
    if (iRoleName == null)
      return null;

    final List<ODocument> result = getDatabase().<OCommandRequest> command(
        new OSQLSynchQuery<ODocument>("select rid from index:ORole.name where key = ? limit 1")).execute(iRoleName);

    if (result != null && !result.isEmpty())
      return result.get(0).rawField("rid");

    return null;
  }

  public ORole createRole(final String iRoleName, final ORole.ALLOW_MODES iAllowMode) {
    return createRole(iRoleName, null, iAllowMode);
  }

  public ORole createRole(final String iRoleName, final ORole iParent, final ORole.ALLOW_MODES iAllowMode) {
    final ORole role = new ORole(iRoleName, iParent, iAllowMode);
    return role.save();
  }

  public boolean dropRole(final String iRoleName) {
    final Number removed = getDatabase().<OCommandRequest> command(
        new OCommandSQL("delete from ORole where name = '" + iRoleName + "'")).execute();

    return removed != null && removed.intValue() > 0;
  }

  public List<ODocument> getAllUsers() {
    return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from OUser")).execute();
  }

  public List<ODocument> getAllRoles() {
    return getDatabase().<OCommandRequest> command(new OSQLSynchQuery<ODocument>("select from ORole")).execute();
  }

  public OUser create() {
    if (!getDatabase().getMetadata().getSchema().getClasses().isEmpty())
      return null;

    final OUser adminUser = createMetadata();

    final ORole readerRole = createRole("reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
    readerRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
    readerRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    readerRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
    readerRole.save();

    createUser("reader", "reader", new String[] { readerRole.getName() });

    final ORole writerRole = createRole("writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
    writerRole.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ + ORole.PERMISSION_CREATE
        + ORole.PERMISSION_UPDATE);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, OMetadataDefault.CLUSTER_INTERNAL_NAME, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
    writerRole.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.RECORD_HOOK, null, ORole.PERMISSION_ALL);
    writerRole.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_READ);
    writerRole.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
    writerRole.save();

    createUser("writer", "writer", new String[] { writerRole.getName() });

    return adminUser;
  }

  /**
   * Repairs the security structure if broken by creating the ADMIN role and user with default password.
   * 
   * @return
   */

  public OUser createMetadata() {
    final ODatabaseDocument database = getDatabase();

    OClass identityClass = database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME); // SINCE 1.2.0
    if (identityClass == null)
      identityClass = database.getMetadata().getSchema().createAbstractClass(OIdentity.CLASS_NAME);

    OClass roleClass = createOrUpdateORoleClass(database, identityClass);

    createOrUpdateOUserClass(database, identityClass, roleClass);

    // CREATE ROLES AND USERS
    ORole adminRole = getRole(ORole.ADMIN);
    if (adminRole == null) {
      adminRole = createRole(ORole.ADMIN, ORole.ALLOW_MODES.ALLOW_ALL_BUT);
      adminRole.addRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL).save();
    }

    OUser adminUser = getUser(OUser.ADMIN);
    if (adminUser == null)
      adminUser = createUser(OUser.ADMIN, OUser.ADMIN, adminRole);

    // SINCE 1.2.0
    createOrUpdateORestrictedClass(database);

    return adminUser;
  }

  private void createOrUpdateORestrictedClass(final ODatabaseDocument database) {
    OClass restrictedClass = database.getMetadata().getSchema().getClass(RESTRICTED_CLASSNAME);
    boolean checkData = true;
    if (restrictedClass == null) {
      restrictedClass = database.getMetadata().getSchema().createAbstractClass(RESTRICTED_CLASSNAME);
      checkData = false;
    }
    if (!restrictedClass.existsProperty(ALLOW_ALL_FIELD))
      ((OClassImpl) restrictedClass).createProperty(ALLOW_ALL_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), checkData);
    if (!restrictedClass.existsProperty(ALLOW_READ_FIELD))
      ((OClassImpl) restrictedClass).createProperty(ALLOW_READ_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), checkData);
    if (!restrictedClass.existsProperty(ALLOW_UPDATE_FIELD))
      ((OClassImpl) restrictedClass).createProperty(ALLOW_UPDATE_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), checkData);
    if (!restrictedClass.existsProperty(ALLOW_DELETE_FIELD))
      ((OClassImpl) restrictedClass).createProperty(ALLOW_DELETE_FIELD, OType.LINKSET,
          database.getMetadata().getSchema().getClass(OIdentity.CLASS_NAME), checkData);
  }

  private void createOrUpdateOUserClass(final ODatabaseDocument database, OClass identityClass, OClass roleClass) {
    boolean checkData = true;
    OClass userClass = database.getMetadata().getSchema().getClass("OUser");
    if (userClass == null) {
      userClass = database.getMetadata().getSchema().createClass("OUser", identityClass);
      checkData = false;
    } else if (!userClass.getSuperClasses().contains(identityClass))
      // MIGRATE AUTOMATICALLY TO 1.2.0
      userClass.setSuperClasses(Arrays.asList(identityClass));

    if (!userClass.existsProperty("name")) {
      ((OClassImpl) userClass).createProperty("name", OType.STRING, (OType) null, checkData).setMandatory(true).setNotNull(true)
          .setCollate("ci");
      userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final OProperty name = userClass.getProperty("name");
      if (name.getAllIndexes().isEmpty())
        userClass.createIndex("OUser.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }
    if (!userClass.existsProperty("password"))
      ((OClassImpl) userClass).createProperty("password", OType.STRING, (OType) null, checkData).setMandatory(true)
          .setNotNull(true);
    if (!userClass.existsProperty("roles"))
      ((OClassImpl) userClass).createProperty("roles", OType.LINKSET, roleClass, checkData);
    if (!userClass.existsProperty("status"))
      ((OClassImpl) userClass).createProperty("status", OType.STRING, (OType) null, checkData).setMandatory(true).setNotNull(true);
  }

  private OClass createOrUpdateORoleClass(final ODatabaseDocument database, OClass identityClass) {
    OClass roleClass = database.getMetadata().getSchema().getClass("ORole");
    boolean checkData = true;
    if (roleClass == null) {
      roleClass = database.getMetadata().getSchema().createClass("ORole", identityClass);
      checkData = false;
    } else if (!roleClass.getSuperClasses().contains(identityClass))
      // MIGRATE AUTOMATICALLY TO 1.2.0
      roleClass.setSuperClasses(Arrays.asList(identityClass));

    if (!roleClass.existsProperty("name")) {
      ((OClassImpl) roleClass).createProperty("name", OType.STRING, (OType) null, checkData).setMandatory(true).setNotNull(true)
          .setCollate("ci");
      roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    } else {
      final OProperty name = roleClass.getProperty("name");
      if (name.getAllIndexes().isEmpty())
        roleClass.createIndex("ORole.name", INDEX_TYPE.UNIQUE, ONullOutputListener.INSTANCE, "name");
    }

    if (!roleClass.existsProperty("mode"))
      ((OClassImpl) roleClass).createProperty("mode", OType.BYTE, (OType) null, checkData);

    if (!roleClass.existsProperty("rules"))
      ((OClassImpl) roleClass).createProperty("rules", OType.EMBEDDEDMAP, OType.BYTE, checkData);
    if (!roleClass.existsProperty("inheritedRole"))
      ((OClassImpl) roleClass).createProperty("inheritedRole", OType.LINK, roleClass, checkData);
    return roleClass;
  }

  @Override
  public void close() {
  }

  @Override
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

  public void createClassTrigger() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    OClass classTrigger = db.getMetadata().getSchema().getClass(OClassTrigger.CLASSNAME);
    if (classTrigger == null)
      classTrigger = db.getMetadata().getSchema().createAbstractClass(OClassTrigger.CLASSNAME);
  }

  @Override
  public OSecurity getUnderlying() {
    return this;
  }

  public OUser getUser(final String iUserName) {
    List<ODocument> result = getDatabase().<OCommandRequest> command(
        new OSQLSynchQuery<ODocument>("select from OUser where name = ? limit 1").setFetchPlan("roles:1")).execute(iUserName);

    if (result != null && !result.isEmpty())
      return new OUser(result.get(0));

    return null;
  }

  public ORID getUserRID(final String iUserName) {
    List<ODocument> result = getDatabase().<OCommandRequest> command(
        new OSQLSynchQuery<ODocument>("select rid from index:OUser.name where key = ? limit 1")).execute(iUserName);

    if (result != null && !result.isEmpty())
      return result.get(0).rawField("rid");

    return null;
  }

  @Override
  public long getVersion() {
    return version.get();
  }

  @Override
  public void incrementVersion() {
    version.incrementAndGet();
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
