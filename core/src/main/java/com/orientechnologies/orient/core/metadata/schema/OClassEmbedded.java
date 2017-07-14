package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 14/06/17.
 */
public class OClassEmbedded extends OClassImpl {
  protected OClassEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OClassEmbedded(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  protected OClassEmbedded(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  public OProperty addProperty(final String propertyName, final OType type, final OType linkedType, final OClass linkedClass,
      final boolean unsafe) {
    if (type == null)
      throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    final ODatabaseDocumentInternal database = getDatabase();
    validatePropertyName(propertyName);
    if (database.getTransaction().isActive()) {
      throw new OSchemaException("Cannot create property '" + propertyName + "' inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null)
      OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null)
      OPropertyImpl.checkSupportLinkedClass(type);

    acquireSchemaWriteLock();
    try {
      final StringBuilder cmd = new StringBuilder("create property ");
      // CLASS.PROPERTY NAME
      cmd.append('`');
      cmd.append(name);
      cmd.append('`');
      cmd.append('.');
      cmd.append('`');
      cmd.append(propertyName);
      cmd.append('`');

      // TYPE
      cmd.append(' ');
      cmd.append(type.name);

      if (linkedType != null) {
        // TYPE
        cmd.append(' ');
        cmd.append(linkedType.name);

      } else if (linkedClass != null) {
        // TYPE
        cmd.append(' ');
        cmd.append('`');
        cmd.append(linkedClass.getName());
        cmd.append('`');
      }

      if (unsafe)
        cmd.append(" unsafe ");

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final OProperty prop = (OProperty) OScenarioThreadLocal.executeAsDistributed(new Callable<OProperty>() {
          @Override
          public OProperty call() throws Exception {
            return addPropertyInternal(propertyName, type, linkedType, linkedClass, unsafe);
          }
        });

        final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        return prop;
      } else
        return (OProperty) OScenarioThreadLocal.executeAsDistributed(new Callable<OProperty>() {
          @Override
          public OProperty call() throws Exception {
            return addPropertyInternal(propertyName, type, linkedType, linkedClass, unsafe);
          }
        });

    } finally {
      releaseSchemaWriteLock();
    }
  }

  private boolean isDistributedCommand(ODatabaseDocumentInternal database) {
    return database.getStorage() instanceof OAutoshardedStorage && !((OAutoshardedStorage) database.getStorage()).isLocalEnv();
  }

  public OClassImpl setEncryption(final String iValue) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` encryption %s", name, iValue);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
        setEncryptionInternal(database, iValue);
      } else
        setEncryptionInternal(database, iValue);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass setClusterSelection(final String value) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
        OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(commandSQL).execute();

        setClusterSelectionInternal(value);
      } else
        setClusterSelectionInternal(value);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClassImpl setCustom(final String name, final String value) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` custom %s=%s", getName(), name, value);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setCustomInternal(name, value);
      } else
        setCustomInternal(name, value);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public void clearCustom() {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` custom clear", getName());
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(commandSQL).execute();

        clearCustomInternal();
      } else
        clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<OClass> toCheck = new ArrayList<OClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      final StringBuilder sb = new StringBuilder();
      if (classes != null && classes.size() > 0) {
        for (OClass superClass : classes) {
          sb.append('`').append(superClass.getName()).append("`,");
        }
        sb.deleteCharAt(sb.length() - 1);
      } else
        sb.append("null");

      final String cmd = String.format("alter class `%s` superclasses %s", name, sb);
      if (isDistributedCommand(database)) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setSuperClassesInternal(classes);
      } else
        setSuperClassesInternal(classes);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass addSuperClass(final OClass superClass) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkParametersConflict(superClass);
    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String
            .format("alter class `%s` superclass +`%s`", name, superClass != null ? superClass.getName() : null);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        addSuperClassInternal(database, superClass);
      } else
        addSuperClassInternal(database, superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String
            .format("alter class `%s` superclass -`%s`", name, superClass != null ? superClass.getName() : null);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        removeSuperClassInternal(superClass);
      } else
        removeSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass setName(final String name) {
    if (getName().equals(name))
      return this;
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error = String.format("Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + name + "'");
    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` name `%s`", this.name, name);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setNameInternal(database, name);
      } else
        setNameInternal(database, name);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty())
        shortName = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {

        final String cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setShortNameInternal(database, shortName);
      } else
        setShortNameInternal(database, shortName);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyEmbedded(this, p);
  }

  public OPropertyImpl addPropertyInternal(final String name, final OType type, final OType linkedType, final OClass linkedClass,
      final boolean unsafe) {
    if (name == null || name.length() == 0)
      throw new OSchemaException("Found property name null");

    if (!unsafe)
      checkPersistentPropertyType(getDatabase(), name, type);

    final OPropertyImpl prop;

    // This check are doubled because used by sql commands
    if (linkedType != null)
      OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null)
      OPropertyImpl.checkSupportLinkedClass(type);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (properties.containsKey(name))
        throw new OSchemaException("Class '" + this.name + "' already has property '" + name + "'");

      OGlobalProperty global = owner.findOrCreateGlobalProperty(name, type);

      prop = createPropertyInstance(global);

      properties.put(name, prop);

      if (linkedType != null)
        prop.setLinkedTypeInternal(linkedType);
      else if (linkedClass != null)
        prop.setLinkedClassInternal(linkedClass);
    } finally {
      releaseSchemaWriteLock();
    }

    if (prop != null && !unsafe)
      fireDatabaseMigration(getDatabase(), name, type);

    return prop;
  }

  protected OPropertyImpl createPropertyInstance(OGlobalProperty global) {
    return new OPropertyEmbedded(this, global);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OClass truncateCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);

    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("truncate cluster %s", clusterName);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
        truncateClusterInternal(clusterName, database);
      } else
        truncateClusterInternal(clusterName, database);
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public OClass setStrictMode(final boolean isStrict) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` strictmode %s", name, isStrict);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setStrictModeInternal(isStrict);
      } else
        setStrictModeInternal(isStrict);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setDescription(String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty())
        iDescription = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (isDistributedCommand(database)) {

        final String cmd = String.format("alter class `%s` description ?", name);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        final OStorage storage = database.getStorage();
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute(iDescription);
        setDescriptionInternal(iDescription);
      } else
        setDescriptionInternal(iDescription);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass addClusterId(final int clusterId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      if (isDistributedCommand(database)) {

        final String cmd = String.format("alter class `%s` addcluster %d", name, clusterId);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        addClusterIdInternal(database, clusterId);
      } else
        addClusterIdInternal(database, clusterId);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass removeClusterId(final int clusterId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0])
      throw new ODatabaseException(" Impossible to remove the last cluster of class '" + getName() + "' drop the class instead");

    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` removecluster %d", name, clusterId);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        removeClusterIdInternal(database, clusterId);
      } else
        removeClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public void dropProperty(final String propertyName) {
    final ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock();
    try {
      if (!properties.containsKey(propertyName))
        throw new OSchemaException("Property '" + propertyName + "' not found in class " + name + "'");

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        OScenarioThreadLocal.executeAsDistributed(new Callable<OProperty>() {
          @Override
          public OProperty call() throws Exception {
            dropPropertyInternal(database, propertyName);
            return null;
          }
        });

        final OCommandSQL commandSQL = new OCommandSQL("drop property " + name + '.' + propertyName);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
      } else
        OScenarioThreadLocal.executeAsDistributed(new Callable<OProperty>() {
          @Override
          public OProperty call() throws Exception {
            dropPropertyInternal(database, propertyName);
            return null;
          }
        });

    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass addCluster(final String clusterNameOrId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final int clusterId = owner.createClusterIfNeeded(database, clusterNameOrId);
        addClusterIdInternal(database, clusterId);

        final String cmd = String.format("alter class `%s` addcluster `%s`", name, clusterNameOrId);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
      } else {
        final int clusterId = owner.createClusterIfNeeded(database, clusterNameOrId);
        addClusterIdInternal(database, clusterId);
      }
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setOverSize(final float overSize) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();

      if (isDistributedCommand(database)) {
        // FORMAT FLOAT LOCALE AGNOSTIC
        final String cmd = String.format("alter class `%s` oversize %s", name, new Float(overSize).toString());
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setOverSizeInternal(database, overSize);
      } else
        setOverSizeInternal(database, overSize);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setAbstract(boolean isAbstract) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {

      final OStorage storage = database.getStorage();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` abstract %s", name, isAbstract);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setAbstractInternal(database, isAbstract);
      } else
        setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

}
