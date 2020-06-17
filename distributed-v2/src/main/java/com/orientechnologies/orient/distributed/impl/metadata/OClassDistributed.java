package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/** Created by tglman on 22/06/17. */
public class OClassDistributed extends OClassEmbedded {

  private volatile int[] bestClusterIds;
  private volatile int lastVersion;

  protected OClassDistributed(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  public OClassDistributed(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  public OClassDistributed(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  @Override
  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyDistributed(this, p);
  }

  @Override
  protected OPropertyEmbedded createPropertyInstance(OGlobalProperty global) {
    return new OPropertyDistributed(this, global);
  }

  public OProperty addProperty(
      final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (type == null) throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    final ODatabaseDocumentInternal database = getDatabase();
    validatePropertyName(propertyName);
    if (database.getTransaction().isActive()) {
      throw new OSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null) OPropertyImpl.checkSupportLinkedClass(type);

    acquireSchemaWriteLock();
    try {
      if (isDistributedCommand(database)) {
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
        cmd.append(type.getName());

        if (linkedType != null) {
          // TYPE
          cmd.append(' ');
          cmd.append(linkedType.getName());

        } else if (linkedClass != null) {
          // TYPE
          cmd.append(' ');
          cmd.append('`');
          cmd.append(linkedClass.getName());
          cmd.append('`');
        }

        if (unsafe) cmd.append(" unsafe ");
        if (!isRunLocal(database)) {
          OScenarioThreadLocal.executeAsDistributed(
              (Callable<OProperty>)
                  () -> addPropertyInternal(propertyName, type, linkedType, linkedClass, unsafe));
        }

        owner.sendCommand(database, cmd.toString());

      } else
        return (OProperty)
            OScenarioThreadLocal.executeAsDistributed(
                (Callable<OProperty>)
                    () -> addPropertyInternal(propertyName, type, linkedType, linkedClass, unsafe));

    } finally {
      releaseSchemaWriteLock();
    }
    return getProperty(propertyName);
  }

  public OClassImpl setEncryption(final String iValue) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` encryption %s", name, iValue);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setEncryptionInternal(database, iValue);
        }
      } else setEncryptionInternal(database, iValue);
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
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setClusterSelectionInternal(value);
        }
      } else setClusterSelectionInternal(value);

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
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` custom `%s`=%s", getName(), name, value);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setCustomInternal(name, value);
        }
      } else setCustomInternal(name, value);

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
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` custom clear", getName());
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          clearCustomInternal();
        }
      } else clearCustomInternal();

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
      if (isDistributedCommand(database)) {
        final StringBuilder sb = new StringBuilder();
        if (classes != null && classes.size() > 0) {
          for (OClass superClass : classes) {
            sb.append('`').append(superClass.getName()).append("`,");
          }
          sb.deleteCharAt(sb.length() - 1);
        } else sb.append("null");

        final String cmd = String.format("alter class `%s` superclasses %s", name, sb);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setSuperClassesInternal(classes);
        }
      } else setSuperClassesInternal(classes);

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
      if (isDistributedCommand(database)) {
        final String cmd =
            String.format(
                "alter class `%s` superclass +`%s`",
                name, superClass != null ? superClass.getName() : null);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          addSuperClassInternal(database, superClass);
        }
      } else addSuperClassInternal(database, superClass);

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
      if (isDistributedCommand(database)) {
        final String cmd =
            String.format(
                "alter class `%s` superclass -`%s`",
                name, superClass != null ? superClass.getName() : null);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          removeSuperClassInternal(superClass);
        }
      } else removeSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass setName(final String name) {
    if (getName().equals(name)) return this;
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + name
              + "'");
    acquireSchemaWriteLock();
    try {

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` name `%s`", this.name, name);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setNameInternal(database, name);
        }
      } else setNameInternal(database, name);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) shortName = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setShortNameInternal(database, shortName);
        }
      } else setShortNameInternal(database, shortName);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  @Override
  public OClass truncateCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);

    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      if (isDistributedCommand(database)) {
        final String cmd = String.format("truncate cluster %s", clusterName);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          truncateClusterInternal(clusterName, database);
        }
      } else truncateClusterInternal(clusterName, database);
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

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` strictmode %s", name, isStrict);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setStrictModeInternal(isStrict);
        }
      } else setStrictModeInternal(isStrict);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setDescription(String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) iDescription = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` description ?", name);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setDescriptionInternal(iDescription);
        }
      } else setDescriptionInternal(iDescription);
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
      if (isDistributedCommand(database)) {

        final String cmd = String.format("alter class `%s` addcluster %d", name, clusterId);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          addClusterIdInternal(database, clusterId);
        }
      } else addClusterIdInternal(database, clusterId);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass removeClusterId(final int clusterId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0])
      throw new ODatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");

    acquireSchemaWriteLock();
    try {

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` removecluster %d", name, clusterId);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          removeClusterIdInternal(database, clusterId);
        }
      } else removeClusterIdInternal(database, clusterId);
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
        throw new OSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");

      if (isDistributedCommand(database)) {
        if (!isRunLocal(database)) {
          OScenarioThreadLocal.executeAsDistributed(
              (Callable<OProperty>)
                  () -> {
                    dropPropertyInternal(database, propertyName);
                    return null;
                  });
        }

        owner.sendCommand(database, "drop property " + name + '.' + propertyName);
      } else
        OScenarioThreadLocal.executeAsDistributed(
            (Callable<OProperty>)
                () -> {
                  dropPropertyInternal(database, propertyName);
                  return null;
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

      if (isDistributedCommand(database)) {
        if (!isRunLocal(database)) {
          final int clusterId = owner.createClusterIfNeeded(database, clusterNameOrId);
          addClusterIdInternal(database, clusterId);
        }

        final String cmd = String.format("alter class `%s` addcluster `%s`", name, clusterNameOrId);
        owner.sendCommand(database, cmd);
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

      if (isDistributedCommand(database)) {
        // FORMAT FLOAT LOCALE AGNOSTIC
        final String cmd =
            String.format("alter class `%s` oversize %s", name, new Float(overSize).toString());
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setOverSizeInternal(database, overSize);
        }
      } else setOverSizeInternal(database, overSize);
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

      if (isDistributedCommand(database)) {
        final String cmd = String.format("alter class `%s` abstract %s", name, isAbstract);
        owner.sendCommand(database, cmd);
        if (!isRunLocal(database)) {
          setAbstractInternal(database, isAbstract);
        }
      } else setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected boolean isDistributedCommand(ODatabaseDocumentInternal database) {
    return database.getStorage() instanceof OAutoshardedStorage
        && !((OAutoshardedStorage) database.getStorage()).isLocalEnv();
  }

  private boolean isRunLocal(ODatabaseDocumentInternal database) {
    return ((OSchemaDistributed) owner).isRunLocal(database);
  }
}
