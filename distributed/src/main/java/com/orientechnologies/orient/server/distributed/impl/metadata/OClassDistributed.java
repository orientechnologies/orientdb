package com.orientechnologies.orient.server.distributed.impl.metadata;

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
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
        final String cmd =
            String.format("alter class `%s` custom `%s`='%s'", getName(), name, value);
        owner.sendCommand(database, cmd);
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
      } else setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  @Override
  public int getClusterForNewInstance(ODocument doc) {
    return getClusterForNewInstance((ODatabaseDocumentDistributed) getDatabase(), doc);
  }

  public int getClusterForNewInstance(ODatabaseDocumentDistributed db, ODocument doc) {
    ODistributedServerManager manager = db.getDistributedManager();
    if (bestClusterIds == null) readConfiguration(db, manager);
    else {
      ODistributedConfiguration cfg = manager.getDatabaseConfiguration(db.getName());
      if (lastVersion != cfg.getVersion()) {
        // DISTRIBUTED CFG IS CHANGED: GET BEST CLUSTER AGAIN
        readConfiguration(db, manager);

        ODistributedServerLog.info(
            this,
            manager.getLocalNodeName(),
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "New cluster list for class '%s': %s (dCfgVersion=%d)",
            getName(),
            Arrays.toString(bestClusterIds),
            lastVersion);
      }
    }

    final int size = bestClusterIds.length;
    if (size == 0) return -1;

    if (size == 1)
      // ONLY ONE: RETURN IT
      return bestClusterIds[0];

    final int cluster = super.getClusterSelection().getCluster(this, bestClusterIds, doc);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(
          this,
          manager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Selected cluster %d for class '%s' from %s (threadId=%d dCfgVersion=%d)",
          cluster,
          getName(),
          Arrays.toString(bestClusterIds),
          Thread.currentThread().getId(),
          lastVersion);

    return cluster;
  }

  public ODistributedConfiguration readConfiguration(
      ODatabaseDocumentDistributed db, ODistributedServerManager manager) {
    if (isAbstract())
      throw new IllegalArgumentException("Cannot create a new instance of abstract class");

    int[] clusterIds = getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int c : clusterIds) clusterNames.add(db.getClusterNameById(c).toLowerCase(Locale.ENGLISH));

    ODistributedConfiguration cfg = manager.getDatabaseConfiguration(db.getName());

    List<String> bestClusters =
        cfg.getOwnedClustersByServer(clusterNames, manager.getLocalNodeName());
    if (bestClusters.isEmpty()) {
      // REBALANCE THE CLUSTERS
      final OModifiableDistributedConfiguration modifiableCfg = cfg.modify();
      manager.reassignClustersOwnership(
          manager.getLocalNodeName(), db.getName(), modifiableCfg, true);

      cfg = modifiableCfg;

      // RELOAD THE CLUSTER LIST TO GET THE NEW CLUSTER CREATED (IF ANY)
      db.activateOnCurrentThread();
      clusterNames.clear();
      clusterIds = getClusterIds();
      for (int c : clusterIds)
        clusterNames.add(db.getClusterNameById(c).toLowerCase(Locale.ENGLISH));

      bestClusters = cfg.getOwnedClustersByServer(clusterNames, manager.getLocalNodeName());

      if (bestClusters.isEmpty()) {
        // FILL THE MAP CLUSTER/SERVERS
        final StringBuilder buffer = new StringBuilder();
        for (String c : clusterNames) {
          if (buffer.length() > 0) buffer.append(" ");

          buffer.append(" ");
          buffer.append(c);
          buffer.append(":");
          buffer.append(cfg.getServers(c, null));
        }

        ODistributedServerLog.warn(
            this,
            manager.getLocalNodeName(),
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Cannot find best cluster for class '%s'. Configured servers for clusters %s are %s (dCfgVersion=%d)",
            getName(),
            clusterNames,
            buffer.toString(),
            cfg.getVersion());

        throw new ODatabaseException(
            "Cannot find best cluster for class '"
                + getName()
                + "' on server '"
                + manager.getLocalNodeName()
                + "' (clusterStrategy="
                + getName()
                + " dCfgVersion="
                + cfg.getVersion()
                + ")");
      }
    }

    db.activateOnCurrentThread();

    final int[] newBestClusters = new int[bestClusters.size()];
    int i = 0;
    for (String c : bestClusters) newBestClusters[i++] = db.getClusterIdByName(c);

    this.bestClusterIds = newBestClusters;
    lastVersion = cfg.getVersion();

    return cfg;
  }

  protected boolean isDistributedCommand(ODatabaseDocumentInternal database) {
    return !database.isLocalEnv();
  }
}
