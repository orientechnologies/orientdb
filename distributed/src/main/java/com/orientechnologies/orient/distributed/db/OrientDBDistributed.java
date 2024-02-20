package com.orientechnologies.orient.distributed.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentEmbeddedPooled;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedConfigurationManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.distributed.impl.ONewDeltaSyncImporter;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {

  private volatile OServer server;
  private volatile ODistributedPlugin plugin;
  protected final ConcurrentHashMap<String, ODistributedDatabaseImpl> databases =
      new ConcurrentHashMap<String, ODistributedDatabaseImpl>();
  protected final ConcurrentHashMap<String, ODistributedConfigurationManager> configurations =
      new ConcurrentHashMap<String, ODistributedConfigurationManager>();

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    // This now is simple but should be replaced by a factory depending to the protocol version
  }

  @Override
  public void init(OServer server) {
    // Cannot get the plugin from here, is too early, doing it lazy
    this.server = server;
  }

  public void loadAllDatabases() {
    List<String> dbs = new ArrayList<String>(this.listDatabases(null, null));
    Collections.sort(dbs);
    for (final String databaseName : dbs) {
      if (!OSystemDatabase.SYSTEM_DB_NAME.equals(databaseName)) {
        ODistributedServerManager dm = getDistributedManager();
        ODistributedServerLog.info(
            this,
            dm.getLocalNodeName(),
            null,
            DIRECTION.NONE,
            "Opening database '%s'...",
            databaseName);
        try {
          openNoAuthorization(databaseName).close();
        } catch (Exception e) {
          OLogManager.instance()
              .warn(this, " Exception on first inizialization of database '%s'", e, databaseName);
        }
      }
    }
  }

  public synchronized ODistributedPlugin getPlugin() {
    if (plugin == null) {
      if (server != null && server.isActive()) plugin = server.getPlugin("cluster");
    }
    return plugin;
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(
      OAbstractPaginatedStorage storage, OrientDBConfig config, OSharedContext sharedContext) {
    ODatabaseDocumentEmbedded embedded;
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.init(config, sharedContext);
    } else {
      embedded = new ODatabaseDocumentDistributed(storage, plugin, sharedContext);
      embedded.init(config, sharedContext);
      registerNewDatabaseIfNeeded(embedded, sharedContext);
    }
    return embedded;
  }

  @Override
  protected ODatabaseDocumentEmbedded newCreateSessionInstance(
      OAbstractPaginatedStorage storage, OrientDBConfig config, OSharedContext sharedContext) {
    ODatabaseDocumentEmbedded embedded;
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.internalCreate(config, getOrCreateSharedContext(storage));
    } else {
      embedded = new ODatabaseDocumentDistributed(storage, plugin, sharedContext);
      embedded.internalCreate(config, getOrCreateSharedContext(storage));
      registerNewDatabaseIfNeeded(embedded, sharedContext);
    }
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OAbstractPaginatedStorage storage, OSharedContext sharedContext) {
    ODatabaseDocumentEmbedded embedded;
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      embedded = new ODatabaseDocumentEmbeddedPooled(pool, storage);
      embedded.init(pool.getConfig(), getOrCreateSharedContext(storage));
    } else {
      embedded = new ODatabaseDocumentDistributedPooled(pool, storage, plugin, sharedContext);
      embedded.init(pool.getConfig(), getOrCreateSharedContext(storage));
      registerNewDatabaseIfNeeded(embedded, sharedContext);
    }
    return embedded;
  }

  public void setPlugin(ODistributedPlugin plugin) {
    this.plugin = plugin;
  }

  public OStorage fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    OAbstractPaginatedStorage storage = null;
    ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      try {
        storage = storages.get(dbName);

        if (storage != null) {
          // The underlying storage instance will be closed so no need to closed it
          ODatabaseDocumentEmbedded deleteInstance =
              newSessionInstance(storage, config, getOrCreateSharedContext(storage));
          dropStorageFiles((OLocalPaginatedStorage) storage);
          OSharedContext context = sharedContexts.remove(dbName);
          context.close();

          storage.delete();
          storages.remove(dbName);
          ODatabaseRecordThreadLocal.instance().remove();
        }
        storage =
            (OAbstractPaginatedStorage)
                disk.createStorage(
                    buildName(dbName),
                    maxWALSegmentSize,
                    doubleWriteLogMaxSegSize,
                    generateStorageId(),
                    this);
        embedded = internalCreate(config, storage);
        storages.put(dbName, storage);
      } catch (OModificationOperationProhibitedException e) {
        throw e;
      } catch (Exception e) {
        if (storage != null) {
          storage.delete();
        }

        throw OException.wrapException(
            new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
      }
    }
    try {
      storage.restoreFullIncrementalBackup(backupStream);
    } catch (RuntimeException e) {
      try {
        if (storage != null) {
          storage.delete();
        }
      } catch (Exception e1) {
        OLogManager.instance()
            .warn(this, "Error doing cleanups, should be safe do progress anyway", e1);
      }
      synchronized (this) {
        sharedContexts.remove(dbName);
        storages.remove(dbName);
      }

      OContextConfiguration configs = getConfigurations().getConfigurations();
      OLocalPaginatedStorage.deleteFilesFromDisc(
          dbName,
          configs.getValueAsInteger(FILE_DELETE_RETRY),
          configs.getValueAsInteger(FILE_DELETE_DELAY),
          buildName(dbName));
      throw e;
    }
    embedded.getSharedContext().reInit(storage, embedded);
    distributedSetOnline(dbName);
    ODatabaseRecordThreadLocal.instance().remove();
    return storage;
  }

  @Override
  public ODatabaseDocumentInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool) {
    ODatabaseDocumentInternal session = super.poolOpen(name, user, password, pool);
    return session;
  }

  @Override
  public void internalDrop(String name) {
    synchronized (this) {
      checkOpen();
      // This is a temporary fix for distributed drop that avoid scheduled view update to re-open
      // the distributed database while is dropped
      OSharedContext sharedContext = sharedContexts.get(name);
      if (sharedContext != null) {
        sharedContext.getViewManager().close();
      }
    }

    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      ODatabaseDocumentInternal db = openNoAuthenticate(name, null);
      for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDrop(db);
      }
      db.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }

    unregisterDatabase(name);
    plugin.removeDbFromClusterMetadata(name);

    synchronized (this) {
      if (exists(name, null, null)) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null) {
          sharedContext.close();
        }
        if (storage instanceof OLocalPaginatedStorage) {
          dropStorageFiles((OLocalPaginatedStorage) storage);
        }
        storage.delete();
        storages.remove(name);
        sharedContexts.remove(name);
      }
    }
  }

  @Override
  public void drop(String name, String user, String password) {
    if (getPlugin() != null && getPlugin().isEnabled()) {
      plugin.executeInDistributedDatabaseLock(
          name,
          20000,
          () -> {
            plugin.dropOnAllServers(name);
            return null;
          });
      plugin.dropConfig(name);
    } else {
      super.drop(name, user, password);
    }
  }

  private boolean checkDbAvailable(String name) {
    if (getPlugin() == null || !getPlugin().isEnabled()) {
      return true;
    }
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(name)) return true;
    DB_STATUS dbStatus = plugin.getDatabaseStatus(plugin.getLocalNodeName(), name);
    return dbStatus == DB_STATUS.ONLINE || dbStatus == DB_STATUS.BACKUP;
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password) {
    if (checkDbAvailable(name)) {
      return super.open(name, user, password);
    } else {
      if (exists(name, user, password)) {
        super.open(name, user, password);
      }
      throw new OOfflineNodeException(
          "database " + name + " not online on " + plugin.getLocalNodeName());
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {

    if (checkDbAvailable(name)) {
      return super.open(name, user, password, config);
    } else {
      if (exists(name, user, password)) {
        super.open(name, user, password, config);
      }
      throw new OOfflineNodeException(
          "database " + name + " not online on " + plugin.getLocalNodeName());
    }
  }

  @Override
  public void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    throw new UnsupportedOperationException("old implementation do not support new flow");
  }

  public static void dropStorageFiles(OLocalPaginatedStorage storage) {
    // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
    final File dCfg =
        new File(
            storage.getStoragePath() + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);

    try {
      if (dCfg.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg.delete()) break;
          Thread.sleep(100);
        }
      }

      final File dCfg2 =
          new File(
              storage.getStoragePath()
                  + "/"
                  + ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME);
      if (dCfg2.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg2.delete()) break;
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  @Override
  public ODistributedServerManager getDistributedManager() {
    return this.plugin;
  }

  @Override
  public void deltaSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    new ONewDeltaSyncImporter()
        .importDelta(server, dbName, backupStream, plugin.getLocalNodeName());
    getDatabase(dbName).setOnline();
  }

  private void offlineOnShutdown() {
    // SET ALL DATABASES TO NOT_AVAILABLE
    for (Entry<String, ODistributedDatabaseImpl> m : databases.entrySet()) {
      if (OSystemDatabase.SYSTEM_DB_NAME.equals(m.getKey())) continue;

      try {
        plugin.setDatabaseStatus(plugin.getLocalNodeName(), m.getKey(), DB_STATUS.NOT_AVAILABLE);
      } catch (Exception t) {
        // IGNORE IT
      }
      m.getValue().shutdown();
    }
    databases.clear();
  }

  public ODistributedDatabaseImpl getDatabase(final String iDatabaseName) {
    return databases.get(iDatabaseName);
  }

  private ODistributedDatabaseImpl newDistributedDatabase(
      String key, ODatabaseDocumentInternal session) {
    ODistributedDatabaseImpl db = new ODistributedDatabaseImpl(this, plugin, key);
    db.initFirstOpen(session);
    return db;
  }

  public ODistributedDatabaseImpl unregisterDatabase(final String iDatabaseName) {
    try {
      plugin.setDatabaseStatus(plugin.getLocalNodeName(), iDatabaseName, DB_STATUS.OFFLINE);
    } catch (Exception t) {
      ODistributedServerLog.warn(
          this, plugin.getLocalNodeName(), null, null, "error un-registering database", t);
      // IGNORE IT
    }

    final ODistributedDatabaseImpl db = databases.remove(iDatabaseName);
    if (db != null) {
      db.onDropShutdown();
    }
    return db;
  }

  public void registerNewDatabaseIfNeeded(
      ODatabaseDocumentInternal session, OSharedContext context) {
    databases.computeIfAbsent(session.getName(), (n) -> this.newDistributedDatabase(n, session));
  }

  public void distributedSetOnline(String database) {
    ODistributedDatabaseImpl distribDatabase = getDatabase(database);
    if (distribDatabase != null) {
      distribDatabase.setOnline();
    }
  }

  public void distributedPauseDatabase(String database) {
    ODistributedDatabaseImpl distribDatabase = getDatabase(database);
    if (distribDatabase != null) {
      distribDatabase.suspend();
    }
  }

  public Set<String> getActiveDatabases() {
    // We assign the ConcurrentHashMap (databases) to the Map interface for this reason:
    // ConcurrentHashMap.keySet() in Java 8 returns a ConcurrentHashMap.KeySetView.
    // ConcurrentHashMap.keySet() in Java 7 returns a Set.
    // If this code is compiled with Java 8 yet is run on Java 7, you'll receive a
    // NoSuchMethodError:
    // java.util.concurrent.ConcurrentHashMap.keySet()Ljava/util/concurrent/ConcurrentHashMap$KeySetView.
    // By assigning the ConcurrentHashMap variable to a Map, the call to keySet() will return a Set
    // and not the Java 8 type, KeySetView.
    Map<String, ODistributedDatabaseImpl> map = databases;

    final Set<String> result = new HashSet<String>(map.keySet());
    result.remove(OSystemDatabase.SYSTEM_DB_NAME);
    return result;
  }

  public Collection<ODistributedDatabaseImpl> getDistributedDatabases() {
    return databases.values();
  }

  public ODistributedConfiguration getOrInitDistributedConfiguration(ODatabaseSession session) {
    return getOrInitConfigurationManager(session.getName()).getDistributedConfiguration(session);
  }

  public ODistributedConfigurationManager getOrInitConfigurationManager(String database) {
    return configurations.computeIfAbsent(
        database,
        (key) -> {
          return new ODistributedConfigurationManager(this, plugin, key);
        });
  }

  public ODistributedConfigurationManager getConfigurationManager(String database) {
    return configurations.get(database);
  }

  public ODistributedConfiguration getDistributedConfiguration(ODatabaseSession session) {
    ODistributedConfigurationManager cm = getConfigurationManager(session.getName());
    if (cm != null) {
      return cm.getDistributedConfiguration(session);
    } else {
      return null;
    }
  }

  private interface ConfigOp<T> {
    T op(ODistributedConfigurationManager cm, ODatabaseSession session);
  }

  public <T> T configOp(ODistributedConfigurationManager cm, String database, ConfigOp<T> op) {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null && !db.isClosed() && db.isDistributed() && db.getName().equals(database)) {
      return op.op(cm, db);
    } else if (exists(database, null, null)) {
      try (ODatabaseSession session = openNoAuthorization(database)) {
        return op.op(cm, session);
      } finally {
        if (db != null && !db.isClosed()) {
          ODatabaseRecordThreadLocal.instance().set(db);
        }
      }
    } else {
      return op.op(cm, null);
    }
  }

  public ODistributedConfiguration getExistingDistributedConfiguration(String database) {
    ODistributedConfigurationManager cm = getConfigurationManager(database);
    if (cm != null) {
      return cm.getExistingDistributedConfiguration();
    } else {
      return null;
    }
  }

  public ODistributedConfiguration getDistributedConfiguration(String database) {
    ODistributedConfigurationManager cm = getConfigurationManager(database);
    if (cm != null) {
      if (cm.getExistingDistributedConfiguration() != null) {
        return cm.getExistingDistributedConfiguration();
      } else {
        return configOp(cm, database, (m, s) -> m.getDistributedConfiguration(s));
      }
    } else {
      return null;
    }
  }

  public void setDistributedConfiguration(
      String database, final OModifiableDistributedConfiguration distributedConfiguration) {
    ODistributedConfigurationManager cm = getOrInitConfigurationManager(database);
    configOp(
        cm,
        database,
        (m, s) -> {
          m.setDistributedConfiguration(s, distributedConfiguration);
          return (Void) null;
        });
  }

  public void saveDatabaseConfiguration(String database) {
    ODistributedConfigurationManager cm = getOrInitConfigurationManager(database);
    configOp(
        cm,
        database,
        (m, s) -> {
          m.saveDatabaseConfiguration(s);
          return (Void) null;
        });
  }

  public ODistributedConfiguration getOrInitDistributedConfiguration(String database) {
    ODistributedConfigurationManager cm = getOrInitConfigurationManager(database);
    if (cm.getExistingDistributedConfiguration() != null) {
      return cm.getExistingDistributedConfiguration();
    } else {
      return cm.getDistributedConfiguration(null);
    }
  }

  public boolean tryUpdatingDatabaseConfigurationLocally(
      final String database, final OModifiableDistributedConfiguration cfg) {
    ODistributedConfigurationManager cm = getConfigurationManager(database);
    if (cm != null) {
      return configOp(cm, database, (m, s) -> m.tryUpdatingDatabaseConfigurationLocally(s, cfg));
    } else {
      return false;
    }
  }

  @Override
  public void close() {
    if (!isOpen()) return;
    offlineOnShutdown();
    super.close();
  }
}
