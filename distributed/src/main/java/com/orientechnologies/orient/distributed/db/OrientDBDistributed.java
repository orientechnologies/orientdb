package com.orientechnologies.orient.distributed.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentEmbeddedPooled;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseTask;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedConfigurationManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedMessageServiceImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.distributed.impl.ONewDeltaSyncImporter;
import com.orientechnologies.orient.server.distributed.impl.ORemoteServerManager;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OrientDBDistributed.class);
  private volatile OServer server;
  private volatile ODistributedPlugin plugin;
  private final CountDownLatch pluginStartupLatch = new CountDownLatch(1);
  protected final ConcurrentHashMap<String, ODistributedConfigurationManager> configurations =
      new ConcurrentHashMap<String, ODistributedConfigurationManager>();

  private final ODistributedMessageServiceImpl messageService;
  // TODO: this require the node name to be instantiate.
  private ONodeState nodeState = null;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    messageService = new ODistributedMessageServiceImpl(this);
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
        logger.infoNode(
            dm != null ? dm.getLocalNodeName() : "", "Opening database '%s'...", databaseName);
        try {
          openNoAuthorization(databaseName).close();
        } catch (Exception e) {
          logger.warn(" Exception on first inizialization of database '%s'", e, databaseName);
        }
      }
    }
  }

  private void waitForPluginStartup() {
    if (isDistributedPluginEnabled() && plugin == null) {
      try {
        logger.info("Waiting for plugin startup");
        if (!pluginStartupLatch.await(10, TimeUnit.SECONDS)) {
          throw new OOfflineNodeException(
              "Distributed manager is offline on " + server.getServerId());
        }
        logger.info("Plugin startup complete");
      } catch (InterruptedException ignored) {
      }
    }
  }

  public void setPlugin(ODistributedPlugin plugin) {
    logger.info("Setting plugin");
    this.plugin = plugin;
    this.pluginStartupLatch.countDown();
  }

  public ODistributedPlugin getPlugin() {
    logger.info("Getting plugin, server?=%s, server.active?=%s", server != null, server.isActive());
    waitForPluginStartup();
    return plugin;
  }

  protected OSharedContext createSharedContext(OStorage storage) {
    if (isDistributedDisabled(storage.getName())) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  private boolean isDistributedPluginEnabled() {
    return server.isDistributedPluginEnabled();
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OStorage storage, OrientDBConfig config) {
    ODatabaseDocumentEmbedded embedded;
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.init(config, getOrCreateSharedContext(storage));
    } else {
      OSharedContext sharedContext = getOrCreateSharedContext(storage);
      embedded = new ODatabaseDocumentDistributed(storage, plugin);
      embedded.init(config, sharedContext);
      //      getOrInitDistributedConfiguration(storage.getName());
    }
    return embedded;
  }

  protected boolean isDistributedDisabled(String storage) {
    return OSystemDatabase.SYSTEM_DB_NAME.equals(storage) || isDistributedPluginEnabled();
  }

  @Override
  protected ODatabaseDocumentEmbedded newCreateSessionInstance(
      OStorage storage, OrientDBConfig config) {
    ODatabaseDocumentEmbedded embedded;

    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbedded(storage);
      OSharedContext sharedContext = getOrCreateSharedContext(storage);
      embedded.internalCreate(config, sharedContext);
    } else {
      embedded = new ODatabaseDocumentDistributed(storage, plugin);
      OSharedContext sharedContext = getOrCreateSharedContext(storage);
      embedded.internalCreate(config, sharedContext);
      //      getOrInitDistributedConfiguration(storage.getName());
    }
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OStorage storage, OSharedContext sharedContext) {
    ODatabaseDocumentEmbedded embedded;
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbeddedPooled(pool, storage);
      embedded.init(pool.getConfig(), getOrCreateSharedContext(storage));
    } else {
      embedded = new ODatabaseDocumentDistributedPooled(pool, storage, plugin);
      embedded.init(pool.getConfig(), getOrCreateSharedContext(storage));
      //      getOrInitDistributedConfiguration(storage.getName());
    }
    return embedded;
  }

  public void fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    OStorage storage = null;
    ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      if (!isOpen()) {
        return;
      }
      try {
        storage = storages.get(dbName);

        if (storage != null) {
          // The underlying storage instance will be closed so no need to closed it
          ODatabaseDocumentEmbedded deleteInstance = newSessionInstance(storage, config);
          OSharedContext context = sharedContexts.remove(dbName);
          dbCount.decrementAndGet();
          context.close();
          dropStorageFiles((OLocalPaginatedStorage) storage);

          storage.delete();
          storages.remove(dbName);
          ODatabaseRecordThreadLocal.instance().remove();
        }
        storage =
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
        logger.warn("Error doing cleanups, should be safe do progress anyway", e1);
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
    return;
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
        OStorage storage = getOrInitStorage(name);
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
      //      dropFlow(name);
      plugin.dropConfig(name);
    } else {
      super.drop(name, user, password);
    }
  }

  private void dropFlow(String name) {
    var start = getNodeState().start(new OStandardCompleteAction(this));
    ODropDbMessage op = new ODropDbMessage(name);
    OProposeOp propose = new OProposeOp(start.promise(), op);
    sendMessage(start.nodes(), propose);
    try {
      Optional<OAcceptResult> result = start.result().get();
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void sendMessage(Set<ONodeId> set, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = getPlugin().getRemoteServerManager();
    for (ONodeId node : set) {
      if (node.equals(getNodeState().getNodeId())) {
        this.receiveMessage(op);
      } else {
        ORemoteServerController rem = remote.getRemoteServer(node.getNode());
        rem.sendMessage(message);
      }
    }
  }

  public void receiveMessage(OStructuralMessage op) {
    this.execute(() -> op.execute(this));
  }

  private boolean checkDbAvailable(String name) {
    if (getPlugin() == null || !getPlugin().isEnabled()) {
      return true;
    }
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(name)) return true;
    DB_STATUS dbStatus = plugin.getDatabaseStatus(getNodeName(), name);
    return dbStatus == DB_STATUS.ONLINE || dbStatus == DB_STATUS.BACKUP;
  }

  private boolean checkDbAvailableOpen(String name) {
    if (!checkDbAvailable(name)) {
      long waitTime =
          getConfigurations()
              .getConfigurations()
              .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);
      if (waitTime != 0) {
        long retry = waitTime / 500;
        // TODO: when there will be proper node online event this should attach to that with a
        // notification instead of sleep
        for (long i = 0; i < retry; i++) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          if (checkDbAvailable(name)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password) {
    if (checkDbAvailableOpen(name)) {
      return super.open(name, user, password);
    } else {
      if (exists(name, user, password)) {
        return super.open(name, user, password);
      }
      throw new OOfflineNodeException("database " + name + " not online on " + getNodeName());
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {

    if (checkDbAvailableOpen(name)) {
      return super.open(name, user, password, config);
    } else {
      if (exists(name, user, password)) {
        return super.open(name, user, password, config);
      }
      throw new OOfflineNodeException("database " + name + " not online on " + getNodeName());
    }
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
  public boolean deltaSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    if (new ONewDeltaSyncImporter().importDelta(this, dbName, backupStream, getNodeName())) {
      getDatabase(dbName).setOnline();
      return true;
    } else {
      return false;
    }
  }

  public String getNodeName() {
    return plugin.getLocalNodeName();
  }

  private void offlineOnShutdown() {
    // SET ALL DATABASES TO NOT_AVAILABLE
    for (String dbName : listLodadedDatabases()) {

      try {
        plugin.setDatabaseStatus(getNodeName(), dbName, DB_STATUS.NOT_AVAILABLE);
      } catch (Exception t) {
        // IGNORE IT
      }
    }
  }

  public ODistributedDatabaseImpl getDatabase(final String iDatabaseName) {
    OSharedContextDistributed ctx = (OSharedContextDistributed) sharedContexts.get(iDatabaseName);
    if (ctx != null) {
      return ctx.getDistributedContext();
    } else {
      return null;
    }
  }

  public ODistributedDatabaseImpl unregisterDatabase(final String iDatabaseName) {
    try {
      plugin.setDatabaseStatus(getNodeName(), iDatabaseName, DB_STATUS.OFFLINE);
    } catch (Exception t) {
      logger.warnNode(getNodeName(), "error un-registering database", t);
      // IGNORE IT
    }

    final ODistributedDatabaseImpl db = getDatabase(iDatabaseName);
    if (db != null) {
      db.onDropShutdown();
    }
    return db;
  }

  @Override
  public void create(
      String name,
      String user,
      String password,
      ODatabaseType type,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    super.create(name, user, password, type, config, createOps);
    if (!isDistributedDisabled(name)) {
      Set<String> nodes = plugin.getActiveServers();
      for (String node : nodes) {
        try {
          plugin.waitUntilNodeOnline(node, name);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
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
    return listLodadedDatabases();
  }

  public Collection<ODistributedDatabaseImpl> getDistributedDatabases() {
    return this.sharedContexts.values().stream()
        .map((x) -> ((OSharedContextDistributed) x).getDistributedContext())
        .toList();
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

  public ODistributedConfiguration getDefaultDistributedConfiguration(String database) {
    ODistributedConfigurationManager cm = getOrInitConfigurationManager(database);
    return cm.getDefaultConfiguration();
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
    this.messageService.shutdown();
    super.close();
  }

  public int getActiveDatabaseCount() {
    return this.dbCount.get();
  }

  public ODistributedMessageService getMessageService() {
    return messageService;
  }

  public synchronized ONodeState getNodeState() {
    if (nodeState == null) {
      nodeState = new ONodeState(new ONodeId(getPlugin().getLocalNodeName()));
    }
    return this.nodeState;
  }

  @Override
  public ONetworkMessage newNetworkMessage() {
    return new ONetworkMessageStructural(this);
  }
}
