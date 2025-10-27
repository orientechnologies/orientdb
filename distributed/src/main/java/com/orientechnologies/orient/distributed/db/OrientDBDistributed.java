package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
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
import com.orientechnologies.orient.core.storage.OStorageEngine.OBackupType;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.OSyncInfo;
import com.orientechnologies.orient.distributed.context.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSync;
import com.orientechnologies.orient.distributed.context.coordination.message.ONextBuffer;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeFirstConnect;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStartSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncData;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncRequest;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OrientDBDistributed.class);
  private volatile OServer server;
  private volatile ODistributedPlugin plugin;
  private final ConcurrentHashMap<String, ODistributedConfigurationManager> configurations =
      new ConcurrentHashMap<String, ODistributedConfigurationManager>();

  private final ODistributedMessageServiceImpl messageService;
  // TODO: this require the node name to be instantiate.
  private ONodeState nodeState = null;
  private final ConcurrentHashMap<UUID, OSyncState> activeSync =
      new ConcurrentHashMap<UUID, OSyncState>();

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

  public ODistributedPlugin getPlugin() {
    if (plugin == null) {
      synchronized (this) {
        if (plugin == null) {
          if (server != null && server.isActive()) {
            plugin = server.getPlugin("cluster");
          }
        }
      }
    }
    return plugin;
  }

  protected OSharedContext createSharedContext(OStorage storage) {
    if (isDistributedDisabled(storage.getName())) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
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
    }
    return embedded;
  }

  protected boolean isDistributedDisabled(String storage) {
    return OSystemDatabase.SYSTEM_DB_NAME.equals(storage) || plugin == null || !plugin.isEnabled();
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
      // getOrInitDistributedConfiguration(storage.getName());
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
      // getOrInitDistributedConfiguration(storage.getName());
    }
    return embedded;
  }

  public void setPlugin(ODistributedPlugin plugin) {
    this.plugin = plugin;
  }

  public void fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    OStorage storage = null;
    ODatabaseDocumentEmbedded embedded;

    if (!isOpen()) {
      return;
    }
    try {
      synchronized (this) {
        storage = storages.get(dbName);

        if (storage != null) {
          // The underlying storage instance will be closed so no need to closed it
          ODatabaseDocumentEmbedded deleteInstance = newSessionInstance(storage, config);
          OSharedContext context = sharedContexts.remove(dbName);
          dbCount.decrementAndGet();
          context.close();
          dropStorageFiles(storage);

          storage.delete();
          storages.remove(dbName);
          ODatabaseRecordThreadLocal.instance().remove();
        }
      }
      storage =
          getDefaultEngine()
              .restoreStream(
                  this,
                  dbName,
                  config.getConfigurations(),
                  backupStream,
                  OBackupType.FULL_INCREMENTAL);
      embedded = newSessionInstance(storage, config);
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
        OStorage storage = getAndOpenStorage(name, getConfigurations());
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null) {
          sharedContext.close();
        }
        dropStorageFiles(storage);
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
      // dropFlow(name);
      plugin.dropConfig(name);
    } else {
      super.drop(name, user, password);
    }
  }

  private void dropFlow(String name) {
    distributedOperation(new ODropDbMessage(name));
  }

  public void sendMessage(Set<ONodeId> set, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = getPlugin().getRemoteServerManager();
    for (ONodeId node : set) {
      if (node.equals(getNodeState().getNodeId())) {
        this.receiveMessage(op);
      } else {
        ORemoteServerController rem = remote.getRemoteServer(node.getNode());
        if (rem != null) {
          rem.sendMessage(message);
        }
      }
    }
  }

  public void sendMessage(ONodeId node, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = getPlugin().getRemoteServerManager();
    if (node.equals(getNodeState().getNodeId())) {
      this.receiveMessage(op);
    } else {
      ORemoteServerController rem = remote.getRemoteServer(node.getNode());
      if (rem != null) {
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

  public static void dropStorageFiles(OStorage storage) {
    Optional<Path> path = storage.getPath();
    if (path.isPresent()) {
      Path p = path.get();
      // REMOVE distributed-config.json and distributed-sync.json files to allow removal of
      // directory
      final File dCfg = new File(p + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);

      try {
        if (dCfg.exists()) {
          for (int i = 0; i < 10; ++i) {
            if (dCfg.delete()) break;
            Thread.sleep(100);
          }
        }

        final File dCfg2 =
            new File(p + "/" + ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME);
        if (dCfg2.exists()) {
          for (int i = 0; i < 10; ++i) {
            if (dCfg2.delete()) break;
            Thread.sleep(100);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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

  private void createDatabaseFlow(
      String name,
      String user,
      String password,
      ODatabaseType type,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    ODatabaseId dbId = new ODatabaseId(name);
    declareDatabaseFlow(name, dbId);
    super.create(name, user, password, type, config, createOps);
    setDatabaseStatus(name, dbId, getNodeState().getNodeId(), ODatabaseState.Online);
    try {
      getNodeState().getDatabaseTopology().waitOnlineQuorum(dbId, Optional.empty());
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("wait for online interrupted"), e);
    }
  }

  private void declareDatabaseFlow(String name, ODatabaseId dbId) {
    Set<ONodeId> currentMembers = nodeState.getNetworkState().getMembers();
    distributedOperation(new ODeclareDbMessage(name, dbId, currentMembers, 0));
  }

  private void setDatabaseStatus(
      String name, ODatabaseId dbId, ONodeId node, ODatabaseState state) {
    long version = this.getNodeState().getDatabaseTopology().getDatabaseVersion(dbId);
    distributedOperation(new OSetDatabaseState(dbId, node, state, version + 1));
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

  public Optional<OAcceptResult> distributedOperation(OOperationMessage operation) {
    var start = getNodeState().start(new OStandardCompleteAction(this));
    OProposeOp propose = new OProposeOp(start.promise(), operation);
    sendMessage(start.nodes(), propose);
    try {
      return start.result().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(
          new OInterruptedException("Interrupted distributed future"), e);
    } catch (ExecutionException e) {
      throw OException.wrapException(
          new OInterruptedException("Execution exception distributed future"), e);
    }
  }

  public synchronized ONodeState getNodeState() {
    if (nodeState == null) {
      // TODO: provide minimum quorum;
      ONodeId nodeId = new ONodeId(getPlugin().getLocalNodeName());
      OSystemStateStore store = new OSystemStateStore(getSystemDatabase());
      nodeState = new ONodeState(nodeId, 0, store);
      nodeState.initFromStore();
    }
    return this.nodeState;
  }

  @Override
  public ONetworkMessage newNetworkMessage() {
    return new ONetworkMessageStructural(this);
  }

  public void firstConnect(ONodeId nodeId, ONodeStateNetwork state) {
    ONodeState localState = getNodeState();
    ODiscoverAction action = localState.nodeJoinStart(nodeId, state);
    action.execute(this);
  }

  public void connected(ONodeId node) {
    sendFirstConnect(node);
  }

  private void sendFirstConnect(ONodeId nodeId) {
    ONodeStateNetwork st = getNodeState().getNetworkState();
    this.sendMessage(
        Collections.singleton(nodeId), new ONodeFirstConnect(getNodeState().getNodeId(), st));
  }

  public void registerNode(ONodeId node, long version) {
    getNodeState().register(node, version);
    // This should make aware of the added node of the fact it joined the network
    sendFirstConnect(node);
  }

  public void cancelRegisterPromise() {
    getNodeState().cancelRegisterPromise();
  }

  public Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    return getNodeState()
        .promiseDeclare(promise, databaseId, database, partecipants, minimumQuorum);
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    getNodeState().declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
    getNodeState()
        .getDatabaseTopology()
        .executeOnOneOnline(
            dbId,
            () -> {
              execute(() -> sync(dbId));
            });
  }

  private void sync(ODatabaseId dbId) {
    OSyncInfo sync = getNodeState().getDatabaseTopology().newSync(dbId);
    sendMessage(
        sync.targets(),
        new OSyncRequest(
            getNodeState().getNodeId(), dbId, sync.syncId(), OSyncMode.IncrementalBackup));
  }

  public void cancelDeclare(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    getNodeState().cancelDatabase(promise, dbId, database);
  }

  public void acceptSync(ONodeId from, ODatabaseId dbId, UUID syncId, OSyncMode mode) {
    // TODO: check if already syncinging with someone do not accept
    if (this.existsDb(dbId)) {
      sendMessage(from, new OCanSync(getNodeState().getNodeId(), dbId, syncId, mode, true));
    } else {
      sendMessage(from, new OCanSync(getNodeState().getNodeId(), dbId, syncId, mode, false));
    }
  }

  private boolean existsDb(ODatabaseId dbId) {
    // TODO Auto-generated method stub
    return false;
  }

  public void canSync(
      ONodeId from, ODatabaseId dbId, UUID syncId, boolean canSync, OSyncMode mode) {
    Optional<OSyncState> state =
        getNodeState()
            .getDatabaseTopology()
            .canSync(from, getNodeState().getNodeId(), dbId, syncId, canSync, mode);

    if (state.isPresent()) {

      OSyncState st = state.get();
      this.activeSync.put(st.getSyncId(), st);
      sendMessage(from, new OStartSync(getNodeState().getNodeId(), dbId, syncId, mode));
      String dbName = getDbName(dbId);
      OReceiverImputStream input = new OReceiverImputStream(this, st);
      st.setReceiver(input);
      Thread thread =
          new Thread(
              () -> {
                fullSync(dbName, input, getConfigurations());
              });
      thread.start();
    }
  }

  public void sendDatabase(ONodeId to, ODatabaseId dbId, UUID syncId, OSyncMode mode) {
    OSyncState state =
        getNodeState()
            .getDatabaseTopology()
            .startSend(to, getNodeState().getNodeId(), dbId, syncId, mode);
    this.activeSync.put(state.getSyncId(), state);
    OutputStream out = new BufferedOutputStream(new OutputStreamMessages(this, state), 8096);
    Thread thread =
        new Thread(
            () -> {
              syncBackup(state, out);
            });
    thread.start();
  }

  public void syncBackup(OSyncState state, OutputStream out) {
    String dbNam = getDbName(state.getDbId());

    ODatabaseDocumentEmbedded db = openNoAuthorization(dbNam);
    OStorage storage = db.getStorage();
    if (storage.supportIncremental() && state.isIncremental()) {
      storage.incrementalSync(out, null);
    } else {
      int compression =
          getConfigurations()
              .getConfigurations()
              .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION);
      try {
        storage.backup(out, null, null, null, compression, 0);
      } catch (IOException e) {
        try {
          out.close();
        } catch (IOException e1) {
        }
      }
    }
  }

  private String getDbName(ODatabaseId dbId) {
    return null;
  }

  public void sendBuffer(OSyncState state, byte[] data) {
    sendMessage(state.getTo(), new OSyncData(state.getSyncId(), data));
    state.transaferd(data.length);
    try {
      state.waitForNext();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void finishSync(OSyncState state) {
    // TODO Auto-generated method stub

  }

  public void receiveSyncData(UUID syncId, byte[] data) {
    var state = this.activeSync.get(syncId);
    state.receiveData(data);
  }

  public void requestNext(OSyncState state) {
    sendMessage(state.getFrom(), new ONextBuffer(state.getSyncId()));
  }

  public void nextBuffer(UUID syncId) {
    var state = this.activeSync.get(syncId);
    state.requestNext();
  }
}
