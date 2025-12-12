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
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEngine.OBackupType;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.ONodeRole;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.ORetryInfo;
import com.orientechnologies.orient.distributed.context.ORetryOperation;
import com.orientechnologies.orient.distributed.context.OStandardCompleteAction;
import com.orientechnologies.orient.distributed.context.OSyncId;
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
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddDatabaseMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.ONoTransactionSequencialAvailable;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedConfigurationManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedMessageServiceImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedOutput;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded
    implements OServerAware, ODatabaseStateChangeListener {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OrientDBDistributed.class);
  private volatile OServer server;
  private volatile ODistributedPlugin plugin;
  private final ConcurrentHashMap<String, ODistributedConfigurationManager> configurations =
      new ConcurrentHashMap<String, ODistributedConfigurationManager>();

  private final ODistributedMessageServiceImpl messageService;
  // TODO: this require the node name to be instantiate.
  private ONodeState nodeState = null;
  private String nodeName;
  private ORemoteServerManager remoteServerManager;
  // LOCAL MSG COUNTER FOR LEGACY IMPLEMENTATIONS WILL BE REMOVED IN FUTURE
  protected AtomicLong localMessageIdCounter = new AtomicLong();

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    messageService = new ODistributedMessageServiceImpl(this);
    Optional<ONodeConfiguration> nodeConfig = config.getNodeConfiguration();
    if (nodeConfig.isPresent()) {
      initDistributed(nodeConfig.get());
    }
  }

  @Override
  public void init(OServer server) {
    // Cannot get the plugin from here, is too early, doing it lazy
    this.server = server;
  }

  public void initDistributed(ONodeConfiguration config) {
    initDistributed(
        config.getNodeName(),
        config.getGroupName(),
        config.getQuorum(),
        new ORemoteServerAvailabilityCheck() {

          @Override
          public void nodeDisconnected(String node) {}

          @Override
          public boolean isNodeAvailable(String node) {
            return false;
          }
        });
  }

  public void initDistributed(
      String nodeName, String groupIdPar, int miminumQuorum, ORemoteServerAvailabilityCheck check) {
    this.nodeName = nodeName;
    // TODO: resolve groupId and minimum quorum;
    ONodeId nodeId = new ONodeId(nodeName);
    OGroupId groupId = new OGroupId(groupIdPar);
    OSystemStateStore store = new OSystemStateStore(getSystemDatabase());
    this.nodeState = new ONodeState(nodeId, groupId, miminumQuorum, store, this);
    this.remoteServerManager = new ORemoteServerManager(nodeName, check);
    ODiscoverAction action = this.nodeState.initFromStore();
    action.execute(
        this,
        newExectution(
            (ctx, complete) -> {
              // no Retry;
            }),
        null);
  }

  private OStandardCompleteExecution newExectution(ORetryOperation operation) {
    ORetryInfo retryInfo = newRetryInfo();
    return new OStandardCompleteExecution(this, operation, retryInfo);
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {
    if (ODatabaseState.Online.equals(state) && !getNodeId().equals(nodeId)) {
      execute(() -> syncIfNeeded(dbId));
    }
    dumpNodeInfo();
  }

  private void dumpNodeInfo() {
    logger.info("current status:\n%s", ODistributedOutput.formatServerStatus(this));
  }

  private void syncIfNeeded(ODatabaseId dbId) {
    if (!ODatabaseState.Online.equals(
        getNodeState().getDatabaseTopology().getNodeState(dbId, getNodeId()))) {
      sync(dbId, Optional.empty());
    }
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
    return OSystemDatabase.SYSTEM_DB_NAME.equals(storage)
        || plugin == null
        || !plugin.isEnabled()
        || nodeState == null;
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

  public void incrementalsSync(String dbName, InputStream backupStream, OrientDBConfig config) {
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
          dropStorageFiles(storage);

          storage.delete();
          storages.remove(dbName);
          ODatabaseRecordThreadLocal.instance().remove();
        }
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
      storage =
          getDefaultEngine()
              .restoreStream(
                  this,
                  dbName,
                  config.getConfigurations(),
                  backupStream,
                  OBackupType.FULL_INCREMENTAL);
      synchronized (this) {
        embedded = newSessionInstance(storage, config);
        storages.put(dbName, storage);
      }
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
    ODatabaseRecordThreadLocal.instance().remove();
    return;
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

    //    if (isDistributedDisabled(password)) {
    //      dropFlow(name);
    //    }
  }

  private void dropFlow(String name) {
    Future<Optional<OAcceptResult>> droped =
        retryOperation(
            (ctx, complete) -> {
              coordinatedOperation(new ODropDbMessage(name), complete);
            });
    try {
      droped.get();
    } catch (InterruptedException | ExecutionException e) {
    }
  }

  public void sendMessage(Set<ONodeId> set, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = remoteServerManager;
    for (ONodeId node : set) {
      if (node.equals(getNodeState().getNodeId())) {
        this.receiveMessage(op);
      } else {
        ORemoteServerController rem = remote.getRemoteServer(node.getNode());
        if (rem != null) {
          rem.sendMessage(message);
        } else {
          logger.warn("Node %s offline could not send message", node);
        }
      }
    }
  }

  public void sendMessage(ONodeId node, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = remoteServerManager;
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
    if (ONewDeltaSyncImporter.importDelta(this, dbName, backupStream, getNodeName())) {
      getDatabase(dbName).setOnline();
      return true;
    } else {
      return false;
    }
  }

  public String getNodeName() {
    return this.nodeName;
  }

  private void offlineOnShutdown() {
    // SET ALL DATABASES TO NOT_AVAILABLE
    for (String dbName : listLodadedDatabases()) {

      try {
        setDatabaseStatus(dbName, DB_STATUS.NOT_AVAILABLE);
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
      setDatabaseStatus(iDatabaseName, DB_STATUS.OFFLINE);
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
      ODatabaseId id,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    super.create(name, user, password, type, id, config, createOps);
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

    //    if (isDistributedDisabled(name)) {
    //      super.create(name, user, password, type, id, config, createOps);
    //    } else {
    //      createDatabaseFlow(name, user, password, type, id, config, createOps);
    //    }
  }

  private void createDatabaseFlow(
      String name,
      String user,
      String password,
      ODatabaseType type,
      ODatabaseId dbId,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    declareDatabaseFlow(name, dbId);
    super.create(name, user, password, type, dbId, config, createOps);
    setDatabaseStatus(dbId, getNodeState().getNodeId(), ODatabaseState.Online);
    try {
      getNodeState().getDatabaseTopology().waitOnlineQuorum(dbId, Optional.empty());
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("wait for online interrupted"), e);
    }
  }

  private void declareDatabaseFlow(String name, ODatabaseId dbId) {
    retryOperation(
        (ctx, cmplete) -> {
          Set<ONodeId> currentMembers = getNodeState().getNetworkMemebers();
          coordinatedOperation(new ODeclareDbMessage(name, dbId, currentMembers, 0), cmplete);
        });
  }

  public Future<Optional<OAcceptResult>> retryOperation(ORetryOperation operation) {
    OStandardCompleteExecution exec = newExectution(operation);
    execute(
        () -> {
          operation.execute(this, exec);
        });
    return exec.getResult();
  }

  public void retryExecution(ORetryOperation operation, OCompleteExecution exec, int delay) {
    delayExecute(
        () -> {
          operation.execute(this, exec);
        },
        delay);
  }

  private void setDatabaseStatus(ODatabaseId dbId, ONodeId node, ODatabaseState state) {
    retryOperation(
        (ctx, complete) -> {
          long version = this.getNodeState().getDatabaseTopology().getDatabaseVersion(dbId);
          coordinatedOperation(new OSetDatabaseState(dbId, node, state, version + 1), complete);
        });
  }

  private ODatabaseState getDatabaseStatus(ODatabaseId dbId, ONodeId node) {
    return this.getNodeState().getDatabaseTopology().getNodeState(dbId, node);
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
    if (this.remoteServerManager != null) {
      this.remoteServerManager.closeAll();
    }
    super.close();
  }

  public int getActiveDatabaseCount() {
    return this.dbCount.get();
  }

  public ODistributedMessageService getMessageService() {
    return messageService;
  }

  public void distributedOperation(OOperationMessage operation, OCompleteExecution retry) {
    OStandardCompleteAction action = newCompleteAction(operation, retry);
    sendOperation(operation, action);
  }

  public OStandardCompleteAction newCompleteAction(
      OOperationMessage operation, OCompleteExecution execution) {
    return new OStandardCompleteAction(this, operation, execution);
  }

  public ORetryInfo newRetryInfo() {
    int retryCountDown =
        getConfigurations()
            .getConfigurations()
            .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int delay =
        getConfigurations()
            .getConfigurations()
            .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
    return new ORetryInfo(retryCountDown, delay);
  }

  private void sendOperation(OOperationMessage operation, OCompleteAction action) {
    var startOp = getNodeState().start(action);
    if (startOp.isPresent()) {
      var start = startOp.get();
      OProposeOp propose = new OProposeOp(start.promise(), operation);
      sendMessage(start.nodes(), propose);
    } else {
      action.complete(null, null, Optional.of(new ONoTransactionSequencialAvailable()));
    }
  }

  public void retryOperation(OOperationMessage operation, OCompleteAction action, int delay) {
    delayExecute(
        () -> {
          sendOperation(operation, action);
        },
        delay);
  }

  public void coordinatedOperation(OOperationMessage operation, OCompleteExecution execution) {
    OCompleteAction action = newCompleteAction(operation, execution);
    sendOperation(operation, action);
  }

  public ONodeState getNodeState() {
    return this.nodeState;
  }

  @Override
  public ONetworkMessage newNetworkMessage() {
    return new ONetworkMessageStructural(this);
  }

  public void firstConnect(ONodeId nodeId, ONodeStateNetwork state) {
    ONodeState localState = getNodeState();
    retryOperation(
        (ctx, complete) -> {
          ODiscoverAction action = localState.nodeJoinStart(nodeId, state);
          logger.debug("executing node join action %s", action);
          action.execute(this, complete, state);
          dumpNodeInfo();
        });
  }

  public void connected(ONodeId node, String url, String user, String password) {
    try {
      connectRemoteServer(node.getNode(), url, user, password);
      sendFirstConnect(node);
    } catch (IOException e) {
      logger.warn("failing to connect to remote node %s", node.getNode(), e);
    }
  }

  private void sendFirstConnect(ONodeId nodeId) {
    sendFirstConnects(Collections.singleton(nodeId));
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
              if (!ODatabaseState.Online.equals(
                  getNodeState().getDatabaseTopology().getNodeState(dbId, getNodeId()))) {
                execute(() -> sync(dbId, Optional.empty()));
              }
            });
  }

  private void sync(ODatabaseId dbId, Optional<OTransactionSequenceStatus> tx) {
    Optional<OSyncInfo> sync = getNodeState().getDatabaseTopology().newSync(dbId);
    if (sync.isPresent()) {
      logger.debug(
          "Requesting sync %s syncId %s receiver %s", dbId, sync.get().syncId(), getNodeId());
      OSyncMode mode;
      if (tx.isPresent()) {
        mode = OSyncMode.Delta;
      } else {
        mode = OSyncMode.IncrementalBackup;
      }
      var req = new OSyncRequest(getNodeId(), dbId, sync.get().syncId(), mode, tx);
      sendMessage(sync.get().targets(), req);
    } else {
      logger.warn("cannot sync missing or already synching db  %s", dbId);
    }
  }

  public void cancelDeclare(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    getNodeState().cancelDatabase(promise, dbId, database);
  }

  public void acceptSync(
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    // TODO check syncMode Accept
    ODatabasesTopologyState topology = getNodeState().getDatabaseTopology();
    boolean accepted = topology.acceptSync(getNodeState().getNodeId(), receiver, dbId, syncId);
    if (accepted) {
      logger.debug(
          "Accepted sync %s syncI: %s sender %s receiver %s", dbId, syncId, getNodeId(), receiver);
    }
    if (OSyncMode.Delta.equals(mode) && sequenceStatus.isPresent()) {
      String dbName = topology.getDatabaseName(dbId);
      List<OTransactionId> missing = getDatabase(dbName).missingTransactions(sequenceStatus.get());
      if (missing.isEmpty()) {
        accepted = false;
      }
    }
    sendMessage(receiver, new OCanSync(getNodeId(), dbId, syncId, mode, sequenceStatus, accepted));
  }

  public void canSync(
      ONodeId sender,
      ODatabaseId dbId,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    ODatabasesTopologyState topology = getNodeState().getDatabaseTopology();
    Optional<OSyncState> state =
        topology.canSync(sender, getNodeId(), dbId, syncId, canSync, mode, sequenceStatus);

    if (state.isPresent()) {
      logger.debug(
          "Receiving sync %s syncId %s sender %s receiver %s", dbId, syncId, sender, getNodeId());
      OSyncState st = state.get();
      sendMessage(sender, new OStartSync(getNodeId(), dbId, syncId, mode, sequenceStatus));
      String dbName = getDbName(dbId);
      OReceiverInputStream input = new OReceiverInputStream(this::requestNext, st);
      st.setReceiver(input);
      Thread thread =
          new Thread(
              () -> {
                receiveSync(dbName, st, input, getConfigurations());
                setDatabaseStatus(st.getDbId(), st.getReceiver(), ODatabaseState.Online);
              });
      thread.start();
    }
  }

  public void receiveSync(String dbName, OSyncState state, InputStream input, OrientDBConfig conf) {

    switch (state.getMode()) {
      case IncrementalBackup -> incrementalsSync(dbName, input, conf);

      case StandardBackup -> restore(dbName, input, null, null, null);

      case Delta -> deltaSync(dbName, input, conf);
    }
  }
  ;

  public void sendDatabase(
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    logger.debug(
        "Sending sync %s syncId %s sender %s receiver %s", dbId, syncId, getNodeId(), receiver);
    OSyncState state =
        getNodeState()
            .getDatabaseTopology()
            .startSend(receiver, getNodeState().getNodeId(), dbId, syncId, mode, sequenceStatus);
    String name = getDbName(state.getDbId());

    Thread thread =
        new Thread(
            () -> {
              syncBackup(name, state, new OutputStreamMessages(this::sendBuffer, state));
            });
    thread.start();
  }

  public void syncBackup(String name, OSyncState state, OutputStream output) {
    ODatabaseDocumentEmbedded db = openNoAuthorization(name);
    OStorage storage = db.getStorage();

    try (OutputStream out = new BufferedOutputStream(output, 8096)) {

      switch (state.getMode()) {
        case IncrementalBackup -> storage.incrementalSync(out, null);
        case StandardBackup -> {
          int compression =
              getConfigurations()
                  .getConfigurations()
                  .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION);
          storage.backup(out, null, null, null, compression, 0);
        }
        case Delta -> {
          var transactions = getDatabase(name).missingTransactions(state.getSequenceStatus().get());
          db.deltaBackup(out, transactions);
        }
      }
    } catch (IOException e) {
      logger.info("exception while sending backup data", e);
    }
  }

  private String getDbName(ODatabaseId dbId) {
    return this.getNodeState().getDatabaseTopology().getDatabaseName(dbId);
  }

  public void sendBuffer(OSyncState state, byte[] data, boolean finished) {
    logger.debug(
        "Sending buffer %s syncId %s sender %s receiver %s",
        state.getDbId(), state.getSyncId(), state.getSender(), state.getReceiver());

    if (state.isClose()) {
      // receiver sent close, drop the data.
      return;
    }
    sendMessage(state.getReceiver(), new OSyncData(state.getSyncId(), data, finished));
    state.transaferd(data.length);
    if (!finished) {
      try {
        state.waitForNext();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void receiveSyncData(OSyncId syncId, byte[] data, boolean finished) {
    var state = this.getNodeState().getDatabaseTopology().getSyncState(syncId);
    logger.debug(
        "Receiving buffer %s syncId %s sender %s receiver %s",
        state.getDbId(), state.getSyncId(), state.getSender(), state.getReceiver());
    state.receiveData(data, finished);
  }

  public void requestNext(OSyncState state, boolean close) {
    sendMessage(state.getSender(), new ONextBuffer(state.getSyncId(), close));
  }

  public void nextBuffer(OSyncId syncId, boolean close) {
    var state = this.getNodeState().getDatabaseTopology().getSyncState(syncId);
    state.requestNext(close);
  }

  public ONodeId getNodeId() {
    return getNodeState().getNodeId();
  }

  public void closeRemoteServer(String node) {
    if (remoteServerManager != null) {
      remoteServerManager.closeRemoteServer(node);
    }
  }

  public ORemoteServerController getRemoteServer(String rNodeName) {
    if (remoteServerManager != null) {
      return remoteServerManager.getRemoteServer(rNodeName);
    }
    return null;
  }

  public ORemoteServerController connectRemoteServer(
      String rNodeName, String url, String replicatorUser, String userPassword) throws IOException {
    if (remoteServerManager != null) {
      return remoteServerManager.connectRemoteServer(rNodeName, url, replicatorUser, userPassword);
    }
    return null;
  }

  public void setDatabaseStatus(ONodeId nodeId, String dbName, DB_STATUS status) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    setDatabaseStatus(dbID.get(), nodeId, ODatabaseState.from(status));

    plugin.setDatabaseStatus(nodeId.getNode(), dbName, status);
  }

  public void setDatabaseStatus(String dbName, DB_STATUS status) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    setDatabaseStatus(dbID.get(), getNodeId(), ODatabaseState.from(status));
    //
    plugin.setDatabaseStatus(getNodeId().getNode(), dbName, status);
  }

  public DB_STATUS getDatabaseStatus(ONodeId nodeId, String dbName) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    return getDatabaseStatus(dbID.get(), nodeId).toSatus();
    //
    return plugin.getDatabaseStatus(nodeId.getNode(), dbName);
  }

  public DB_STATUS getDatabaseStatus(String node, String dbName) {
    return getDatabaseStatus(new ONodeId(node), dbName);
  }

  public DB_STATUS getDatabaseStatus(String dbName) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    if (dbID.isPresent()) {
    //      return getDatabaseStatus(dbID.get(), getNodeId()).toSatus();
    //    } else {
    //      return null;
    //    }
    return plugin.getDatabaseStatus(getNodeId().getNode(), dbName);
  }

  public OServer getServer() {
    return server;
  }

  public long getNextMessageIdCounter() {
    return localMessageIdCounter.getAndIncrement();
  }

  public boolean installDatabase(
      boolean iStartup, String databaseName, boolean forceDeployment, boolean tryWithDeltaFirst) {
    //    Optional<ODatabaseId> id =
    // getNodeState().getDatabaseTopology().getDatabaseId(databaseName);
    //    if (id.isPresent()) {
    //      sync(id.get());
    //      return true;
    //    } else {
    //      return false;
    //    }
    return plugin.installDatabase(iStartup, databaseName, forceDeployment, tryWithDeltaFirst);
  }

  public Set<String> getAvailableNodeNotLocalNames(String name) {
    //    Set<String> nodes = getAvailableNodeNames(name);
    //    nodes.remove(getNodeName());
    //    return nodes;
    return plugin.getAvailableNodeNotLocalNames(name);
  }

  public Set<String> getAvailableNodeNames(String name) {
    //    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(name);
    //    if (id.isPresent()) {
    //      return getNodeState().getDatabaseTopology().getOnlineNodes(id.get()).stream()
    //          .map((x) -> x.getNode())
    //          .collect(Collectors.toSet());
    //    } else {
    //      return getNodeState().getNetworkMemebers().stream()
    //          .map((x) -> x.getNode())
    //          .collect(Collectors.toSet());
    //    }
    return plugin.getAvailableNodeNames(name);
  }

  public int getOnlineMasters(String databaseName) {
    //    ODatabasesTopologyState databaseTopology = getNodeState().getDatabaseTopology();
    //    Optional<ODatabaseId> id = databaseTopology.getDatabaseId(databaseName);
    //    if (id.isPresent()) {
    //      return (int)
    //          databaseTopology.getOnlineNodes(id.get()).stream()
    //              .filter((x) -> databaseTopology.isMain(id.get(), x))
    //              .count();
    //    } else {
    //      return 0;
    //    }
    return plugin.getAvailableNodes(databaseName);
  }

  public void enstablish(OGroupId groupId, Set<ONodeId> candidates) {
    Set<ONodeId> allNodes = getNodeState().enstablish(groupId, candidates);
    for (ONodeId node : allNodes) {
      sendFirstConnect(node);
    }
    dumpNodeInfo();
  }

  public void sendFirstConnects(Set<ONodeId> nodes) {
    ONodeStateNetwork st = getNodeState().getNetworkState();
    this.sendMessage(nodes, new ONodeFirstConnect(getNodeState().getNodeId(), st));
  }

  public List<String> getOnlineNodesNotLocal(String dbName) {
    //    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    List<String> result;
    //    if (id.isPresent()) {
    //      result =
    //          getNodeState().getDatabaseTopology().getOnlineNodes(id.get()).stream()
    //              .map((x) -> x.getNode())
    //              .collect(Collectors.toList());
    //    } else {
    //      result =
    //          getNodeState().getNetworkMemebers().stream()
    //              .map((x) -> x.getNode())
    //              .collect(Collectors.toList());
    //    }
    //    result.remove(getNodeName());
    //    return result;
    return plugin.getOnlineNodesNotLocal(dbName);
  }

  /** Returns the nodes with the requested status. */
  public int getNodesWithStatus(
      final Collection<String> iNodes, final String databaseName, final DB_STATUS... statuses) {
    //    Optional<ODatabaseId> id =
    // getNodeState().getDatabaseTopology().getDatabaseId(databaseName);
    //    ODatabasesTopologyState topology = getNodeState().getDatabaseTopology();
    //    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
    //      final String node = it.next();
    //      ODatabaseState state = topology.getNodeState(id.get(), new ONodeId(node));
    //      DB_STATUS s = state.toSatus();
    //      boolean matchState = false;
    //      for (DB_STATUS st : statuses) {
    //        if (s == st) matchState = true;
    //      }
    //      if (!matchState) it.remove();
    //    }
    //    return iNodes.size();

    return plugin.getNodesWithStatus(iNodes, databaseName, statuses);
  }

  public boolean isNodeOnline(String targetNode, String databaseName) {
    return DB_STATUS.ONLINE.equals(getDatabaseStatus(nodeName, databaseName));
  }

  public boolean isNodeAvailable(String targetNode, String databaseName) {
    final ODistributedServerManager.DB_STATUS s = getDatabaseStatus(targetNode, databaseName);
    return s != ODistributedServerManager.DB_STATUS.OFFLINE
        && s != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  public void sendMergeOperation(
      Set<ONodeId> members, OCompleteExecution execution, ONodeStateNetwork otherState) {
    Set<ODatabaseId> dbs = new HashSet<ODatabaseId>(nodeState.getDatabaseTopology().getDatabases());
    if (otherState != null) {
      dbs.removeAll(otherState.getDatabases().stream().map((x) -> x.getId()).toList());
    }
    if (dbs.isEmpty()) {
      ONodeStateNetwork st = getNodeState().getNetworkState();
      st.getTopology().setMerge(true);
      this.sendMessage(members, new ONodeFirstConnect(getNodeState().getNodeId(), st));
    } else {
      logger.warn("found joinable network, but can't merge into it with databases");
    }
  }

  public void autoDeployIfNeed() {
    Set<ONodeId> members = getNodeState().getNetworkMemebers();
    ODatabasesTopologyState databaseTopology = getNodeState().getDatabaseTopology();
    Collection<ODatabaseId> dbs = databaseTopology.getDatabases();
    for (ODatabaseId id : dbs) {
      // TODO: check autodeploy setting
      List<OAddNodeInfo> nodes = new ArrayList<OAddNodeInfo>();
      for (ONodeId node : members) {
        ODatabaseState state = databaseTopology.getDatabaseStatus(node, id);
        if (state == null) {
          nodes.add(new OAddNodeInfo(node, ONodeRole.Main));
        }
      }
      if (!nodes.isEmpty()) {
        retryOperation(
            (ctx, op) -> {
              coordinatedOperation(
                  new OAddDatabaseMember(databaseTopology.getDatabaseVersion(id), id, nodes), op);
            });
      }
    }
  }
}
