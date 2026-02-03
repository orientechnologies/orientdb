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
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.ODisconnectAction;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.OMergeCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.ORecoordinateCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.OStandardCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeRequest;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeResult;
import com.orientechnologies.orient.distributed.context.coordination.message.ONextBuffer;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeFirstConnect;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.ORetryProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStartSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncData;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncRequest;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyPing;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.ONoTransactionSequencialAvailable;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.retryable.OAddDatabaseMembersRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODeclareDatabaseRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODiscoverActionRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODropRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ORetryInfo;
import com.orientechnologies.orient.distributed.context.retryable.ORetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.OSetDatabaseStateRetryOperation;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
            }));

    var period =
        getConfigurations()
            .getConfigurations()
            .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY);
    periodicExecute(this::sendTopologyPing, period);
    periodicExecute(this::checkDisconnectedNodes, period);
  }

  private OStandardCompleteExecution newExectution(ORetryOperation operation) {
    ORetryInfo retryInfo = newRetryInfo();
    return new OStandardCompleteExecution(this, operation, retryInfo);
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {
    if (getNodeState().getDatabaseTopology().shouldSink(dbId, getNodeId())) {
      execute(() -> syncIfNeeded(dbId));
    }
    dumpNodeInfo();
  }

  public void dumpNodeInfo() {
    logger.info("current status:\n%s", ODistributedOutput.formatServerStatus(this));
  }

  private void syncIfNeeded(ODatabaseId dbId) {
    if (getNodeState().getDatabaseTopology().shouldSink(dbId, getNodeId())) {
      sync(dbId, Optional.empty());
    }
  }

  public void loadAllDatabases() {
    List<String> dbs = new ArrayList<String>(this.listDatabases(null, null));
    Collections.sort(dbs);
    for (final String databaseName : dbs) {
      if (!OSystemDatabase.SYSTEM_DB_NAME.equals(databaseName)) {
        try {
          logger.infoNode(getNodeName(), "Opening database '%s'...", databaseName);
          ODatabaseDocumentEmbedded db = openNoAuthorization(databaseName);
          if (this.nodeState.getDatabaseTopology().getDatabaseId(databaseName).isEmpty()) {
            declareDatabaseFlow(databaseName, db.getStorage().getDatbaseId()).get();
            setDatabaseState(db.getStorage().getDatbaseId(), getNodeId(), ODatabaseState.Online);
          }
          db.close();
        } catch (Exception e) {
          logger.warn("Exception on first inizialization of database '%s'", e, databaseName);
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
    OSharedContext sharedContext = getOrCreateSharedContext(storage);
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbedded(storage, sharedContext);
      embedded.init(config);
    } else {
      embedded = new ODatabaseDocumentDistributed(storage, sharedContext, plugin);
      embedded.init(config);
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

    OSharedContext sharedContext = getOrCreateSharedContext(storage);
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbedded(storage, sharedContext);
      embedded.internalCreate(config, sharedContext);
    } else {
      embedded = new ODatabaseDocumentDistributed(storage, sharedContext, plugin);
      embedded.internalCreate(config, sharedContext);
      // getOrInitDistributedConfiguration(storage.getName());
    }
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OStorage storage, OSharedContext sharedContext) {
    ODatabaseDocumentEmbedded embedded;
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbeddedPooled(pool, storage, sharedContext);
      embedded.init(pool.getConfig());
    } else {
      embedded = new ODatabaseDocumentDistributedPooled(pool, storage, sharedContext, plugin);
      embedded.init(pool.getConfig());
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

        storage =
            getDefaultEngine()
                .createForRestoreLocal(
                    this, new ODatabaseId("mock"), dbName, config.getConfigurations());

        storages.put(dbName, storage);
      }
      storage.restoreFullIncrementalBackup(backupStream);
      synchronized (this) {
        embedded = newSessionInstance(storage, config);
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

        storage =
            getDefaultEngine()
                .createForRestoreLocal(
                    this, new ODatabaseId("mock"), dbName, config.getConfigurations());

        storages.put(dbName, storage);
      }
      storage.restoreFullIncrementalBackup(backupStream);
      synchronized (this) {
        embedded = newSessionInstance(storage, config);
      }
    } catch (OModificationOperationProhibitedException e) {
      throw e;
    } catch (Exception e) {
      if (storage != null) {
        storage.delete();
      }
      storages.remove(dbName);

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

    //    if (isDistributedDisabled(name)) {
    //      super.drop(name, user, password);
    //    } else {
    //      dropFlow(name);
    //    }
  }

  private void dropFlow(String name) {
    Future<Optional<OAcceptResult>> droped = retryOperation(new ODropRetryOperation(name));
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
          logger.warn("Node %s offline could not send message %s ", node, op);
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
      } else {
        logger.warn("Node %s offline could not send message %s ", node, op);
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
    DB_STATUS dbStatus = getDatabaseStatus(name);
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
    setDatabaseState(dbId, getNodeState().getNodeId(), ODatabaseState.Online);
    try {
      getNodeState().getOps().waitOnlineQuorum(dbId, Optional.empty());
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("wait for online interrupted"), e);
    }
  }

  private Future<Optional<OAcceptResult>> declareDatabaseFlow(String name, ODatabaseId dbId) {
    var members =
        getNodeState().getNetworkMembers().stream()
            .map((n) -> new OAddNodeInfo(n, ONodeRole.Main))
            .collect(Collectors.toSet());
    int minimumQuorum = members.size() / 2 + 1;
    return retryOperation(new ODeclareDatabaseRetryOperation(dbId, name, members, minimumQuorum));
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

  private Future<Optional<OAcceptResult>> setDatabaseState(
      ODatabaseId dbId, ONodeId node, ODatabaseState state) {
    return retryOperation(new OSetDatabaseStateRetryOperation(node, dbId, state));
  }

  private ODatabaseState getDatabaseState(ODatabaseId dbId, ONodeId node) {
    return this.getNodeState().getDatabaseTopology().getState(dbId, node);
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
    threadsGroup.interrupt();
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

  public OStandardCompleteAction newCompleteAction(
      OOperationMessage operation, OCompleteExecution execution) {
    return new OStandardCompleteAction(this, operation, execution);
  }

  public ORecoordinateCompleteAction newRecoordinateAction(OOperationMessage operation) {
    return new ORecoordinateCompleteAction(this, operation);
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
    logger.debugNode(getNodeId(), "starting operation %s", operation);
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

  public void firstConnect(ONodeId nodeId, ONodeStateNetwork state, boolean merge) {
    if (nodeId.equals(getNodeId())) {
      return;
    }
    ODiscoverAction action = getNodeState().getOps().nodeJoinStart(nodeId, state, merge);
    logger.debugNode(getNodeId(), "executing node join action %s", action);
    retryOperation(new ODiscoverActionRetryOperation(action));
  }

  public void connected(ONodeId node, String url, String user, String password) {
    try {
      connectRemoteServer(node.getNode(), url, user, password);
      //      sendFirstConnect(node);
    } catch (IOException e) {
      logger.warn("failing to connect to remote node %s", node.getNode(), e);
    }
  }

  private void sendFirstConnect(ONodeId nodeId) {
    sendFirstConnects(Collections.singleton(nodeId));
  }

  public void registerNode(ONodeId node, long version, OTransactionIdPromise promise) {
    getNodeState().getOps().registerNode(node, version, promise);
    // This should make aware of the added node of the fact it joined the network
    sendFirstConnect(node);
    autoDeployIfNeed();
  }

  public void cancelRegisterPromise(OTransactionIdPromise promise) {
    getNodeState().getOps().cancelRegisterNode(promise);
  }

  public Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    return getNodeState()
        .getOps()
        .validateDeclareDatabase(promise, databaseId, database, partecipants, minimumQuorum);
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    getNodeState().getOps().declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
    getNodeState()
        .getOps()
        .executeOnOneOnline(
            dbId,
            () -> {
              if (!ODatabaseState.Online.equals(
                  getNodeState().getDatabaseTopology().getState(dbId, getNodeId()))) {
                execute(() -> sync(dbId, Optional.empty()));
              }
            });
  }

  private void sync(ODatabaseId dbId, Optional<OTransactionSequenceStatus> tx) {
    Optional<OSyncInfo> sync = getNodeState().getOps().newSync(dbId);
    if (sync.isPresent()) {
      logger.debug(
          "Requesting sync %s syncId %s receiver %s", dbId, sync.get().syncId(), getNodeId());
      OSyncMode mode;
      if (tx.isPresent()) {
        mode = OSyncMode.Delta;
      } else {
        // TODO: here should check if it support the incremental, also that at the receiving side.
        mode = OSyncMode.IncrementalBackup;
      }
      var req = new OSyncRequest(getNodeId(), dbId, sync.get().syncId(), mode, tx);
      sendMessage(sync.get().targets(), req);
    } else {
      logger.warn("cannot sync missing or already synching db  %s", dbId);
    }
  }

  public void cancelDeclare(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    getNodeState().getOps().cancelDeclareDatabase(promise, dbId, database);
  }

  public void acceptSync(
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    // TODO check syncMode Accept
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    boolean accepted = ops.acceptSync(getNodeState().getNodeId(), receiver, dbId, syncId);
    if (accepted) {
      logger.debug(
          "Accepted sync %s syncI: %s sender %s receiver %s", dbId, syncId, getNodeId(), receiver);
    }
    String dbName = ops.getDatabaseTopology().getDatabaseName(dbId);
    if (OSyncMode.Delta.equals(mode) && sequenceStatus.isPresent()) {
      List<OTransactionId> missing = getDatabase(dbName).missingTransactions(sequenceStatus.get());
      if (missing.isEmpty()) {
        accepted = false;
      }
    }
    if (OSyncMode.IncrementalBackup.equals(mode)) {
      OStorage storage = getStorage(dbName);
      if (storage != null) {
        if (!storage.supportIncremental()) {
          mode = OSyncMode.StandardBackup;
        }
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
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    Optional<OSyncState> state =
        ops.canSync(sender, getNodeId(), dbId, syncId, canSync, mode, sequenceStatus);

    if (state.isPresent()) {
      logger.debug(
          "Receiving sync %s syncId %s sender %s receiver %s", dbId, syncId, sender, getNodeId());
      OSyncState st = state.get();
      sendMessage(sender, new OStartSync(getNodeId(), dbId, syncId, mode, sequenceStatus));
      String dbName = getDbName(dbId);
      OReceiverInputStream input = new OReceiverInputStream(this::requestNext, st);
      st.setReceiverStream(input);
      runOnThread(
          () -> {
            receiveSync(dbName, st, input, getConfigurations());
          });
    }
  }

  public void receiveSync(
      String dbName, OSyncState state, InputStream inputStream, OrientDBConfig conf) {
    try (InputStream input = inputStream) {
      switch (state.getMode()) {
        case IncrementalBackup -> incrementalsSync(dbName, input, conf);

        case StandardBackup -> restore(dbName, input, null, null, null);

        case Delta -> deltaSync(dbName, input, conf);
      }
      setDatabaseState(state.getDbId(), state.getReceiver(), ODatabaseState.Online);
    } catch (IOException e) {
      logger.debug("Error on close of sync", e);
    } finally {
      getNodeState().getOps().completeSync(state.getSyncId());
    }
  }

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
            .getOps()
            .startSend(receiver, getNodeState().getNodeId(), dbId, syncId, mode, sequenceStatus);
    String name = getDbName(state.getDbId());

    runOnThread(
        () -> {
          syncBackup(name, state, new OutputStreamMessages(this::sendBuffer, state));
        });
  }

  public void syncBackup(String name, OSyncState state, OutputStream output) {
    try (OutputStream out = new BufferedOutputStream(output, 8096)) {
      ODatabaseDocumentEmbedded db = openNoAuthorization(name);
      OStorage storage = db.getStorage();

      switch (state.getMode()) {
        case IncrementalBackup -> {
          storage.incrementalSync(out, null);
        }
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
    } finally {
      getNodeState().getOps().completeSync(state.getSyncId());
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
    var state = this.getNodeState().getOps().getSyncState(syncId);
    logger.debug(
        "Receiving buffer %s syncId %s sender %s receiver %s",
        state.getDbId(), state.getSyncId(), state.getSender(), state.getReceiver());
    state.receiveData(data, finished);
  }

  public void requestNext(OSyncState state, boolean close) {
    sendMessage(state.getSender(), new ONextBuffer(state.getSyncId(), close));
  }

  public void nextBuffer(OSyncId syncId, boolean close) {
    var state = this.getNodeState().getOps().getSyncState(syncId);
    if (state != null) {
      state.requestNext(close);
    }
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
    } else {
      logger.warn("failed to connect server manager not initied");
    }
    return null;
  }

  public void setDatabaseStatus(ONodeId nodeId, String dbName, DB_STATUS status) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    if (dbID.isPresent()) {
    //      setDatabaseState(dbID.get(), nodeId, ODatabaseState.from(status));
    //    } else {
    //      logger.warn("setting database status to %s, for not defined db %s", status, dbName);
    //    }

    plugin.setDatabaseStatus(nodeId.getNode(), dbName, status);
  }

  public void setDatabaseStatus(String dbName, DB_STATUS status) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    if (dbID.isPresent()) {
    //      setDatabaseState(dbID.get(), getNodeId(), ODatabaseState.from(status));
    //    } else {
    //      logger.warn("setting database status to %s, for not defined db %s", status, dbName);
    //    }
    plugin.setDatabaseStatus(getNodeId().getNode(), dbName, status);
  }

  public DB_STATUS getDatabaseStatus(ONodeId nodeId, String dbName) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    if (dbID.isPresent()) {
    //      ODatabaseState status = getDatabaseState(dbID.get(), nodeId);
    //      if (status != null) {
    //        return status.toSatus();
    //      }
    //    }
    //    return null;

    return plugin.getDatabaseStatus(nodeId.getNode(), dbName);
  }

  public DB_STATUS getDatabaseStatus(String node, String dbName) {
    return getDatabaseStatus(new ONodeId(node), dbName);
  }

  public DB_STATUS getDatabaseStatus(String dbName) {
    //    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    //    if (dbID.isPresent()) {
    //      ODatabaseState status = getDatabaseState(dbID.get(), getNodeId());
    //      if (status != null) {
    //        return status.toSatus();
    //      } else {
    //        return null;
    //      }
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
    //      sync(id.get(), Optional.empty());
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
    //      return getNodeState().getNetworkMembers().stream()
    //          .map((x) -> x.getNode())
    //          .collect(Collectors.toSet());
    //    }
    return plugin.getAvailableNodeNames(name);
  }

  public int getOnlineMasters(String databaseName) {
    //    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
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

  public void establish(OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    Set<ONodeId> allNodes = getNodeState().getOps().establish(groupId, candidates, promise);
    for (ONodeId node : allNodes) {
      sendFirstConnect(node);
    }
    dumpNodeInfo();
  }

  public void sendFirstConnects(Set<ONodeId> nodes) {
    ONodeStateNetwork st = getNodeState().getNetworkState();
    this.sendMessage(nodes, new ONodeFirstConnect(getNodeState().getNodeId(), st, false));
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
    //          getNodeState().getNetworkMembers().stream()
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
    //    ODatabasesTopology topology = getNodeState().getDatabaseTopology();
    //    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
    //      final String node = it.next();
    //      ODatabaseState state = topology.getState(id.get(), new ONodeId(node));
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

  public void sendMergeOperation(ONodeId requestToMerge, OCompleteExecution execution) {
    ONodeState ns = getNodeState();
    sendMessage(requestToMerge, new ONodeFirstConnect(ns.getNodeId(), ns.getNetworkState(), true));
  }

  public void autoDeployIfNeed() {
    Set<ONodeId> members = getNodeState().getNetworkMembers();
    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
    Collection<ODatabaseId> dbs = databaseTopology.getDatabases();
    for (ODatabaseId id : dbs) {
      // TODO: check autodeploy setting
      List<OAddNodeInfo> nodes = new ArrayList<OAddNodeInfo>();
      for (ONodeId node : members) {
        ODatabaseState state = databaseTopology.getState(id, node);
        if (ODatabaseState.NotAvailable.equals(state)) {
          nodes.add(new OAddNodeInfo(node, ONodeRole.Main));
        }
      }
      if (!nodes.isEmpty()) {
        retryOperation(new OAddDatabaseMembersRetryOperation(nodes, id));
      }
    }
  }

  public void sendEstablish(
      OGroupId groupId, Set<ONodeId> candidates, OCompleteExecution execution) {
    OEstablishTopology operation = new OEstablishTopology(groupId, candidates);
    Optional<OTransactionIdPromise> promise =
        getNodeState().getOps().startEstablish(candidates, newCompleteAction(operation, execution));
    if (promise.isPresent()) {
      sendMessage(candidates, new OProposeOp(promise.get(), operation));
    } else {
      execution.complete(Optional.of(new ONoTransactionSequencialAvailable()));
    }
  }

  public void sendMergeNodeAction(ONodeId node, OCompleteExecution execution) {
    // This should do a two phase operation in the current network, but also ask for
    // permission to the merging node, to avoid in a two network and a node case to make
    // the node join both networks.
    long version = getNodeState().getOps().nextTopologyVersion();
    var operation = new OAddTopologyMember(version, node);
    OCompleteAction action = new OMergeCompleteAction(this, operation, execution, node);
    logger.debugNode(getNodeId(), "starting operation %s", operation);
    var startOp = getNodeState().start(action);
    if (startOp.isPresent()) {
      var start = startOp.get();
      OProposeOp propose = new OProposeOp(start.promise(), operation);
      sendMessage(start.nodes(), propose);
      sendMessage(
          node, new OMergeRequest(start.promise(), this.getNodeState().getOps().getGroupId()));
    } else {
      action.complete(null, null, Optional.of(new ONoTransactionSequencialAvailable()));
    }
  }

  public void acceptMerge(OTransactionIdPromise promise, OGroupId group) {
    var accepted = getNodeState().getOps().validateMerge(group, promise);
    sendMessage(promise.getCoordinator(), new OMergeResult(getNodeId(), promise, accepted));
  }

  public void cancelMerge(OTransactionIdPromise promise) {
    getNodeState().getOps().cancelMerge(promise);
  }

  public void applyMerge(OTransactionIdPromise promise) {
    // Do Nothing for now, just wait for new network  notification
  }

  public void confirmMerge(
      ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted) {
    getNodeState().getOps().confirmMerge(node, promise, accepted);
  }

  public void disconnected(ONodeId node) {
    ODisconnectAction action = getNodeState().getOps().nodeDisconnected(node);
    action.execute(this);
  }

  public void recoordinateOperation(OTransactionIdPromise promise, OOperationMessage op) {
    OCompleteAction action = newRecoordinateAction(op);

    conseunsusOperation(promise, op, action);
  }

  private void conseunsusOperation(
      OTransactionIdPromise prePromise, OOperationMessage op, OCompleteAction action) {
    var startOp = getNodeState().getOps().restart(prePromise, action);
    if (startOp.isPresent()) {
      var promise = startOp.get().promise();
      var nodes = startOp.get().nodes();
      ORetryProposeOp propose = new ORetryProposeOp(promise, op);
      sendMessage(nodes, propose);
    }
  }

  private void sendTopologyPing() {
    var members = getNodeState().getOps().getNetworkMembers();
    sendMessage(
        members,
        new OTopologyPing(getNodeId(), getNodeState().getOps().getTransactionSequenceState()));
  }

  public void receivePing(ONodeId nodeId, OTransactionSequenceStatus status) {
    getNodeState().getOps().receivePing(nodeId, status);
  }

  private void checkDisconnectedNodes() {
    var time =
        getConfigurations()
            .getConfigurations()
            .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT);
    var offlineNodes = getNodeState().getOps().checkOffline(time);
    for (var offlineNode : offlineNodes) {
      var action = getNodeState().getOps().nodeDisconnected(offlineNode);
      action.execute(this);
    }
  }
}
