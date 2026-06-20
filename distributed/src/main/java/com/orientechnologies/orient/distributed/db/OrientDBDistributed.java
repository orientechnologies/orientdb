package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.thread.OCompletedFuture;
import com.orientechnologies.common.thread.OSourceTraceExecutorService;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.core.OConstants;
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
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.ONodeListenerConfig;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.OStatsManager;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.OMergeCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.ORecoordinateCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.action.OStandardCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.OCanSyncResult;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSyncAccept;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmedOps;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmedRetryOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeRequest;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeResult;
import com.orientechnologies.orient.distributed.context.coordination.message.ONextBuffer;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeFirstConnect;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeInfoListener;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStatsNotify;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.ORetryProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OSendTransactions;
import com.orientechnologies.orient.distributed.context.coordination.message.OStartSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncData;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncRequest;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyPing;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OMergeNode;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.ONoTransactionSequencialAvailable;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode.Delta;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode.NonBlockingBackup;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.retryable.OAddDatabaseMembersRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODeclareDatabaseRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODiscoverActionRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ODropRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.OInitRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ORemoveMemberRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.ORetryInfo;
import com.orientechnologies.orient.distributed.context.retryable.ORetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.OSetDatabaseNodeRoleRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.OSetDatabaseQuorumRetryOperation;
import com.orientechnologies.orient.distributed.context.retryable.OSetDatabaseStateRetryOperation;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.OWriteOperationNotPermittedException;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
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
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseSequenceStatusTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded
    implements OServerAware, ODatabaseStateChangeListener, OOperationContext {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OrientDBDistributed.class);
  private volatile OServer server;
  private volatile ODistributedPlugin plugin;
  private final ConcurrentHashMap<String, ODistributedConfigurationManager> configurations =
      new ConcurrentHashMap<>();

  private final ODistributedMessageServiceImpl messageService;
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

  private ExecutorService newNetIoExecutor() {
    int ioSize = excutorMaxSize(OGlobalConfiguration.EXECUTOR_POOL_MAX_SIZE);
    ExecutorService exec = OThreadPoolExecutors.newThreadPool("NetIO", allGroups, ioSize, ioSize);
    if (getBoolConfig(OGlobalConfiguration.EXECUTOR_DEBUG_TRACE_SOURCE)) {
      exec = new OSourceTraceExecutorService(exec);
    }
    return exec;
  }

  @Override
  public void init(OServer server) {
    // Cannot get the plugin from here, is too early, doing it lazy
    this.server = server;
  }

  public void initDistributed(ONodeConfiguration config) {
    initDistributed(config.getNodeName(), config.getGroupName(), config.getQuorum());
  }

  public void initDistributed(String nodeName, String groupIdPar, int miminumQuorum) {
    this.nodeName = nodeName;
    ONodeId nodeId = new ONodeId(nodeName);
    OGroupId groupId = new OGroupId(groupIdPar);
    OSystemStateStore store = new OSystemStateStore(getSystemDatabase());
    this.nodeState = new ONodeState(nodeId, groupId, miminumQuorum, store, this);
    var check =
        new ORemoteServerAvailabilityCheck() {

          @Override
          public void nodeDisconnected(ONodeId node) {
            closeRemoteServer(node);
            disconnected(nodeId);
          }

          @Override
          public boolean isNodeAvailable(ONodeId node) {
            return false;
          }
        };

    this.remoteServerManager = new ORemoteServerManager(nodeId, check, newNetIoExecutor());
    ODiscoverAction action = this.nodeState.initFromStore();

    action.execute(this, null, newExectution(new OInitRetryOperation(action)));

    reconciliateState();
    this.nodeState.getOps().executeOnEnstablish(() -> execute(this::loadAllDatabases));

    var period = getLongConfig(OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY);
    if (period >= 0) {
      logger.warn("invalid value for health check period using default value");
      period = 10000l;
    }
    periodicExecute(this::sendTopologyPing, period);
    periodicExecute(this::sendDatabasesPing, period);
    periodicExecute(this::checkDisconnectedNodes, period);
    periodicExecute(this::checkOperationsTimeout, period);

    var statsPeriod = period * 10;
    periodicExecute(this::sendStats, statsPeriod);
  }

  private void sendDatabasesPing() {
    if (isDistributedDisabled() || !getOps().getNetworkTopology().isSelfEnstablished()) return;
    execute(
        () -> {
          var dbTopology = getOps().getDatabaseTopology();

          for (var dbId : dbTopology.getDatabases()) {
            if (dbTopology.isOnline(dbId, getNodeId())) continue;
            var dbName = dbTopology.getDatabaseName(dbId);
            try {
              var status =
                  getSharedDatabaseContext(dbName)
                      .map(x -> x.getTransactionSequence().currentStatus());
              final List<String> servers = getOnlineNodesNotLocal(dbName);
              if (status.isPresent() && !servers.isEmpty()) {
                ORemoteTask task = new OUpdateDatabaseSequenceStatusTask(dbName, status.get());
                plugin.sendRequest(dbName, servers, task);
              }
            } catch (ODistributedException | ODistributedOperationException e) {
              logger.debugNode(getNodeId(), "Error on sending request for cluster health check", e);
            }
          }
        });
  }

  private void sendStats() {
    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;

    var members = getNodeState().getOps().getNetworkTopology().getMembers();
    sendMessage(
        members,
        new ONodeStatsNotify(
            getNodeId(),
            bootTime,
            maxMem,
            totMem,
            freeMem,
            usedMem,
            getMessageService().getNodesLatencies(),
            getMessageService().getNodesMessages()));
  }

  public void reconciliateState() {
    ODatabasesTopology dt = this.nodeState.getDatabaseTopology();
    var dbs = dt.getDatabases();
    for (var db : dbs) {
      var dbName = dt.getDatabaseName(db);
      if (!this.exists(dbName, null, null)) {
        this.nodeState.getOps().dbRemovedFromDiskWhenOffline(db);
      }
    }
  }

  private OStandardCompleteExecution newExectution(ORetryOperation operation) {
    ORetryInfo retryInfo = newRetryInfo();
    return new OStandardCompleteExecution(this, operation, retryInfo);
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {
    autoDeployIfNeed();
    syncIfNeeded(dbId);
    autoAssignAllocation(dbId);
    dumpNodeInfo();
    notifyLegacyStateListener(dbId, nodeId, state);
  }

  private void notifyLegacyStateListener(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {
    if (plugin == null) return;
    String node = nodeId.getNode();
    String db = getNodeState().getOps().getDatabaseTopology().getDatabaseName(dbId);
    execute(
        () -> {
          if (plugin != null) {
            plugin.onDatabaseEvent(node, db, state.toSatus());
          }
        });
  }

  public void dumpNodeInfo() {
    logger.info("%s", ODistributedOutput.formatServerStatus(this));
  }

  private void syncIfNeeded(ODatabaseId dbId) {
    if (getNodeState().getDatabaseTopology().shouldSink(dbId, getNodeId())) {
      sync(dbId, Optional.empty());
    }
  }

  public void autoAssignAllocation(ODatabaseId dbId) {
    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
    if (databaseTopology.isQuorumOnline(dbId) && databaseTopology.isOnline(dbId, getNodeId())) {
      var name = databaseTopology.getDatabaseName(dbId);
      executeNoAuthorizationOnActive(
          name, db -> ((ODatabaseDocumentInternal) db).autoAssignAllocations(true));
    }
  }

  private void syncIfNeededAll() {
    var dbp = getNodeState().getDatabaseTopology();
    for (var dbId : dbp.getDatabases()) {
      if (dbp.shouldSink(dbId, getNodeId())) {
        sync(dbId, Optional.empty());
      }
    }
  }

  @Override
  public void loadAllDatabases() {
    List<String> dbs = new ArrayList<>(this.listDatabases(null, null));
    Collections.sort(dbs);
    for (final String databaseName : dbs) {
      if (!OSystemDatabase.SYSTEM_DB_NAME.equals(databaseName)) {
        try (ODatabaseDocumentEmbedded db = openNoAuthorization(databaseName)) {
          logger.infoNode(getNodeName(), "Opening database '%s'...", databaseName);
          if (this.nodeState.getDatabaseTopology().getDatabaseId(databaseName).isEmpty()) {
            declareDatabaseFlow(databaseName, db.getStorage().getDatabaseId()).get();
            setDatabaseState(db.getStorage().getDatabaseId(), getNodeId(), ODatabaseState.Online);
          } else {
            setDatabaseState(db.getStorage().getDatabaseId(), getNodeId(), ODatabaseState.Online);
          }
        } catch (Exception e) {
          logger.warn("Exception on first inizialization of database '%s'", e, databaseName);
        }
      }
    }
  }

  public ODistributedPlugin getPlugin() {
    if (plugin == null && server != null) {
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

  @Override
  protected OSharedContextEmbedded createSharedContext(OStorage storage) {
    if (isDistributedDisabled(storage.getName())) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  @Override
  protected ODatabaseDocumentEmbedded newSessionInstance(String database, OrientDBConfig config) {
    ODatabaseDocumentEmbedded embedded;
    OSharedContextEmbedded sharedContext =
        getOrCreateSharedContext(database, config.getConfigurations());
    if (isDistributedDisabled(database)) {
      embedded = new ODatabaseDocumentEmbedded(sharedContext);
      embedded.init(config);
    } else {
      embedded = new ODatabaseDocumentDistributed(sharedContext, plugin);
      embedded.init(config);
    }
    return embedded;
  }

  public boolean isDistributedDisabled(String storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage)) {
      return true;
    } else {
      return isDistributedDisabled();
    }
  }

  protected boolean isDistributedDisabled() {
    if (plugin == null || !plugin.isEnabled()) {
      return nodeState == null;
    } else {
      return false;
    }
  }

  @Override
  protected ODatabaseDocumentEmbedded newCreateSessionInstance(
      OStorage storage, OrientDBConfig config) {
    ODatabaseDocumentEmbedded embedded;

    OSharedContextEmbedded sharedContext =
        sharedContexts.computeIfAbsent(storage.getName(), k -> createSharedContext(storage));
    if (isDistributedDisabled(storage.getName())) {
      embedded = new ODatabaseDocumentEmbedded(sharedContext);
      embedded.internalCreate(config, sharedContext);
    } else {
      embedded = new ODatabaseDocumentDistributed(sharedContext, plugin);
      embedded.internalCreate(config, sharedContext);
    }
    return embedded;
  }

  @Override
  protected ODatabaseDocumentEmbedded onlyOpenNoAuthorization(String name) {
    checkDatabaseName(name);
    try {
      final ODatabaseDocumentEmbedded embedded;
      checkOpen();
      OSharedContextEmbedded sharedContext = sharedContexts.get(name);
      if (sharedContext != null
          && sharedContext.isLoaded()
          && sharedContext.getStorage().isOpen()) {
        if (isDistributedDisabled(sharedContext.getStorage().getName())) {
          embedded = new ODatabaseDocumentEmbedded(sharedContext);
        } else {
          embedded = new ODatabaseDocumentDistributed(sharedContext, plugin);
        }
        OrientDBConfig config = solveConfig(null);
        embedded.init(config);
        return embedded;
      } else {
        return null;
      }
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, String name) {
    OSharedContextEmbedded sharedContext =
        getOrCreateSharedContext(name, getConfigurations().getConfigurations());
    ODatabaseDocumentEmbedded embedded;
    if (isDistributedDisabled(name)) {
      embedded = new ODatabaseDocumentEmbeddedPooled(pool, sharedContext);
      embedded.init(pool.getConfig());
    } else {
      embedded = new ODatabaseDocumentDistributedPooled(pool, sharedContext, plugin);
      embedded.init(pool.getConfig());
    }
    return embedded;
  }

  public void setPlugin(ODistributedPlugin plugin) {
    this.plugin = plugin;
  }

  public boolean nonBlockingSync(
      String name, ODatabaseId databaseId, InputStream backupStream, OrientDBConfig config) {
    ODatabaseDocumentEmbedded embedded;

    if (!isOpen()) {
      return false;
    }
    try {
      OSharedContextEmbedded context =
          sharedContexts.computeIfAbsent(
              name,
              (k) -> {
                var storage =
                    getDefaultEngine()
                        .createForRestoreLocal(
                            OrientDBDistributed.this, databaseId, k, config.getConfigurations());
                return createSharedContext(storage);
              });
      context.unload();
      context.getStorage().restoreFullIncrementalBackup(backupStream);
      synchronized (this) {
        embedded = newSessionInstance(name, config);
      }
      embedded.getSharedContext().reInit(context.getStorage(), embedded);
      distributedSetOnline(embedded.getSharedContext());
      ODatabaseRecordThreadLocal.instance().remove();
      return true;
    } catch (OModificationOperationProhibitedException e) {
      throw e;
    } catch (Exception e) {
      logger.warnNode(getNodeId(), "failed non blocking sync of database %s", e, name);
      synchronized (this) {
        sharedContexts.remove(name);
      }
      return false;
    }
  }

  @Override
  public void internalDrop(String name) {
    if (!exists(name, null, null)) {
      return;
    }
    synchronized (this) {
      checkOpen();
      // This is a temporary fix for distributed drop that avoid scheduled view update to re-open
      // the distributed database while is dropped
      OSharedContextEmbedded sharedContext = sharedContexts.get(name);
      if (sharedContext != null) {
        sharedContext.getViewManager().close();
      }
    }

    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try (ODatabaseDocumentInternal db = openNoAuthenticate(name, null)) {

      for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDrop(db);
      }
      db.callOnDropListeners();
    } catch (OStorageException | ODatabaseException e) {
      logger.warnNoDb("Error opening %s for drop hook call ", name, e);
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }

    unregisterDatabase(name);
    synchronized (this) {
      if (exists(name, null, null)) {
        OSharedContextEmbedded sharedContext =
            getOrCreateSharedContext(name, getConfigurations().getConfigurations());
        sharedContext.close();
        sharedContext.getStorage().delete();
        dropStorageFiles(sharedContext.getStorage());
        sharedContexts.remove(name);
      }
    }
  }

  @Override
  public void drop(String name, String user, String password) {
    if (isDistributedDisabled(name)) {
      super.drop(name, user, password);
    } else {
      dropFlow(name);
    }
  }

  private void dropFlow(String name) {
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    var id = ops.getDatabaseTopology().getDatabaseId(name);
    if (id.isPresent()) {
      Future<Optional<OAcceptResult>> droped = retryOperation(new ODropRetryOperation(id.get()));
      try {
        droped.get(10, TimeUnit.MINUTES);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        logger.debug("fail wait on drop", e);
      }
    } else {
      logger.warn("no database defined with name %s", name);
    }
  }

  public void sendMessage(Set<ONodeId> set, OStructuralMessage op) {
    ONetworkMessageStructural message = new ONetworkMessageStructural(this, op);
    ORemoteServerManager remote = remoteServerManager;
    for (ONodeId node : set) {
      if (node.equals(getNodeState().getNodeId())) {
        this.receiveMessage(op);
      } else {
        ORemoteServerController rem = remote.getRemoteServer(node);
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
      ORemoteServerController rem = remote.getRemoteServer(node);
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
    long waitTime = getLongConfig(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);
    try {
      if (this.nodeState == null) {
        return true;
      } else {
        return this.nodeState.getOps().waitSelfOnline(name, Optional.of(waitTime));
      }
    } catch (InterruptedException e) {
      return false;
    }
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
    if (isDistributedDisabled()) {
      return;
    }
    for (var db : getNodeState().getDatabaseTopology().getDatabases()) {
      setDatabaseState(db, getNodeId(), ODatabaseState.Offline);
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

  public Optional<OSharedContextEmbedded> getSharedDatabaseContext(String database) {
    return Optional.ofNullable(sharedContexts.get(database));
  }

  public ODistributedDatabaseImpl unregisterDatabase(final String iDatabaseName) {
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

    if (isDistributedDisabled(name)) {
      super.create(name, user, password, type, id, config, createOps);
    } else {
      createDatabaseFlow(name, user, password, type, id, config, createOps);
    }
  }

  private void createDatabaseFlow(
      String name,
      String user,
      String password,
      ODatabaseType type,
      ODatabaseId dbId,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    try {
      declareDatabaseFlow(name, dbId).get(10, TimeUnit.MINUTES);
      super.create(name, user, password, type, dbId, config, createOps);
      setDatabaseState(dbId, getNodeState().getNodeId(), ODatabaseState.Online);
      getNodeState().getOps().waitOnlineAll(dbId, Optional.of(10 * 60 * 1000L));
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("wait for online interrupted"), e);
    } catch (ExecutionException | TimeoutException e) {
      throw OException.wrapException(
          new ODatabaseException("Failed execution of declare database"), e);
    }
  }

  public Future<Optional<OAcceptResult>> declareDatabaseFlow(String name, ODatabaseId dbId) {
    return retryOperation(new ODeclareDatabaseRetryOperation(dbId, name));
  }

  public Future<Optional<OAcceptResult>> retryOperation(ORetryOperation operation) {
    OStandardCompleteExecution exec = newExectution(operation);
    execute(() -> operation.execute(this, exec, Optional.empty()));
    return exec.getResult();
  }

  public void retryExecution(
      ORetryOperation operation,
      OCompleteExecution exec,
      int delay,
      Optional<OAcceptResult> result) {
    delayExecute(() -> operation.execute(this, exec, result), delay);
  }

  private Future<Optional<OAcceptResult>> setDatabaseState(
      ODatabaseId dbId, ONodeId node, ODatabaseState state) {
    return retryOperation(new OSetDatabaseStateRetryOperation(node, dbId, state));
  }

  public ODatabaseState getDatabaseState(ODatabaseId dbId, ONodeId node) {
    return this.getNodeState().getDatabaseTopology().getState(dbId, node);
  }

  public boolean isDatabaseOnline(String dbName) {
    ODatabasesTopology dbTopology = getNodeState().getDatabaseTopology();
    Optional<ODatabaseId> dbID = dbTopology.getDatabaseId(dbName);
    if (dbID.isPresent()) {
      return dbTopology.isOnline(dbID.get(), getNodeId());
    } else {
      return false;
    }
  }

  @Override
  public void distributedSetOnline(OSharedContextEmbedded ctx) {
    ((OSharedContextDistributed) ctx).getDistributedContext().setOnline();
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
        .map(x -> ((OSharedContextDistributed) x).getDistributedContext())
        .toList();
  }

  public ODistributedConfiguration getOrInitDistributedConfiguration(ODatabaseSession session) {
    return getOrInitConfigurationManager(session.getName()).getDistributedConfiguration(session);
  }

  public ODistributedConfigurationManager getOrInitConfigurationManager(String database) {
    return configurations.computeIfAbsent(
        database, key -> new ODistributedConfigurationManager(this, plugin, key));
  }

  public ODistributedConfigurationManager getConfigurationManager(String database) {
    return configurations.get(database);
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
          return null;
        });
  }

  public void saveDatabaseConfiguration(String database) {
    ODistributedConfigurationManager cm = getOrInitConfigurationManager(database);
    configOp(
        cm,
        database,
        (m, s) -> {
          m.saveDatabaseConfiguration(s);
          return null;
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
    return this.sharedContexts.size();
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
    int retryCountDown = getIntConfig(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int delay = getIntConfig(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
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
    delayExecute(() -> sendOperation(operation, action), delay);
  }

  public void coordinatedOperation(OOperationMessage operation, OCompleteExecution execution) {
    OCompleteAction action = newCompleteAction(operation, execution);
    sendOperation(operation, action);
  }

  public ONodeState getNodeState() {
    return this.nodeState;
  }

  public OCoordinatedDistributedOps getOps() {
    return getNodeState().getOps();
  }

  @Override
  public ONetworkMessage newNetworkMessage() {
    return new ONetworkMessageStructural(this);
  }

  public void firstConnect(ONodeId nodeId, ONodeStateNetwork state, boolean merge, ONodeInfo info) {
    if (nodeId.equals(getNodeId())) {
      return;
    }
    if (this.remoteServerManager != null) {
      this.remoteServerManager.registerRemoteAddresses(nodeId, info.listeners());
    }
    ODiscoverAction action = getNodeState().getOps().nodeJoinStart(nodeId, state, merge);
    logger.debugNode(getNodeId(), "executing node join action %s", action);
    retryOperation(new ODiscoverActionRetryOperation(action, state));
  }

  public void connected(ONodeId node, String url, String user, String password) {
    try {
      connectRemoteServer(node, url, user, password);
      sendFirstConnect(node);
    } catch (IOException e) {
      logger.warn("failing to connect to remote node %s", node.getNode(), e);
    }
  }

  private void sendFirstConnect(ONodeId nodeId) {
    sendFirstConnects(Collections.singleton(nodeId));
  }

  public void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise) {
    getNodeState().getOps().registerNode(node, version, promise);
    // This should make aware of the added node of the fact it joined the network
    sendFirstConnect(node);
    autoDeployIfNeed();
    dumpNodeInfo();
    notifyLegacyNodeJoinListener(node);
  }

  private void notifyLegacyNodeJoinListener(ONodeId node) {
    if (plugin == null) return;
    execute(
        () -> {
          if (plugin != null) {
            plugin.notifyNodeJoined(node.getNode());
          }
        });
  }

  private void notifyLegacyNodeLeftListener(ONodeId node) {
    if (plugin == null) return;
    execute(
        () -> {
          if (plugin != null) {
            plugin.notifyNodeLeft(node.getNode());
          }
        });
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    getNodeState().getOps().declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
    getNodeState().getOps().executeOnOneOnline(dbId, () -> syncIfNeeded(dbId));
  }

  private Optional<Future<Boolean>> sync(
      ODatabaseId dbId, Optional<OTransactionSequenceStatus> tx) {
    Optional<OSyncInfo> sync = getNodeState().getOps().newSync(dbId);
    if (sync.isPresent()) {
      logger.debugNode(
          getNodeId(),
          "Requesting sync of %s to %s syncId %s receiver %s",
          dbId,
          sync.get().targets(),
          sync.get().syncId(),
          getNodeId());
      OSyncMode mode;
      if (tx.isPresent()) {
        mode = new OSyncMode.Delta(tx.get());
      } else {
        // TODO: here should check if it support the incremental, also that at the receiving side.
        mode = new OSyncMode.BlockingBackup();
      }
      var req = new OSyncRequest(sync.get().syncId(), mode);
      sendMessage(sync.get().targets(), req);
      return Optional.of(sync.get().finished());
    } else {
      logger.warnNode(getNodeId(), "Failed to start database sync  %s", dbId);
      return Optional.empty();
    }
  }

  public void acceptSync(OSyncId syncId, OSyncMode mode) {
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    boolean accepted = ops.acceptSync(getNodeState().getNodeId(), syncId);
    OCanSyncAccept accept;
    String dbName = ops.getDatabaseTopology().getDatabaseName(syncId.getDbId());
    if (accepted && exists(dbName, null, null)) {
      logger.debugNode(getNodeId(), "Accepted sync %s sender %s ", syncId, getNodeId());

      if (mode instanceof NonBlockingBackup) {
        OStorage storage = getStorage(dbName);
        if (!storage.supportIncremental()) {
          accept = new OCanSyncAccept.BlockingSync();
        } else {
          // TODO: check with the engine
          accept = new OCanSyncAccept.NonBlockingSync();
        }
      } else if (mode instanceof Delta d) {
        var dbCtx = (OSharedContextDistributed) sharedContexts.get(dbName);

        List<OTransactionId> missing = null;
        if (dbCtx != null) {
          if (dbCtx.getTransactionSequence().missingDDL(d.status())) {
            accept = defaultFullSync();
          } else {
            missing = dbCtx.getTransactionSequence().missingTransactions(d.status());
            if (missing == null || missing.isEmpty()) {
              accept = new OCanSyncAccept.NotAccepted();
            } else {
              accept = new OCanSyncAccept.DeltaSync(d.status());
            }
          }
        } else {
          accept = new OCanSyncAccept.NotAccepted();
        }
      } else {
        // Default not instanceof...
        accept = defaultFullSync();
      }
    } else {
      accept = new OCanSyncAccept.NotAccepted();
    }

    sendMessage(syncId.getReceiver(), new OCanSync(getNodeId(), syncId, accept));
  }

  private OCanSyncAccept defaultFullSync() {
    return new OCanSyncAccept.BlockingSync();
  }

  public void canSync(ONodeId sender, OSyncId syncId, OCanSyncAccept canSync) {
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    Optional<OCanSyncResult> state = ops.canSync(sender, syncId, canSync);

    if (state.isPresent()) {
      logger.debugNode(getNodeId(), "Receiving sync %s sender %s ", syncId, sender);
      // Send a close to the other nodes in case they accepted the sync
      sendMessage(state.get().others(), new ONextBuffer(syncId, true));

      OSyncState st = state.get().state();
      sendMessage(sender, new OStartSync(syncId, canSync));

      String dbName = getDbName(syncId.getDbId());
      OReceiverInputStream input = new OReceiverInputStream(this::requestNext, st);
      st.setReceiverStream(input);
      runOnThread(() -> receiveSync(dbName, st, input, getConfigurations()));
    } else {
      logger.infoNode(
          getNodeId(),
          "Not starting sync %s from %s missing database or already syncing",
          syncId.getDbId(),
          sender);
    }
  }

  public boolean receiveSync(
      String dbName, OSyncState state, InputStream inputStream, OrientDBConfig conf) {
    boolean success = false;
    try (InputStream input = inputStream) {
      if (state.getAcceptMode() instanceof OCanSyncAccept.NonBlockingSync) {
        success = nonBlockingSync(dbName, state.getDbId(), input, conf);
      } else if (state.getAcceptMode() instanceof OCanSyncAccept.BlockingSync) {
        success = networkRestore(dbName, state.getDbId(), input);
      } else if (state.getAcceptMode() instanceof OCanSyncAccept.DeltaSync) {
        success = deltaSync(dbName, input, conf);
      }

      if (success) {
        setDatabaseState(state.getDbId(), state.getReceiver(), ODatabaseState.Online);
      } else {
        setDatabaseState(state.getDbId(), state.getReceiver(), ODatabaseState.Offline);
      }
      return success;
    } catch (IOException e) {
      logger.debugNode(getNodeId(), "Error on close of sync", e);
      return false;
    } finally {
      logger.debugNode(getNodeId(), "Completing sync %s", state.getSyncId());
      getNodeState().getOps().completeSync(state.getSyncId(), success);
    }
  }

  public void sendDatabase(OSyncId syncId, OCanSyncAccept mode) {
    logger.debugNode(getNodeId(), "Sending sync %s sender %s ", syncId, getNodeId());
    Optional<OSyncState> state =
        getNodeState().getOps().startSend(getNodeState().getNodeId(), syncId, mode);
    if (state.isPresent()) {
      var st = state.get();
      String name = getDbName(st.getDbId());
      runOnThread(() -> syncBackup(name, st, new OutputStreamMessages(this::sendBuffer, st)));
    } else {
      logger.debugNode(getNodeId(), "No sync %s present, closing ", syncId);
      sendMessage(syncId.getReceiver(), new OSyncData(syncId, new byte[] {}, 0, true));
    }
  }

  public void syncBackup(String name, OSyncState state, OutputStream output) {
    boolean success = false;
    try (OutputStream out = new BufferedOutputStream(output, 8096);
        var db = openNoAuthorization(name)) {
      var context = sharedContexts.get(name);
      OStorage storage = context.getStorage();

      if (state.getAcceptMode() instanceof OCanSyncAccept.NonBlockingSync) {
        storage.incrementalSync(out, null);
      } else if (state.getAcceptMode() instanceof OCanSyncAccept.BlockingSync) {
        int compression = getIntConfig(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION);
        storage.backup(out, null, null, null, compression, 0);
      } else if (state.getAcceptMode() instanceof OCanSyncAccept.DeltaSync d) {
        List<OTransactionId> transactions =
            context.getTransactionSequence().missingTransactions(d.status());
        storage.backupTransactions(out, transactions);
      } else {
        throw new ODatabaseException("Cannot start sync with mode " + state.getAcceptMode());
      }
      success = true;
    } catch (IOException e) {
      logger.infoNode(getNodeId(), "exception while sending backup data", e);
    } finally {
      getNodeState().getOps().completeSync(state.getSyncId(), success);
    }
  }

  private String getDbName(ODatabaseId dbId) {
    return this.getNodeState().getDatabaseTopology().getDatabaseName(dbId);
  }

  public void sendBuffer(OSyncState state, byte[] data, long sequential, boolean finished) {
    logger.debug(
        "Sending buffer %s syncId %s sender %s receiver %s finished %b",
        state.getDbId(), state.getSyncId(), state.getSender(), state.getReceiver(), finished);

    if (state.isClose()) {
      // receiver sent close, drop the data.
      return;
    }
    sendMessage(state.getReceiver(), new OSyncData(state.getSyncId(), data, sequential, finished));
    state.transaferd(data.length);
    if (!finished) {
      try {
        state.waitForNext();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void receiveSyncData(OSyncId syncId, byte[] data, long sequential, boolean finished) {
    var state = this.getNodeState().getOps().getSyncState(syncId);
    if (state.isPresent()) {
      var st = state.get();
      logger.debug(
          "Receiving buffer %s syncId %s sender %s receiver %s",
          st.getDbId(), st.getSyncId(), st.getSender(), st.getReceiver());
      st.receiveData(data, sequential, finished);
    } else {
      logger.warn("Receiving buffer syncId %s finished:%s no sync state", syncId, finished);
    }
  }

  public void requestNext(OSyncState state, boolean close) {
    sendMessage(state.getSender(), new ONextBuffer(state.getSyncId(), close));
  }

  public void nextBuffer(OSyncId syncId, boolean close) {
    this.getNodeState().getOps().requestNext(syncId, close);
  }

  public ONodeId getNodeId() {
    if (nodeState != null) {
      return nodeState.getNodeId();
    } else {
      return super.getNodeId();
    }
  }

  public void closeRemoteServer(String node) {
    closeRemoteServer(new ONodeId(node));
  }

  public void closeRemoteServer(ONodeId node) {
    if (remoteServerManager != null) {
      remoteServerManager.closeRemoteServer(node);
    }
  }

  public ORemoteServerController getRemoteServer(ONodeId nodeId) {
    if (remoteServerManager != null) {
      return remoteServerManager.getRemoteServer(nodeId);
    }
    return null;
  }

  public ORemoteServerController getRemoteServer(String rNodeName) {
    return getRemoteServer(new ONodeId(rNodeName));
  }

  public ORemoteServerController connectRemoteServer(
      ONodeId node, String url, String replicatorUser, String userPassword) throws IOException {
    if (remoteServerManager != null) {
      return remoteServerManager.connectRemoteServer(node, url, replicatorUser, userPassword);
    } else {
      logger.warn("failed to connect server manager not initialized");
    }
    return null;
  }

  public void setDatabaseStatus(ONodeId nodeId, String dbName, DB_STATUS status) {
    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    if (dbID.isPresent()) {
      setDatabaseState(dbID.get(), nodeId, ODatabaseState.from(status));
    } else {
      logger.warn("setting database status to %s, for not defined db %s", status, dbName);
    }
  }

  public void setDatabaseStatus(String dbName, DB_STATUS status) {
    var pre = getDatabaseStatus(dbName);
    if (pre != null && pre.equals(status)) {
      return;
    }
    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    if (dbID.isPresent()) {
      setDatabaseState(dbID.get(), getNodeId(), ODatabaseState.from(status));
    } else {
      logger.warn("setting database status to %s, for not defined db %s", status, dbName);
    }
  }

  public DB_STATUS getDatabaseStatus(ONodeId nodeId, String dbName) {
    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    if (dbID.isPresent()) {
      ODatabaseState status = getDatabaseState(dbID.get(), nodeId);
      if (status != null) {
        return status.toSatus();
      }
    }
    return DB_STATUS.NOT_AVAILABLE;
  }

  public DB_STATUS getDatabaseStatus(String node, String dbName) {
    return getDatabaseStatus(new ONodeId(node), dbName);
  }

  public DB_STATUS getDatabaseStatus(String dbName) {
    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    if (dbID.isPresent()) {
      ODatabaseState status = getDatabaseState(dbID.get(), getNodeId());
      if (status != null) {
        return status.toSatus();
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public OServer getServer() {
    return server;
  }

  public long getNextMessageIdCounter() {
    return localMessageIdCounter.getAndIncrement();
  }

  public Future<Boolean> installDatabase(
      String databaseName, boolean force, boolean tryWithDeltaFirst) {
    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(databaseName);
    if (id.isPresent()) {
      Optional<OTransactionSequenceStatus> deltaInfo = Optional.empty();
      if (tryWithDeltaFirst && exists(databaseName, null, null)) {
        var dbContext = this.sharedContexts.get(databaseName);
        if (dbContext != null) {
          var transactionSequence = dbContext.getTransactionSequence();
          deltaInfo = Optional.of(transactionSequence.currentStatus());
        }
      }
      var res = sync(id.get(), deltaInfo);
      if (res.isPresent()) {
        return res.get();
      } else {
        return new OCompletedFuture<>(false);
      }
    } else {
      return new OCompletedFuture<>(false);
    }
  }

  public Set<String> getAvailableNodeNotLocalNames(String name) {
    Set<String> nodes = getAvailableNodeNames(name);
    nodes.remove(getNodeName());
    return nodes;
  }

  public Set<String> getAvailableNodeNames(String name) {
    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(name);
    if (id.isPresent()) {
      return getNodeState().getDatabaseTopology().getOnlineNodes(id.get()).stream()
          .map(ONodeId::getNode)
          .collect(Collectors.toSet());
    } else {
      return getNodeState().getNetworkMembers().stream()
          .map(ONodeId::getNode)
          .collect(Collectors.toSet());
    }
  }

  public int getOnlineMasters(String databaseName) {
    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
    Optional<ODatabaseId> id = databaseTopology.getDatabaseId(databaseName);
    if (id.isPresent()) {
      return (int)
          databaseTopology.getOnlineNodes(id.get()).stream()
              .filter((x) -> databaseTopology.isMain(id.get(), x))
              .count();
    } else {
      return 0;
    }
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
    ONodeFirstConnect msg =
        new ONodeFirstConnect(getNodeState().getNodeId(), getNodeInfo(), st, false);
    this.sendMessage(nodes, msg);
  }

  public List<String> getOnlineNodesNotLocal(String dbName) {
    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(dbName);
    List<String> result;
    if (id.isPresent()) {
      result =
          getNodeState().getDatabaseTopology().getOnlineNodes(id.get()).stream()
              .map(ONodeId::getNode)
              .collect(Collectors.toList());
    } else {
      result =
          getNodeState().getNetworkMembers().stream()
              .map(ONodeId::getNode)
              .collect(Collectors.toList());
    }
    result.remove(getNodeName());
    return result;
  }

  /** Returns the nodes with the requested status. */
  public int getNodesWithStatus(
      final Collection<String> iNodes, final String databaseName, final DB_STATUS... statuses) {
    Optional<ODatabaseId> id = getNodeState().getDatabaseTopology().getDatabaseId(databaseName);
    ODatabasesTopology topology = getNodeState().getDatabaseTopology();
    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();
      ODatabaseState state = topology.getState(id.get(), new ONodeId(node));
      DB_STATUS s = state.toSatus();
      boolean matchState = false;
      for (DB_STATUS st : statuses) {
        if (s == st) matchState = true;
      }
      if (!matchState) it.remove();
    }
    return iNodes.size();
  }

  public boolean isNodeOnline(String targetNode, String databaseName) {
    return DB_STATUS.ONLINE.equals(getDatabaseStatus(targetNode, databaseName));
  }

  public boolean isNodeAvailable(String targetNode, String databaseName) {
    final ODistributedServerManager.DB_STATUS s = getDatabaseStatus(targetNode, databaseName);
    return s != ODistributedServerManager.DB_STATUS.OFFLINE
        && s != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  public void sendMergeOperation(ONodeId requestToMerge, OCompleteExecution execution) {
    ONodeState ns = getNodeState();
    ONodeFirstConnect msg =
        new ONodeFirstConnect(ns.getNodeId(), getNodeInfo(), ns.getNetworkState(), true);
    sendMessage(requestToMerge, msg);
  }

  public void autoDeployIfNeed() {
    Set<ONodeId> members = getNodeState().getNetworkMembers();
    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
    Collection<ODatabaseId> dbs = databaseTopology.getDatabases();
    for (ODatabaseId id : dbs) {
      // TODO: check autodeploy setting
      List<OAddNodeInfo> nodes = new ArrayList<>();
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

  public void sendMergeNodeAction(
      ONodeId node, ONodeStateNetwork state, OCompleteExecution execution) {
    // This should do a two phase operation in the current network, but also ask for
    // permission to the merging node, to avoid in a two network and a node case to make
    // the node join both networks.
    OCoordinatedDistributedOps ops = getNodeState().getOps();
    if (ops.getNetworkTopology().getMembers().contains(node)) {
      execution.complete(Optional.empty());
      return;
    }
    var mergedState = ops.createMergedState(state);
    if (mergedState.isPresent()) {
      var newState = mergedState.get();
      var operation = new OMergeNode(node, newState, ops.getNetworkState());
      OCompleteAction action =
          new OMergeCompleteAction(this, operation, newState, state, execution, node);
      logger.debugNode(getNodeId(), "starting operation %s", operation);
      sendMergeOperationMessages(node, newState, state, operation, action);
    } else {
      logger.debugNode(getNodeId(), "incompatible network cannot merge with node %s", node);
    }
  }

  public void retryMergeOperationMessages(
      ONodeId mergeNode,
      ONodeStateNetwork mergedState,
      ONodeStateNetwork original,
      OOperationMessage operation,
      OCompleteAction action,
      int delay) {
    delayExecute(
        () -> sendMergeOperationMessages(mergeNode, mergedState, original, operation, action),
        delay);
  }

  private void sendMergeOperationMessages(
      ONodeId mergeNode,
      ONodeStateNetwork mergedState,
      ONodeStateNetwork original,
      OOperationMessage operation,
      OCompleteAction action) {
    OCoordinatedDistributedOps ops = this.getNodeState().getOps();
    var startOp = ops.start(action);
    if (startOp.isPresent()) {
      var start = startOp.get();
      OProposeOp propose = new OProposeOp(start.promise(), operation);
      sendMessage(start.nodes(), propose);
      OMergeRequest op =
          new OMergeRequest(start.promise(), ops.getGroupId(), mergedState, original);
      sendMessage(mergeNode, op);
    } else {
      action.complete(null, null, Optional.of(new ONoTransactionSequencialAvailable()));
    }
  }

  public void validateMergeToNetwork(
      OGroupId group,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    var accepted = getNodeState().getOps().validateMergeToNetwork(group, state, original, promise);
    sendMessage(promise.getCoordinator(), new OMergeResult(getNodeId(), promise, accepted));
  }

  public void cancelMergeToNetwork(OTransactionIdPromise promise) {
    getNodeState().getOps().cancelMergeToNetwork(promise);
  }

  public void mergeToNetwork(OTransactionIdPromise promise) {
    getNodeState().getOps().mergeToNetwork(promise);
    dumpNodeInfo();
  }

  public void mergeNodeResult(
      ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted) {
    getNodeState().getOps().nodeMergeResult(node, promise, accepted);
  }

  public void disconnected(ONodeId node) {
    notifyLegacyNodeLeftListener(node);
    var action = getNodeState().getOps().nodeDisconnected(node);
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
    var members = getNodeState().getOps().getNetworkTopology().getMembers();
    sendMessage(
        members,
        new OTopologyPing(getNodeId(), getNodeState().getOps().getTransactionSequenceState()));
  }

  public void receivePing(ONodeId nodeId, OTransactionSequenceStatus status) {
    var transactions = getNodeState().getOps().receivePing(nodeId, status);
    if (!transactions.isEmpty()) {
      requestTransactions(nodeId, transactions);
    }
  }

  private void requestTransactions(ONodeId nodeId, List<OTransactionId> transactions) {
    sendMessage(nodeId, new OSendTransactions(getNodeId(), transactions));
  }

  private void checkDisconnectedNodes() {
    var time = getLongConfig(OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT);
    var offlineNodes = getNodeState().getOps().checkOffline(time);
    for (var offlineNode : offlineNodes) {
      disconnected(offlineNode);
    }
  }

  private void checkOperationsTimeout() {
    var timeOutSync = getLongConfig(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT);
    getNodeState().getOps().checkTimeoutSync(timeOutSync);
  }

  public void checkNodeIsMaster(ONodeId localNodeId, String name, String operation) {
    Optional<ODatabaseId> dbID = getNodeState().getDatabaseTopology().getDatabaseId(name);
    if (dbID.isPresent()) {

      ONodeRole role = getNodeState().getDatabaseTopology().getRole(dbID.get(), localNodeId);
      if (role != ONodeRole.Main)
        throw new OWriteOperationNotPermittedException(
            "Cannot execute write operation ("
                + operation
                + ") on node '"
                + localNodeId
                + "' because is non a master");
    } else {
      throw new OWriteOperationNotPermittedException(
          "Cannot execute write operation ("
              + operation
              + ") on node '"
              + localNodeId
              + "' because is non a master");
    }
  }

  public boolean isNodeMaster(String node, String databaseName) {
    ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
    Optional<ODatabaseId> id = databaseTopology.getDatabaseId(databaseName);
    if (id.isPresent()) {
      return databaseTopology.isMain(id.get(), new ONodeId(node));
    } else {
      return false;
    }
  }

  public boolean removeDatabaseMember(ODatabaseId databaseId, ONodeId node) {
    Future<Optional<OAcceptResult>> future =
        retryOperation(new ORemoveMemberRetryOperation(databaseId, List.of(node)));

    try {
      return future.get(10, TimeUnit.MINUTES).isEmpty();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return false;
    }
  }

  public void distributedDrop(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    String dbName = getDbName(dbId);
    if (dbName != null) {
      this.internalDrop(dbName);
    }
    this.getNodeState().getOps().dropDatabase(dbId, version, promise);
  }

  @Override
  public boolean waitOnline(String database) throws InterruptedException {
    long waitTime = getLongConfig(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);
    return getNodeState().getOps().waitSelfOnline(database, Optional.of(waitTime));
  }

  public void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    getNodeState().getOps().mergeNode(node, state, original, promise);
    syncIfNeededAll();
    dumpNodeInfo();
  }

  public Future<Optional<OAcceptResult>> setDatabaseNodeRole(
      ODatabaseId databaseId, String serverName, String role) {
    var r = ONodeRole.fromString(role);
    return retryOperation(
        new OSetDatabaseNodeRoleRetryOperation(databaseId, new ONodeId(serverName), r));
  }

  public Future<Optional<OAcceptResult>> setDatabaseQuorum(ODatabaseId databaseId, int newQuorum) {
    return retryOperation(new OSetDatabaseQuorumRetryOperation(databaseId, newQuorum));
  }

  @Override
  public void gracefulWaitFullStartup() throws InterruptedException {
    long waitTime = getLongConfig(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);
    if (nodeState != null) {
      this.nodeState.getOps().waitForEnstablish(Optional.of(waitTime));
    }
  }

  @Override
  public boolean isDistributedOnline() {
    if (isOpen() && getNodeState() != null) {
      return getNodeState().getOps().getNetworkTopology().isSelfEnstablished();
    } else {
      return false;
    }
  }

  public ONodeInfo getNodeInfo() {
    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;
    List<ONodeInfoListener> listeners = new ArrayList<>();
    if (server != null) {
      for (OServerNetworkListener listener : server.getNetworkListeners()) {
        listeners.add(
            new ONodeInfoListener(
                listener.getProtocolType().getSimpleName(), listener.getListeningAddress(true)));
      }
    }

    return new ONodeInfo(OConstants.getRawVersion(), listeners, usedMem, freeMem, maxMem);
  }

  public OClusterConfiguration getClusterConfiguration() {
    OClusterConfiguration cc = new OClusterConfiguration();
    cc.setLocalName(getNodeName());
    cc.setLocalId(getSystemDatabase().getServerId());

    var networkTopology = getNodeState().getOps().getNetworkTopology();
    var databaseTopology = getNodeState().getOps().getDatabaseTopology();
    // INSERT MEMBERS
    for (var member : networkTopology.getMembers()) {
      ONodeConfig nodeConfig = getNodeConfiguration(member);
      final String nodeName = member.getNode();
      final Map<String, String> dbStatus = new HashMap<>();
      for (var db : databaseTopology.getDatabases()) {
        var dbName = databaseTopology.getDatabaseName(db);
        final DB_STATUS nodeDbState = getDatabaseStatus(nodeName, dbName);
        dbStatus.put(dbName, nodeDbState.toString());
      }
      nodeConfig.setDatabasesStatus(dbStatus);
      cc.addMember(nodeConfig);
    }

    return cc;
  }

  private ONodeConfig getNodeConfiguration(ONodeId member) {
    // TODO: collect more informations
    ONodeConfig nodeCfg = new ONodeConfig();
    nodeCfg.setName(member.getNode());
    //    nodeCfg.setUuid();

    if (getNodeState() != null) {
      ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      var dbs = dbIds.stream().map(databaseTopology::getDatabaseName).collect(Collectors.toSet());
      nodeCfg.setDatabases(dbs);
    }

    List<ONodeListenerConfig> listeners = new ArrayList<>();
    for (var listener : remoteServerManager.getRemoteAddresses(member)) {
      listeners.add(new ONodeListenerConfig("binary", listener.address()));
    }
    nodeCfg.setListeners(listeners);

    if (getNodeState() != null) {
      ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      var dbs =
          dbIds.stream()
              .filter(d -> databaseTopology.getState(d, member) != ODatabaseState.NotAvailable)
              .map(databaseTopology::getDatabaseName)
              .collect(Collectors.toSet());
      nodeCfg.setDatabases(dbs);

      OStatsManager stats = getNodeState().getStats();
      var nodeStats = stats.getStats(member);
      if (nodeStats.isPresent()) {
        var ns = nodeStats.get();
        nodeCfg.setStartedOn(new Date(ns.getBootTime()));
        nodeCfg.setUsedMemory(ns.getUsedMem());
        nodeCfg.setFreeMemory(ns.getFreeMem());
        nodeCfg.setMaxMemory(ns.getMaxMem());

        nodeCfg.setLatencies(ns.getNodesLatencies());
        nodeCfg.setMessages(ns.getNodesMessages());
      }
      // TODO: handle cpu for third party, not shared and optional

    }

    return nodeCfg;
  }

  // This will probably disappear soon
  public static final String REPLICATOR_USER = "_CrossServerTempUser";

  public ONodeConfig getLocalNodeConfiguration() {
    ONodeConfig nodeCfg = new ONodeConfig();
    nodeCfg.setUuid(getSystemDatabase().getServerId());
    nodeCfg.setName(nodeName);
    nodeCfg.setVersion(OConstants.getRawVersion());
    //    if(plugin != null) {
    //      nodeCfg.setPublicAddress(plugin.getPublicAddress());
    //    }
    nodeCfg.setStartedOn(new Date(bootTime));
    nodeCfg.setStatus(getNodeState().getOps().getNetworkTopology().getState().toString());
    nodeCfg.setConnections(server.getClientConnectionManager().getTotal());

    List<ONodeListenerConfig> listeners = new ArrayList<>();
    for (OServerNetworkListener listener : server.getNetworkListeners()) {
      listeners.add(
          new ONodeListenerConfig(
              listener.getProtocolType().getSimpleName(), listener.getListeningAddress(true)));
    }
    nodeCfg.setListeners(listeners);

    // STORE THE TEMP USER/PASSWD USED FOR REPLICATION
    final OSecurityUser user = getSecuritySystem().getUser(REPLICATOR_USER);
    if (user != null)
      nodeCfg.setReplicator(getSecuritySystem().getUser(REPLICATOR_USER).getPassword());

    if (getNodeState() != null) {
      ODatabasesTopology databaseTopology = getNodeState().getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      var dbs =
          dbIds.stream()
              .filter(d -> databaseTopology.getState(d, getNodeId()) != ODatabaseState.NotAvailable)
              .map(databaseTopology::getDatabaseName)
              .collect(Collectors.toSet());
      nodeCfg.setDatabases(dbs);
    }

    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;

    nodeCfg.setUsedMemory(usedMem);
    nodeCfg.setFreeMemory(freeMem);
    nodeCfg.setMaxMemory(maxMem);

    nodeCfg.setLatencies(getMessageService().getNodesLatencies());
    nodeCfg.setMessages(getMessageService().getNodesMessages());

    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners();
        it.hasNext(); ) {
      final ODatabaseLifecycleListener listener = it.next();
      if (listener != null) listener.onLocalNodeConfigurationRequest(nodeCfg);
    }

    return nodeCfg;
  }

  @Override
  public ONetworkMessage newNetworkMessageRequest() {
    return new ONetworkRequestMessage(this);
  }

  @Override
  public ONetworkMessage newNetworkMessageResponse() {
    return new ONetworkResponseMessage(this);
  }

  public void validateDatabaseStatus(String databaseName, OTransactionSequenceStatus status) {
    var context = getSharedDatabaseContext(databaseName);
    if (context.isPresent()) {
      List<OTransactionId> res = context.get().getTransactionSequence().checkSelfStatus(status);
      ((OSharedContextDistributed) context.get()).getDistributedContext().removeRunning(res);
      if (!res.isEmpty()) {
        execute(() -> installDatabase(databaseName, true, true));
      }
    }
  }

  public void sendTopologyTransactions(ONodeId nodeId, List<OTransactionId> transactions) {
    var messages = this.getNodeState().recover(transactions);
    var ops =
        messages.stream().map(m -> new OConfirmedRetryOp(m.getPromiseId(), m.getOp())).toList();
    sendMessage(nodeId, new OConfirmedOps(ops));
  }

  public void receiveRecovery(List<OConfirmedRetryOp> ops) {
    this.getNodeState().recover(ops, this);
  }
}
