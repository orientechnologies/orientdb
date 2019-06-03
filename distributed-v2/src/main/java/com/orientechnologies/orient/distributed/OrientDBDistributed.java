package com.orientechnologies.orient.distributed;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.distributed.impl.ODistributedNetworkManager;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.distributed.impl.structural.*;
import com.orientechnologies.orient.distributed.impl.structural.operations.OCreateDatabaseSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.operations.OCreateDatabaseSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.operations.ODropDatabaseSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.*;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by tglman on 08/08/17.
 */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware, OServerLifecycleListener {

  private static final String DISTRIBUTED_USER = "distributed_replication";

  private          OServer                                   server;
  private volatile boolean                                   coordinator      = false;
  private volatile ONodeIdentity                             coordinatorIdentity;
  private          OStructuralDistributedContext             structuralDistributedContext;
  private          ODistributedNetworkManager                networkManager;
  private volatile boolean                                   distributedReady = false;
  private final    ConcurrentMap<String, ODistributedStatus> databasesStatus  = new ConcurrentHashMap<>();
  private          ONodeConfiguration                        nodeConfiguration;
  private          OStructuralConfiguration                  structuralConfiguration;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);

    this.nodeConfiguration = config.getNodeConfiguration();
  }

  public ONodeConfiguration getNodeConfig() {
    return nodeConfiguration;
  }

  private ONodeIdentity getNodeIdentity() {
    return structuralConfiguration.getCurrentNodeIdentity();
  }

  @Override
  public void init(OServer server) {
    //Cannot get the plugin from here, is too early, doing it lazy  
    this.server = server;
    this.server.registerLifecycleListener(this);
  }

  @Override
  public void onAfterActivate() {
    structuralConfiguration = new OStructuralConfiguration(this.getServer().getSystemDatabase(), this);
    checkPort();
    structuralDistributedContext = new OStructuralDistributedContext(this);
    networkManager = new ODistributedNetworkManager(this, getNodeConfig(), generateInternalConfiguration());
    networkManager.startup();

  }

  public void checkPort() {
    // Use the inbound port in case it's not provided
    if (this.nodeConfiguration.getTcpPort() == null) {
      OServerNetworkListener protocol = server.getListenerByProtocol(ONetworkProtocolBinary.class);
      this.nodeConfiguration.setTcpPort(protocol.getInboundAddr().getPort());
    }
  }

  private ONodeInternalConfiguration generateInternalConfiguration() {
    String userName = getNodeIdentity().getName() + DISTRIBUTED_USER;
    OServerUserConfiguration user = server.getUser(userName);
    if (user == null) {
      server.addTemporaryUser(userName, "" + new SecureRandom().nextLong(), "*");
      user = server.getUser(userName);
    }
    OLogId lastLogId = structuralDistributedContext.getOpLog().lastPersistentLog();

    return new ONodeInternalConfiguration(lastLogId, getNodeIdentity(), userName, user.password);
  }

  @Override
  public void onBeforeDeactivate() {
    networkManager.shutdown();
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbedded(storage);
    }
    return new ODatabaseDocumentDistributed(storage, getNodeIdentity());
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(ODatabasePoolInternal pool, OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbeddedPooled(pool, storage);
    }
    return new ODatabaseDocumentDistributedPooled(pool, storage, getNodeIdentity());

  }

  public OStorage fullSync(String dbName, String backupPath, OrientDBConfig config) {
    final ODatabaseDocumentEmbedded embedded;
    OAbstractPaginatedStorage storage = null;
    synchronized (this) {

      try {
        storage = storages.get(dbName);

        if (storage != null) {
          OCommandCacheSoftRefs.clearFiles(storage);
          OSharedContext context = sharedContexts.remove(dbName);
          context.close();
          storage.delete();
          storages.remove(dbName);
        }
        storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(dbName), new HashMap<>(), maxWALSegmentSize);
        embedded = internalCreate(config, storage);
        storages.put(dbName, storage);
      } catch (Exception e) {
        if (storage != null) {
          storage.delete();
        }

        throw OException.wrapException(new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
      }
    }
    storage.restoreFromIncrementalBackup(backupPath);
    //DROP AND CREATE THE SHARED CONTEXT SU HAS CORRECT INFORMATION.
    synchronized (this) {
      OSharedContext context = sharedContexts.remove(dbName);
      context.close();
    }
    ODatabaseDocumentEmbedded instance = openNoAuthorization(dbName);
    instance.close();
    checkCoordinator(dbName);
    return storage;
  }

  @Override
  public void restore(String name, InputStream in, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener) {
    super.restore(name, in, options, callable, iListener);
    //THIS MAKE SURE THAT THE SHARED CONTEXT IS INITED.
    ODatabaseDocumentEmbedded instance = openNoAuthorization(name);
    instance.close();
    checkCoordinator(name);
  }

  @Override
  public void restore(String name, String user, String password, ODatabaseType type, String path, OrientDBConfig config) {
    super.restore(name, user, password, type, path, config);
    //THIS MAKE SURE THAT THE SHARED CONTEXT IS INITED.
    ODatabaseDocumentEmbedded instance = openNoAuthorization(name);
    instance.close();
    checkCoordinator(name);
  }

  @Override
  public void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(name)) {
      super.create(name, user, password, type, config);
      return;
    }
    checkReadyForHandleRequests();
    //TODO:RESOLVE CONFIGURATION PARAMETERS
    Future<OStructuralSubmitResponse> created = structuralDistributedContext.getSubmitContext()
        .send(new OSessionOperationId(), new OCreateDatabaseSubmitRequest(name, type.name(), new HashMap<>()));
    try {
      OCreateDatabaseSubmitResponse response = (OCreateDatabaseSubmitResponse) created.get();
      if (!response.isSuccess()) {
        throw new ODatabaseException(response.getError());
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      return;
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    //This initialize the distributed configuration.
    checkCoordinator(name);
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthenticate(String name, String user) {
    ODatabaseDocumentEmbedded session = super.openNoAuthenticate(name, user);
    checkCoordinator(name);
    return session;
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    ODatabaseDocumentEmbedded session = super.openNoAuthorization(name);
    checkCoordinator(name);
    return session;

  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    checkDatabaseReady(name);
    ODatabaseDocumentInternal session = super.open(name, user, password, config);
    checkCoordinator(name);
    return session;
  }

  @Override
  public ODatabaseDocumentInternal poolOpen(String name, String user, String password, ODatabasePoolInternal pool) {
    checkDatabaseReady(name);
    ODatabaseDocumentInternal session = super.poolOpen(name, user, password, pool);
    checkCoordinator(name);
    return session;

  }

  private synchronized void checkCoordinator(String database) {
    if (!database.equals(OSystemDatabase.SYSTEM_DB_NAME)) {
      OSharedContext shared = sharedContexts.get(database);
      if (shared instanceof OSharedContextDistributed) {
        ODistributedContext distributed = ((OSharedContextDistributed) shared).getDistributedContext();
        if (distributed.getCoordinator() == null) {
          if (coordinator) {
            distributed.makeCoordinator(getNodeIdentity(), shared);
            for (ONodeIdentity node : networkManager.getRemoteServers()) {
              ODistributedMember member = new ODistributedMember(node, database, networkManager.getChannel(node));
              distributed.getCoordinator().join(member);
            }
          } else {
            ODistributedMember member = new ODistributedMember(coordinatorIdentity, database,
                networkManager.getChannel(coordinatorIdentity));
            distributed.setExternalCoordinator(member);
          }
        }

        for (ONodeIdentity node : networkManager.getRemoteServers()) {
          ODistributedMember member = new ODistributedMember(node, database, networkManager.getChannel(node));
          distributed.getExecutor().join(member);
        }
      }
    }

  }

  public synchronized void nodeConnected(ONodeIdentity nodeIdentity, ODistributedChannel channel) {
    if (this.getNodeIdentity().equals(nodeIdentity))
      return;
    for (OSharedContext context : sharedContexts.values()) {
      if (isContextToIgnore(context))
        continue;
      ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
      ODistributedMember member = new ODistributedMember(nodeIdentity, context.getStorage().getName(), channel);
      distributed.getExecutor().join(member);
      if (coordinator) {
        ODistributedCoordinator c = distributed.getCoordinator();
        if (c == null) {
          distributed.makeCoordinator(getNodeIdentity(), context);
          c = distributed.getCoordinator();
        }
        c.join(member);
      }
    }
    if (coordinator && structuralDistributedContext.getLeader() != null) {
      structuralDistributedContext.getLeader().connected(nodeIdentity, channel);
      structuralDistributedContext.getLeader().join(nodeIdentity);
    }
  }

  public synchronized void nodeDisconnected(ONodeIdentity nodeIdentity) {
    if (this.getNodeIdentity().equals(nodeIdentity))
      return;
    if (coordinator) {
      structuralDistributedContext.getLeader().disconnected(nodeIdentity);
    }
    for (OSharedContext context : sharedContexts.values()) {
      if (isContextToIgnore(context))
        continue;
      ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
      distributed.getExecutor().leave(distributed.getExecutor().getMember(nodeIdentity));
      if (coordinator) {
        ODistributedCoordinator c = distributed.getCoordinator();
        if (c == null) {
          c.leave(c.getMember(nodeIdentity));
        }

      }
    }
  }

  private boolean isContextToIgnore(OSharedContext context) {
    return context.getStorage().getName().equals(OSystemDatabase.SYSTEM_DB_NAME) || context.getStorage().isClosed();
  }

  public synchronized void setLeader(ONodeIdentity coordinatorIdentity, OLogId lastValid) {
    this.coordinatorIdentity = coordinatorIdentity;
    if (getNodeIdentity().equals(coordinatorIdentity)) {
      if (!this.coordinator) {
        for (OSharedContext context : sharedContexts.values()) {
          if (isContextToIgnore(context))
            continue;
          ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
          distributed.makeCoordinator(coordinatorIdentity, context);
          for (ONodeIdentity node : networkManager.getRemoteServers()) {
            ODistributedMember member = new ODistributedMember(node, context.getStorage().getName(),
                networkManager.getChannel(node));
            distributed.getCoordinator().join(member);
          }
        }
        structuralDistributedContext.makeLeader(coordinatorIdentity);

        for (ONodeIdentity node : networkManager.getRemoteServers()) {
          structuralDistributedContext.getLeader().connected(node, networkManager.getChannel(node));
        }
        structuralDistributedContext.getLeader().join(getNodeIdentity());
        for (ONodeIdentity node : networkManager.getRemoteServers()) {
          structuralDistributedContext.getLeader().join(node);
        }
        this.coordinator = true;
      }
    } else {
      structuralDistributedContext.setExternalLeader(networkManager.getChannel(coordinatorIdentity));
      realignToLog(lastValid);
      for (OSharedContext context : sharedContexts.values()) {
        if (isContextToIgnore(context))
          continue;
        ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
        ODistributedMember member = new ODistributedMember(coordinatorIdentity, context.getStorage().getName(),
            networkManager.getChannel(coordinatorIdentity));

        distributed.setExternalCoordinator(member);
      }
      this.coordinator = false;
    }
    setOnline();
  }

  private void realignToLog(OLogId lastValid) {
    OLogId lastStateId = this.structuralConfiguration.getLastUpdateId();
    OOperationLog opLog = this.structuralDistributedContext.getOpLog();
    OLogId lastPersistent = opLog.lastPersistentLog();
    if (lastPersistent != null && lastValid != null) {
      Iterator<OOperationLogEntry> list = opLog.iterate(lastStateId, lastValid);
      while (list.hasNext()) {
        OOperationLogEntry change = list.next();
        this.getStructuralDistributedContext().getFollower().recover((ORaftOperation) change.getRequest());
      }
      int isCoordinatorLastMoreRecent = lastValid.compareTo(lastPersistent);
      if (isCoordinatorLastMoreRecent > 0) {
        nodeSyncRequest(lastPersistent);
      } else if (isCoordinatorLastMoreRecent < 0) {
        //Remove from the log the staff i've after the master id, this should not have been applied to the state yet.
        //TODO: double check if state has an id more recent, this should not happen just add an assert.
        opLog.removeAfter(lastValid);
      }
    } else if (lastValid != null) {
      nodeFirstJoin();
    } else if (lastPersistent != null) {
      //TODO:  I've something that the master has not, rely on what the master tell me.
    } else {
      //TODO: First join for everyone, do nothing for a while ? and then sync the databases existing on the disc on nodes.
    }

  }

  private void nodeFirstJoin() {
    this.getStructuralDistributedContext().getSubmitContext().send(new OSessionOperationId(), new SyncRequest(Optional.empty()));
  }

  private void nodeSyncRequest(OLogId logId) {
    this.getStructuralDistributedContext().getSubmitContext().send(new OSessionOperationId(), new SyncRequest(Optional.of(logId)));
  }

  private synchronized void syncDatabase(OStructuralNodeDatabase configuration) {

  }

  private synchronized void parkDatabase(String database) {
    forceDatabaseClose(database);
    //TODO: Move The  database folder somewhere else, backup folder?
  }

  private void triggerParkDatabase(String databaseName) {
    //TODO: This should make sure that two park do not happen concurrently on the same database
    executeNoDb(() -> {
      parkDatabase(databaseName);
      return null;
    });
  }

  private void triggerSyncDatabase(OStructuralNodeDatabase db) {
    //TODO: This should make sure that two sync do not happen concurrently on the same database
    executeNoDb(() -> {
      syncDatabase(db);
      return null;
    });
  }

  public synchronized void syncToConfiguration(OLogId lastId, OReadStructuralSharedConfiguration sharedConfiguration) {
    getStructuralConfiguration().receiveSharedConfiguration(lastId, sharedConfiguration);
    OStructuralNodeConfiguration nodeConfig = getStructuralConfiguration().readSharedConfiguration().getNode(getNodeIdentity());
    assert nodeConfig != null : "if arrived here the configuration should have this node configured";
    super.loadAllDatabases();
    Collection<OStorage> storages = super.getStorages();
    for (OStorage st : storages) {
      if (st instanceof OAbstractPaginatedStorage) {
        OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) st;
        OStructuralNodeDatabase db = nodeConfig.getDatabase(storage.getUuid());
        if (db != null) {
          triggerSyncDatabase(db);
        } else {
          triggerParkDatabase(st.getName());
        }
      }
    }
    Set<UUID> existingIds = storages.stream().map((s) -> ((OAbstractPaginatedStorage) s).getUuid()).collect(Collectors.toSet());
    for (OStructuralNodeDatabase db : nodeConfig.getDatabases()) {
      if (!existingIds.contains(db.getUuid())) {
        triggerSyncDatabase(db);
      }
    }
  }

  public synchronized ODistributedContext getDistributedContext(String database) {
    OSharedContext shared = sharedContexts.get(database);
    if (shared != null) {
      return ((OSharedContextDistributed) shared).getDistributedContext();
    }
    return null;
  }

  public OStructuralDistributedContext getStructuralDistributedContext() {
    return structuralDistributedContext;
  }

  @Override
  public void drop(String name, String user, String password) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(name)) {
      super.drop(name, user, password);
      return;
    }
    checkReadyForHandleRequests();
    //TODO:RESOLVE CONFIGURATION PARAMETERS
    Future<OStructuralSubmitResponse> created = structuralDistributedContext.getSubmitContext()
        .send(new OSessionOperationId(), new ODropDatabaseSubmitRequest(name));
    try {
      created.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void coordinatedRequest(OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    networkManager.coordinatedRequest(connection, requestType, clientTxId, channel);
  }

  public synchronized void internalCreateDatabase(OSessionOperationId operationId, String database, String type,
      Map<String, String> configurations) {
    //TODO:INIT CONFIG
    super.create(database, null, null, ODatabaseType.valueOf(type), null);
    OStructuralSharedConfiguration config = getStructuralConfiguration().modifySharedConfiguration();
    config.addDatabase(database);
    getStructuralConfiguration().update(config);
    this.databasesStatus.put(database, ODistributedStatus.ONLINE);
    checkCoordinator(database);
    //TODO: double check this notify, it may unblock as well checkReadyForHandleRequests that is not what is expected
    this.notifyAll();
  }

  public void internalDropDatabase(String database) {
    super.drop(database, null, null);
    OStructuralSharedConfiguration config = getStructuralConfiguration().modifySharedConfiguration();
    config.removeDatabase(database);
    getStructuralConfiguration().update(config);
  }

  public synchronized void checkReadyForHandleRequests() {
    try {
      if (!distributedReady) {
        this.wait(MINUTES.toMillis(1));
        if (!distributedReady) {
          throw new ODatabaseException("Server Not Yet Online");
        }
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("Interrupted while waiting to start"), e);
    }
  }

  public synchronized void setOnline() {
    this.distributedReady = true;
    this.notifyAll();
  }

  public synchronized void checkDatabaseReady(String database) {
    checkReadyForHandleRequests();
    try {
      if (!ODistributedStatus.ONLINE.equals(databasesStatus.get(database))) {
        this.wait(MINUTES.toMillis(1));
        if (!distributedReady) {
          throw new ODatabaseException("Server Not Yet Online");
        }
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("Interrupted while waiting to start"), e);
    }
  }

  @Override
  public void close() {
    if (networkManager != null) {
      this.networkManager.shutdown();
    }
    super.close();
  }

  public OServer getServer() {
    return server;
  }

  public OStructuralConfiguration getStructuralConfiguration() {
    return structuralConfiguration;
  }

  public ODistributedNetworkManager getNetworkManager() {
    return networkManager;
  }
}
