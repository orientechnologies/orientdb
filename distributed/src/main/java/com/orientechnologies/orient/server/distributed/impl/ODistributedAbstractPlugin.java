/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.conflict.ODistributedConflictResolverFactory;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.sql.OCommandExecutorSQLHASyncCluster;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODatabaseIsOldException;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME;

/**
 * Abstract plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public abstract class ODistributedAbstractPlugin extends OServerPluginAbstract
    implements ODistributedServerManager, ODatabaseLifecycleListener, OCommandOutputListener {
  public static final String REPLICATOR_USER = "_CrossServerTempUser";

  protected static final String PAR_DEF_DISTRIB_DB_CONFIG = "configuration.db.default";
  protected static final String NODE_NAME_ENV             = "ORIENTDB_NODE_NAME";

  protected OServer serverInstance;
  protected boolean enabled = true;
  protected String nodeUuid;
  protected String nodeName = null;
  protected int    nodeId   = -1;
  protected File defaultDatabaseConfigFile;
  protected          ConcurrentHashMap<String, ODistributedStorage> storages = new ConcurrentHashMap<String, ODistributedStorage>();
  protected volatile NODE_STATUS                                    status   = NODE_STATUS.OFFLINE;
  protected long lastClusterChangeOn;
  protected       List<ODistributedLifecycleListener>            listeners                         = new ArrayList<ODistributedLifecycleListener>();
  protected final ConcurrentMap<String, ORemoteServerController> remoteServers                     = new ConcurrentHashMap<String, ORemoteServerController>();
  protected       TimerTask                                      publishLocalNodeConfigurationTask = null;
  protected       OClusterHealthChecker                          healthCheckerTask                 = null;
  protected String coordinatorServer;

  // LOCAL MSG COUNTER
  protected AtomicLong                          localMessageIdCounter     = new AtomicLong();
  protected OClusterOwnershipAssignmentStrategy clusterAssignmentStrategy = new ODefaultClusterOwnershipAssignmentStrategy(this);

  protected static final int                  DEPLOY_DB_MAX_RETRIES  = 10;
  protected              Map<String, Member>  activeNodes            = new ConcurrentHashMap<String, Member>();
  protected              Map<String, String>  activeNodesNamesByUuid = new ConcurrentHashMap<String, String>();
  protected              Map<String, String>  activeNodesUuidByName  = new ConcurrentHashMap<String, String>();
  protected final        List<String>         registeredNodeById     = new ArrayList<String>();
  protected final        Map<String, Integer> registeredNodeByName   = new HashMap<String, Integer>();
  protected              Map<String, Long>    autoRemovalOfServers   = new ConcurrentHashMap<String, Long>();
  protected volatile ODistributedMessageServiceImpl messageService;
  protected Date                 startedOn              = new Date();
  protected ORemoteTaskFactory   taskFactory            = new ODefaultRemoteTaskFactory();
  protected ODistributedStrategy responseManagerFactory = new ODefaultDistributedStrategy();

  private volatile String                              lastServerDump          = "";
  protected        CountDownLatch                      serverStarted           = new CountDownLatch(1);
  private          ODistributedConflictResolverFactory conflictResolverFactory = new ODistributedConflictResolverFactory();
  private final    ODistributedLockManagerRequester    lockManagerRequester    = new ODistributedLockManagerRequester(this);
  private final    ODistributedLockManagerExecutor     lockManagerExecutor     = new ODistributedLockManagerExecutor(this);

  protected ODistributedAbstractPlugin() {
  }

  public void waitUntilNodeOnline() throws InterruptedException {
    serverStarted.await();
  }

  public void waitUntilNodeOnline(final String nodeName, final String databaseName) throws InterruptedException {
    while (messageService == null || messageService.getDatabase(databaseName) == null || !isNodeOnline(nodeName, databaseName))
      Thread.sleep(100);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    serverInstance = oServer;
    oServer.setVariable("ODistributedAbstractPlugin", this);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(OSystemVariableResolver.resolveSystemVariables(param.value))) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("nodeName")) {
        nodeName = param.value;
        if (nodeName.contains("."))
          throw new OConfigurationException("Illegal node name '" + nodeName + "'. '.' is not allowed in node name");
      } else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        setDefaultDatabaseConfigFile(param.value);
      }
    }

    if (serverInstance.getUser("replicator") == null)
      // DROP THE REPLICATOR USER. THIS USER WAS NEEDED BEFORE 2.2, BUT IT'S NOT REQUIRED ANYMORE
      OLogManager.instance().config(this,
          "Found 'replicator' user. Starting from OrientDB v2.2 this internal user is no needed anymore. Removing it...");
    try {
      serverInstance.dropUser("replicator");
    } catch (IOException e) {
      throw OException.wrapException(new OConfigurationException("Error on deleting 'replicator' user"), e);
    }
  }

  @Override
  public synchronized String getCoordinatorServer() {
    return this.coordinatorServer;
  }

  public File getDefaultDatabaseConfigFile() {
    return defaultDatabaseConfigFile;
  }

  public void setDefaultDatabaseConfigFile(final String iFile) {
    defaultDatabaseConfigFile = new File(OSystemVariableResolver.resolveSystemVariables(iFile));
    if (!defaultDatabaseConfigFile.exists())
      throw new OConfigurationException("Cannot find distributed database config file: " + defaultDatabaseConfigFile);
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    Orient.instance().addDbLifecycleListener(this);
  }

  @Override
  public ODistributedAbstractPlugin registerLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.add(iListener);
    return this;
  }

  @Override
  public ODistributedAbstractPlugin unregisterLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.remove(iListener);
    return this;
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    if (publishLocalNodeConfigurationTask != null)
      publishLocalNodeConfigurationTask.cancel();

    if (healthCheckerTask != null)
      healthCheckerTask.cancel();

    if (messageService != null)
      messageService.shutdown();

    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    setNodeStatus(NODE_STATUS.OFFLINE);

    Orient.instance().removeDbLifecycleListener(this);

    // CLOSE AND FREE ALL THE STORAGES
    for (ODistributedStorage s : storages.values())
      try {
        s.shutdownAsynchronousWorker();
        s.close();
      } catch (Exception e) {
      }
    storages.clear();
  }

  @Override
  public ODistributedLockManagerRequester getLockManagerRequester() {
    return lockManagerRequester;
  }

  @Override
  public ODistributedLockManagerExecutor getLockManagerExecutor() {
    return lockManagerExecutor;
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    if (status != NODE_STATUS.ONLINE && status != NODE_STATUS.STARTING)
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      final String dbName = iDatabase.getName();

      final ODistributedConfiguration cfg = getDatabaseConfiguration(dbName);
      if (cfg == null)
        return;

      ODistributedDatabaseImpl distribDatabase = getMessageService().getDatabase(dbName);
      if (distribDatabase == null) {
        // CHECK TO PUBLISH IT TO THE CLUSTER
        distribDatabase = messageService.registerDatabase(dbName, cfg);
        distribDatabase.resume();
        distribDatabase.setOnline();
      }

      if (!(iDatabase.getStorage() instanceof ODistributedStorage) || ((ODistributedStorage) iDatabase.getStorage())
          .getDistributedManager().isOffline()) {

        final ODistributedStorage storage = getStorage(dbName);

        // INIT IT
        storage.wrap((OAbstractPaginatedStorage) iDatabase.getStorage().getUnderlying());

        iDatabase.replaceStorage(storage);

        if (isNodeOnline(nodeName, dbName))
          installDbClustersLocalStrategy(iDatabase);
      }
    } catch (HazelcastException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

    } catch (HazelcastInstanceNotActiveException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    removeStorage(iDatabase.getName());

    final ODistributedMessageService msgService = getMessageService();
    if (msgService != null) {
      msgService.unregisterDatabase(iDatabase.getName());
    }
  }

  public void removeStorage(final String name) {
    synchronized (storages) {
      final ODistributedStorage storage = storages.remove(name);
      if (storage != null) {
        storage.close(true, true);
      }
    }
  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {
  }

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  public String getNodeName(final Member iMember) {
    return getNodeName(iMember, true);
  }

  public String getNodeName(final Member iMember, final boolean useCache) {
    if (iMember == null || iMember.getUuid() == null)
      return "?";

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    final String name = activeNodesNamesByUuid.get(iMember.getUuid());
    if (name != null)
      return name;

    final ODocument cfg = getNodeConfigurationByUuid(iMember.getUuid(), useCache);
    if (cfg != null)
      return cfg.field("name");

    return "ext:" + iMember.getUuid();
  }

  public boolean updateCachedDatabaseConfiguration(final String iDatabaseName, final OModifiableDistributedConfiguration cfg) {
    final ODistributedStorage stg = storages.get(iDatabaseName);
    if (stg == null)
      return false;

    final ODistributedConfiguration dCfg = stg.getDistributedConfiguration();

    ODocument oldCfg = dCfg != null ? dCfg.getDocument() : null;
    Integer oldVersion = oldCfg != null ? (Integer) oldCfg.field("version") : null;
    if (oldVersion == null)
      oldVersion = 0;

    int currVersion = cfg.getVersion();

    final boolean modified = currVersion > oldVersion;

    if (oldCfg != null && !modified) {
      // NO CHANGE, SKIP IT
      OLogManager.instance()
          .debug(this, "Skip saving of distributed configuration file for database '%s' because is unchanged (version %d)",
              iDatabaseName, currVersion);
      return false;
    }

    // SAVE IN NODE'S LOCAL RAM
    stg.setDistributedConfiguration(cfg);

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Broadcasting new distributed configuration for database: %s (version=%d)\n", iDatabaseName, currVersion);

    return modified;
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName) {
    return getDatabaseConfiguration(iDatabaseName, true);
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName, final boolean createIfNotPresent) {
    final ODistributedStorage stg = createIfNotPresent ? getStorage(iDatabaseName) : getStorageIfExists(iDatabaseName);

    if (stg == null)
      return null;

    return stg.getDistributedConfiguration();
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  @Override
  public ODocument getClusterConfiguration() {
    if (!enabled)
      return null;

    final ODocument cluster = new ODocument();

    cluster.field("localName", getName());
    cluster.field("localId", nodeUuid);

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    // members.add(getLocalNodeConfiguration());
    for (Member member : activeNodes.values()) {
      members.add(getNodeConfigurationByUuid(member.getUuid(), true));
    }

    return cluster;
  }

  public abstract String getPublicAddress();

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();
    nodeCfg.setTrackingChanges(false);

    nodeCfg.field("id", nodeId);
    nodeCfg.field("uuid", nodeUuid);
    nodeCfg.field("name", nodeName);
    nodeCfg.field("publicAddress", getPublicAddress());
    nodeCfg.field("startedOn", startedOn);
    nodeCfg.field("status", getNodeStatus());
    nodeCfg.field("connections", serverInstance.getClientConnectionManager().getTotal());

    final List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : serverInstance.getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress(true));
    }

    // STORE THE TEMP USER/PASSWD USED FOR REPLICATION
    final OServerUserConfiguration user = serverInstance.getUser(REPLICATOR_USER);
    if (user != null)
      nodeCfg.field("user_replicator", serverInstance.getUser(REPLICATOR_USER).password);

    nodeCfg.field("databases", getManagedDatabases());

    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;

    nodeCfg.field("usedMemory", usedMem);
    nodeCfg.field("freeMemory", freeMem);
    nodeCfg.field("maxMemory", maxMem);

    nodeCfg.field("latencies", getMessageService().getLatencies(), OType.EMBEDDED);
    nodeCfg.field("messages", getMessageService().getMessageStats(), OType.EMBEDDED);

    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); ) {
      final ODatabaseLifecycleListener listener = it.next();
      if (listener != null)
        listener.onLocalNodeConfigurationRequest(nodeCfg);
    }

    return nodeCfg;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public NODE_STATUS getNodeStatus() {
    return status;
  }

  @Override
  public void setNodeStatus(final NODE_STATUS iStatus) {
    if (status.equals(iStatus))
      // NO CHANGE
      return;

    status = iStatus;

    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Updated node status to '%s'", status);
  }

  public boolean checkNodeStatus(final NODE_STATUS iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  @Override
  public ODistributedResponse sendRequest(final String iDatabaseName, final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes, final ORemoteTask iTask, final long reqId,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback) {

    final ODistributedRequest req = new ODistributedRequest(taskFactory, nodeId, reqId, iDatabaseName, iTask);

    final ODatabaseDocument currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (currentDatabase != null && currentDatabase.getUser() != null)
      // SET CURRENT DATABASE NAME
      req.setUserRID((ORecordId) currentDatabase.getUser().getIdentity().getIdentity());

    final ODistributedDatabaseImpl db = messageService.getDatabase(iDatabaseName);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      ODistributedServerLog
          .error(this, nodeName, null, DIRECTION.OUT, "No nodes configured for partition '%s.%s' request: %s", iDatabaseName,
              iClusterNames, req);
      throw new ODistributedException(
          "No nodes configured for partition '" + iDatabaseName + "." + iClusterNames + "' request: " + req);
    }

    if (db == null) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.OUT, "Distributed database '%s' not found", iDatabaseName);
      throw new ODistributedException("Distributed database '" + iDatabaseName + "' not found on server '" + nodeName + "'");
    }

    messageService.updateMessageStats(iTask.getName());

    return db.send2Nodes(req, iClusterNames, iTargetNodes, iExecutionMode, localResult, iAfterSentCallback);
  }

  /**
   * Executes the request on local node. In case of error returns the Exception itself
   */
  @Override
  public Object executeOnLocalNode(final ODistributedRequestId reqId, final ORemoteTask task,
      final ODatabaseDocumentInternal database) {
    if (database != null && !(database.getStorage() instanceof ODistributedStorage))
      throw new ODistributedException(
          "Distributed storage was not installed for database '" + database.getName() + "'. Implementation found: " + database
              .getStorage().getClass().getName());

    final ODistributedAbstractPlugin manager = this;

    return OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          final Object result = task.execute(reqId, serverInstance, manager, database);

          if (result instanceof Throwable && !(result instanceof OException))
            // EXCEPTION
            ODistributedServerLog.error(this, nodeName, getNodeNameById(reqId.getNodeId()), DIRECTION.IN,
                "Error on executing request %d (%s) on local node: ", (Throwable) result, reqId, task);
          else {
            // OK
            final String sourceNodeName = task.getNodeSource();

            if (database != null) {
              final ODistributedDatabaseImpl ddb = getMessageService().getDatabase(database.getName());

              if (ddb != null && !(result instanceof Throwable) && task instanceof OAbstractReplicatedTask) {
                // UPDATE LSN WITH LAST OPERATION
                ddb.setLSN(sourceNodeName, ((OAbstractReplicatedTask) task).getLastLSN(), true);
              }
            }
          }

          return result;

        } catch (InterruptedException e) {
          // IGNORE IT
          ODistributedServerLog.debug(this, nodeName, getNodeNameById(reqId.getNodeId()), DIRECTION.IN,
              "Interrupted execution on executing distributed request %s on local node: %s", e, reqId, task);
          return e;

        } catch (Throwable e) {
          if (!(e instanceof OException))
            ODistributedServerLog.error(this, nodeName, getNodeNameById(reqId.getNodeId()), DIRECTION.IN,
                "Error on executing distributed request %s on local node: %s", e, reqId, task);

          return e;
        }
      }
    });
  }

  public Set<String> getManagedDatabases() {
    return messageService != null ? messageService.getDatabases() : Collections.EMPTY_SET;
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public int getLocalNodeId() {
    return nodeId;
  }

  @Override
  public void onLocalNodeConfigurationRequest(final ODocument iConfiguration) {
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed())
      return;

    if (status != NODE_STATUS.ONLINE && status != NODE_STATUS.STARTING)
      return;

    // RUN ONLY IN NON-DISTRIBUTED MODE
    if (!isRelatedToLocalServer(iDatabase))
      return;

    if (messageService.getDatabase(iDatabase.getName()) == null)
      // NOT INITIALIZED YET
      return;

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());

    installClustersOfClass(iDatabase, iClass, cfg.modify());
  }

  @SuppressWarnings("unchecked")
  public ODocument getStats() {
    final ODocument doc = new ODocument();

    final Map<String, HashMap<String, Object>> nodes = new HashMap<String, HashMap<String, Object>>();
    doc.field("nodes", nodes);

    Map<String, Object> localNode = new HashMap<String, Object>();
    doc.field("localNode", localNode);

    localNode.put("name", nodeName);
    localNode.put("averageResponseTime", messageService.getAverageResponseTime());

    Map<String, Object> databases = new HashMap<String, Object>();
    localNode.put("databases", databases);
    for (String dbName : messageService.getDatabases()) {
      Map<String, Object> db = new HashMap<String, Object>();
      databases.put(dbName, db);
    }

    return doc;
  }

  @Override
  public String getNodeNameById(final int id) {
    if (id < 0)
      throw new IllegalArgumentException("Node id " + id + " is invalid");

    synchronized (registeredNodeById) {
      if (id < registeredNodeById.size())
        return registeredNodeById.get(id);
    }
    return null;
  }

  @Override
  public int getNodeIdByName(final String name) {
    synchronized (registeredNodeByName) {
      final Integer val = registeredNodeByName.get(name);
      if (val == null)
        return -1;
      return val.intValue();
    }
  }

  @Override
  public String getNodeUuidByName(final String name) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Node name " + name + " is invalid");

    return activeNodesUuidByName.get(name);
  }

  @Override
  public void updateLastClusterChange() {
    lastClusterChangeOn = System.currentTimeMillis();
  }

  @Override
  public void reassignClustersOwnership(final String iNode, final String databaseName,
      final OModifiableDistributedConfiguration cfg) {

    // REASSIGN CLUSTERS WITHOUT AN OWNER, AVOIDING TO REBALANCE EXISTENT
    final ODatabaseDocumentTx database = serverInstance.openDatabase(databaseName, "internal", "internal", null, true);
    try {
      executeInDistributedDatabaseLock(databaseName, 15000, cfg, new OCallable<Boolean, OModifiableDistributedConfiguration>() {
        @Override
        public Boolean call(final OModifiableDistributedConfiguration cfg) {
          rebalanceClusterOwnership(iNode, database, cfg);
          return null;
        }
      });
    } finally {
      database.activateOnCurrentThread();
      database.close();
    }
  }

  @Override
  public boolean isNodeAvailable(final String iNodeName, final String iDatabaseName) {
    final DB_STATUS s = getDatabaseStatus(iNodeName, iDatabaseName);
    return s != DB_STATUS.OFFLINE && s != DB_STATUS.NOT_AVAILABLE;
  }

  @Override
  public boolean isNodeStatusEqualsTo(final String iNodeName, final String iDatabaseName, final DB_STATUS... statuses) {
    final DB_STATUS s = getDatabaseStatus(iNodeName, iDatabaseName);
    for (DB_STATUS st : statuses) {
      if (s == st)
        return true;
    }
    return false;
  }

  @Override
  public boolean isNodeAvailable(final String iNodeName) {
    if (iNodeName == null)
      return false;
    return activeNodes.containsKey(iNodeName);
  }

  @Override
  public boolean isNodeOnline(final String iNodeName, final String iDatabaseName) {
    return getDatabaseStatus(iNodeName, iDatabaseName) == DB_STATUS.ONLINE;
  }

  public boolean isOffline() {
    return status != NODE_STATUS.ONLINE;
  }

  /**
   * Returns the available nodes (not offline) and clears the node list by removing the offline nodes.
   */
  @Override
  public int getAvailableNodes(final Collection<String> iNodes, final String databaseName) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();

      if (!isNodeAvailable(node, databaseName))
        it.remove();
    }
    return iNodes.size();
  }

  /**
   * Returns the nodes with the requested status.
   */
  @Override
  public int getNodesWithStatus(final Collection<String> iNodes, final String databaseName, final DB_STATUS... statuses) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();

      if (!isNodeStatusEqualsTo(node, databaseName, statuses))
        it.remove();
    }
    return iNodes.size();
  }

  @Override
  public String toString() {
    return nodeName;
  }

  @Override
  public ODistributedMessageServiceImpl getMessageService() {
    while (messageService == null)
      // THIS COULD HAPPEN ONLY AT STARTUP
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new OOfflineNodeException("Message Service is not available");
      }
    return messageService;
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
  }

  @Override
  public int getAvailableNodes(final String iDatabaseName) {
    int availableNodes = 0;
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        availableNodes++;
    }
    return availableNodes;
  }

  @Override
  public List<String> getOnlineNodes(final String iDatabaseName) {
    final List<String> onlineNodes = new ArrayList<String>(activeNodes.size());
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeOnline(entry.getKey(), iDatabaseName))
        onlineNodes.add(entry.getKey());
    }
    return onlineNodes;
  }

  @Override
  public synchronized boolean installDatabase(final boolean iStartup, final String databaseName, final boolean forceDeployment,
      final boolean tryWithDeltaFirst) {
    if (getDatabaseStatus(getLocalNodeName(), databaseName) == DB_STATUS.OFFLINE)
      // OFFLINE: AVOID TO INSTALL IT
      return false;

    if (databaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      // DON'T REPLICATE SYSTEM BECAUSE IS DIFFERENT AND PER SERVER
      return false;

    final ODistributedDatabaseImpl distrDatabase = messageService.registerDatabase(databaseName, null);

    return executeInDistributedDatabaseLock(databaseName, 0, null, new OCallable<Boolean, OModifiableDistributedConfiguration>() {
      @Override
      public Boolean call(OModifiableDistributedConfiguration cfg) {
        // GET ALL THE OTHER SERVERS
        final Collection<String> nodes = cfg.getServers(null, nodeName);
        getAvailableNodes(nodes, databaseName);
        if (nodes.size() == 0) {
          ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
              "Cannot install database '%s' on local node, because no servers are available", databaseName);
          return false;
        }

        ODistributedServerLog
            .info(this, nodeName, null, DIRECTION.NONE, "Current node is a %s for database '%s'", cfg.getServerRole(nodeName),
                databaseName);

        final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();
        if (!iStartup && configuredDatabases.contains(databaseName))
          return false;

        // INIT STORAGE + UPDATE LOCAL FILE ONLY
        final ODistributedStorage stg = getStorage(databaseName);
        stg.setDistributedConfiguration(cfg);

        // DISCARD MESSAGES DURING THE REQUEST OF DATABASE INSTALLATION
        distrDatabase.suspend();

        final Boolean deploy = forceDeployment ? Boolean.TRUE : (Boolean) cfg.getDocument().field("autoDeploy");

        boolean databaseInstalled;

        try {

          // CREATE THE DISTRIBUTED QUEUE
          if (!distrDatabase.exists() || distrDatabase.getSyncConfiguration().getMomentum().isEmpty()) {

            if (deploy == null || !deploy) {
              // NO AUTO DEPLOY
              ODistributedServerLog.debug(this, nodeName, null, DIRECTION.NONE,
                  "Skipping download of database '%s' from the cluster because autoDeploy=false", databaseName);

              setDatabaseStatus(nodeName, databaseName, DB_STATUS.ONLINE);
              distrDatabase.resume();
              return false;
            }

            // FIRST TIME, ASK FOR FULL REPLICA
            databaseInstalled = requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);

          } else {
            if (tryWithDeltaFirst) {
              try {

                // TRY WITH DELTA SYNC
                databaseInstalled = requestDatabaseDelta(distrDatabase, databaseName, cfg);

              } catch (ODistributedDatabaseDeltaSyncException e) {
                // FALL BACK TO FULL BACKUP
                removeStorage(databaseName);

                if (deploy == null || !deploy) {
                  // NO AUTO DEPLOY
                  ODistributedServerLog.debug(this, nodeName, null, DIRECTION.NONE,
                      "Skipping download of database '%s' from the cluster because autoDeploy=false", databaseName);

                  setDatabaseStatus(nodeName, databaseName, DB_STATUS.ONLINE);
                  distrDatabase.resume();
                  return false;
                }

                databaseInstalled = requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);
              }
            } else
              // SKIP DELTA AND EXECUTE FULL BACKUP
              databaseInstalled = requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);
          }

          if (databaseInstalled) {
            // OVERWRITE THE LSN
            final ODatabaseDocumentTx db = distrDatabase.getDatabaseInstance();
            try {
              try {
                distrDatabase.getSyncConfiguration()
                    .setLastLSN(nodeName, ((OLocalPaginatedStorage) db.getStorage().getUnderlying()).getLSN(), true);
              } catch (IOException e) {
                ODistributedServerLog
                    .error(this, nodeName, null, DIRECTION.NONE, "Error on setting LSN after the installation of database '%s'",
                        databaseName);
              }
            } finally {
              db.close();
            }
          }

        } catch (ODatabaseIsOldException e) {
          // CURRENT DATABASE IS NEWER, SET ALL OTHER DATABASES AS NOT_AVAILABLE TO FORCE THEM TO ASK FOR THE CURRENT DATABASE
          setDatabaseStatus(nodeName, databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

          final Set<String> otherServers = getAvailableNodeNames(databaseName);
          otherServers.remove(nodeName);

          ODistributedServerLog.info(this, nodeName, otherServers.toString(), DIRECTION.OUT,
              "Current copy of database '%s' is newer then the copy present in the cluster. Use the local copy and force other nodes to download this",
              databaseName);

          for (String s : otherServers) {
            setDatabaseStatus(s, databaseName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
          }
          databaseInstalled = true;
          distrDatabase.resume();
        }

        return databaseInstalled;
      }
    });
  }

  protected boolean requestFullDatabase(final ODistributedDatabaseImpl distrDatabase, final String databaseName,
      final boolean backupDatabase, final OModifiableDistributedConfiguration cfg) {
    for (int retry = 0; retry < DEPLOY_DB_MAX_RETRIES; ++retry) {
      // ASK DATABASE TO THE FIRST NODE, THE FIRST ATTEMPT, OTHERWISE ASK TO EVERYONE
      if (requestDatabaseFullSync(distrDatabase, backupDatabase, databaseName, retry > 0, cfg))
        // DEPLOYED
        return true;
    }
    // RETRY COUNTER EXCEED
    return false;
  }

  public boolean requestDatabaseDelta(final ODistributedDatabaseImpl distrDatabase, final String databaseName,
      final OModifiableDistributedConfiguration cfg) {
    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, nodeName);
    getAvailableNodes(nodes, databaseName);
    if (nodes.size() == 0)
      return false;

    ODistributedServerLog
        .warn(this, nodeName, nodes.toString(), DIRECTION.OUT, "requesting delta database sync for '%s' on local server...",
            databaseName);

    // CREATE A MAP OF NODE/LSN BY READING LAST LSN SAVED
    final Map<String, OLogSequenceNumber> selectedNodes = new HashMap<String, OLogSequenceNumber>(nodes.size());
    for (String node : nodes) {
      final OLogSequenceNumber lsn = distrDatabase.getSyncConfiguration().getLastLSN(node);
      if (lsn != null) {
        selectedNodes.put(node, lsn);
      } else
        ODistributedServerLog
            .info(this, nodeName, node, DIRECTION.OUT, "Last LSN not found for database '%s', skip delta database sync",
                databaseName);
    }

    if (selectedNodes.isEmpty()) {
      // FORCE FULL DATABASE SYNC
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
          "No LSN found for delta sync for database '%s'. Asking for full database sync...", databaseName);
      throw new ODistributedDatabaseDeltaSyncException("Requested database delta sync but no LSN was found");
    }

    for (Map.Entry<String, OLogSequenceNumber> entry : selectedNodes.entrySet()) {

      final String targetNode = entry.getKey();
      final OLogSequenceNumber lsn = entry.getValue();

      final OSyncDatabaseDeltaTask deployTask = new OSyncDatabaseDeltaTask(lsn,
          distrDatabase.getSyncConfiguration().getLastOperationTimestamp());

      final List<String> targetNodes = new ArrayList<String>(1);
      targetNodes.add(targetNode);

      ODistributedServerLog
          .info(this, nodeName, targetNode, DIRECTION.OUT, "Requesting database delta sync for '%s' LSN=%s...", databaseName, lsn);

      try {
        final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, targetNodes, deployTask,
            getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null).getPayload();

        ODistributedServerLog
            .debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Database delta sync returned: %s", results);

        final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

        // EXTRACT THE REAL RESULT
        for (Map.Entry<String, Object> r : results.entrySet()) {
          final Object value = r.getValue();

          if (value instanceof Boolean)
            continue;
          else {
            final String server = r.getKey();

            if (value instanceof ODistributedDatabaseDeltaSyncException) {
              final ODistributedDatabaseDeltaSyncException exc = (ODistributedDatabaseDeltaSyncException) value;

              ODistributedServerLog
                  .warn(this, nodeName, server, DIRECTION.IN, "Error on installing database delta for '%s' (err=%s)", databaseName,
                      exc.getMessage());

              ODistributedServerLog
                  .warn(this, nodeName, server, DIRECTION.IN, "Requesting full database '%s' sync...", databaseName);

              // RESTORE STATUS TO ONLINE
              setDatabaseStatus(server, databaseName, DB_STATUS.ONLINE);

              throw (ODistributedDatabaseDeltaSyncException) value;

            } else if (value instanceof ODatabaseIsOldException) {

              // MANAGE THIS EXCEPTION AT UPPER LEVEL
              throw (ODatabaseIsOldException) value;

            } else if (value instanceof Throwable) {

              ODistributedServerLog
                  .error(this, nodeName, server, DIRECTION.IN, "Error on installing database delta %s in %s (%s)", value,
                      databaseName, dbPath, value);

              setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);
              return false;

            } else if (value instanceof ODistributedDatabaseChunk) {
              // distrDatabase.filterBeforeThisMomentum(((ODistributedDatabaseChunk) value).getMomentum());
              // DISABLED BECAYSE THE MOMENTUM IS NOT YET RELIABLE
              // distrDatabase.setParsing(true);

              final File uniqueClustersBackupDirectory = getClusterOwnedExclusivelyByCurrentNode(dbPath, databaseName);

              installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, server, (ODistributedDatabaseChunk) value, true,
                  uniqueClustersBackupDirectory, cfg);

              ODistributedServerLog
                  .info(this, nodeName, targetNode, DIRECTION.IN, "Installed delta of database '%s'...", databaseName);

              if (!cfg.isSharded())
                // DB NOT SHARDED, THE 1ST BACKUP IS GOOD
                break;

            } else
              throw new IllegalArgumentException("Type " + value + " not supported");
          }
        }
      } catch (ODatabaseIsOldException e) {
        // FORWARD IT
        throw (ODatabaseIsOldException) e;
      } catch (Exception e) {
        ODistributedServerLog
            .error(this, nodeName, targetNode, DIRECTION.OUT, "Error on asking delta backup of database '%s' (err=%s)",
                databaseName, e.getMessage());
        throw new ODistributedDatabaseDeltaSyncException(lsn, e.toString());
      }
    }

    distrDatabase.resume();

    return true;
  }

  protected boolean requestDatabaseFullSync(final ODistributedDatabaseImpl distrDatabase, final boolean backupDatabase,
      final String databaseName, final boolean iAskToAllNodes, final OModifiableDistributedConfiguration cfg) {
    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, nodeName);

    if (nodes.isEmpty()) {
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
          "Cannot request full deploy of database '%s' because there are no nodes available with such database", databaseName);
      return false;
    }

    final List<String> selectedNodes = new ArrayList<String>();

    if (!iAskToAllNodes) {
      // GET THE FIRST ONE IN BACKUP STATUS. THIS FORCES TO HAVE ONE NODE TO DO BACKUP SAVING RESOURCES IN CASE BACKUP IS STILL
      // VALID FOR FURTHER NODES
      for (String n : nodes) {
        if (isNodeStatusEqualsTo(n, databaseName, DB_STATUS.BACKUP)) {
          // SERVER ALREADY IN BACKUP: USE IT
          selectedNodes.add(n);
          break;
        }
      }

      if (selectedNodes.isEmpty()) {
        // GET THE FIRST ONE TO ASK FOR DATABASE. THIS FORCES TO HAVE ONE NODE TO DO BACKUP SAVING RESOURCES IN CASE BACKUP IS STILL
        // VALID FOR FURTHER NODES
        final Iterator<String> it = nodes.iterator();
        while (it.hasNext()) {
          final String f = it.next();
          if (isNodeStatusEqualsTo(f, databaseName, DB_STATUS.ONLINE, DB_STATUS.BACKUP)) {
            selectedNodes.add(f);
            break;
          }
        }
      }
    }

    if (selectedNodes.isEmpty())
      // NO NODE ONLINE, SEND THE MESSAGE TO EVERYONE
      selectedNodes.addAll(nodes);

    ODistributedServerLog
        .warn(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Requesting deploy of database '%s' on local server...",
            databaseName);

    final OLogSequenceNumber lastLSN = distrDatabase.getSyncConfiguration().getLastLSN(getLocalNodeName());

    final OAbstractReplicatedTask deployTask = new OSyncDatabaseTask(lastLSN,
        distrDatabase.getSyncConfiguration().getLastOperationTimestamp());

    final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, selectedNodes, deployTask,
        getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null).getPayload();

    ODistributedServerLog.debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Deploy returned: %s", results);

    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    // EXTRACT THE REAL RESULT
    for (Map.Entry<String, Object> r : results.entrySet()) {
      final Object value = r.getValue();

      if (value instanceof Boolean)
        continue;
      else if (value instanceof ODatabaseIsOldException) {

        // MANAGE THIS EXCEPTION AT UPPER LEVEL
        throw (ODatabaseIsOldException) value;

      } else if (value instanceof Throwable) {
        ODistributedServerLog
            .error(this, nodeName, r.getKey(), DIRECTION.IN, "Error on installing database '%s' in %s", (Exception) value,
                databaseName, dbPath);

        setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);

        if (value instanceof ODistributedException)
          throw (ODistributedException) value;

      } else if (value instanceof ODistributedDatabaseChunk) {

        // DISABLED BECAUSE MOMENTUM IS NOT RELIABLE YET
        // distrDatabase.filterBeforeThisMomentum(((ODistributedDatabaseChunk) value).getMomentum());
        final File uniqueClustersBackupDirectory = getClusterOwnedExclusivelyByCurrentNode(dbPath, databaseName);

        // CLOSE THE STORAGE FIRST
        final ODistributedStorage stg = storages.remove(databaseName);
        if (stg != null)
          stg.close(true, false);

        if (backupDatabase)
          backupCurrentDatabase(databaseName);

        installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, r.getKey(), (ODistributedDatabaseChunk) value, false,
            uniqueClustersBackupDirectory, cfg);

        distrDatabase.resume();

        return true;

      } else
        throw new IllegalArgumentException("Type " + value + " not supported");
    }

    throw new ODistributedException("No response received from remote nodes for auto-deploy of database '" + databaseName + "'");
  }

  protected File getClusterOwnedExclusivelyByCurrentNode(final String dbPath, final String iDatabaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabaseName);

    final HashSet<String> clusters = new HashSet<String>();

    for (String clName : cfg.getClusterNames()) {
      final List<String> servers = cfg.getServers(clName, null);
      if (servers != null) {
        if (servers.size() == 1 && servers.get(0).equals(getLocalNodeName()))
          clusters.add(clName);
      }
    }

    if (!clusters.isEmpty()) {
      // COPY FILES IN A SAFE LOCATION TO BE REPLACED AFTER THE DATABASE RESTORE

      // MOVE DIRECTORY TO ../backup/databases/<db-name>
      final String backupDirectory = Orient.instance().getHomePath() + "/temp/db_" + iDatabaseName;
      final File backupFullPath = new File(backupDirectory);
      if (backupFullPath.exists())
        OFileUtils.deleteRecursively(backupFullPath);
      else
        backupFullPath.mkdirs();

      // MOVE THE DATABASE ON CURRENT NODE
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
          "Saving clusters %s to directory '%s' to be replaced after distributed full backup...", clusters, backupFullPath);

      for (String clName : clusters) {
        // MOVE .PCL and .PCM FILES
        {
          final File oldFile = new File(dbPath + "/" + clName + OPaginatedCluster.DEF_EXTENSION);
          final File newFile = new File(backupFullPath + "/" + clName + OPaginatedCluster.DEF_EXTENSION);

          if (oldFile.exists()) {
            if (!oldFile.renameTo(newFile)) {
              ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
                  "Cannot make a safe copy of exclusive clusters. Error on moving file %s -> %s: restore of database '%s' has been aborted because unsafe",
                  oldFile, newFile, iDatabaseName);
              throw new ODistributedException("Cannot make a safe copy of exclusive clusters");
            }
          }
        }

        {
          final File oldFile = new File(dbPath + "/" + clName + OClusterPositionMap.DEF_EXTENSION);
          final File newFile = new File(backupFullPath + "/" + clName + OClusterPositionMap.DEF_EXTENSION);

          if (oldFile.exists()) {
            if (!oldFile.renameTo(newFile)) {
              ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
                  "Cannot make a safe copy of exclusive clusters. Error on moving file %s -> %s: restore of database '%s' has been aborted because unsafe",
                  oldFile, newFile, iDatabaseName);
              throw new ODistributedException("Cannot make a safe copy of exclusive clusters");
            }
          }
        }

        // TODO: ADD AUTO-SHARDING INDEX FILES TOO

      }
      return backupFullPath;
    }

    return null;
  }

  protected void backupCurrentDatabase(final String iDatabaseName) {
    Orient.instance().unregisterStorageByName(iDatabaseName);

    // MOVE DIRECTORY TO ../backup/databases/<db-name>
    final String backupDirectory = OGlobalConfiguration.DISTRIBUTED_BACKUP_DIRECTORY.getValueAsString();
    if (backupDirectory == null || OIOUtils.getStringContent(backupDirectory).trim().isEmpty())
      // SKIP BACKUP
      return;

    final String backupPath = serverInstance.getDatabaseDirectory() + "/" + backupDirectory + "/" + iDatabaseName;
    final File backupFullPath = new File(backupPath);
    final File f = new File(backupDirectory);
    if (f.exists())
      OFileUtils.deleteRecursively(backupFullPath);
    else
      f.mkdirs();

    final String dbPath = serverInstance.getDatabaseDirectory() + iDatabaseName;

    // MOVE THE DATABASE ON CURRENT NODE
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
        "Moving existent database '%s' in '%s' to '%s' and get a fresh copy from a remote node...", iDatabaseName, dbPath,
        backupPath);

    final File oldDirectory = new File(dbPath);
    if (!oldDirectory.renameTo(backupFullPath)) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
          "Error on moving existent database '%s' located in '%s' to '%s'. Deleting old database...", iDatabaseName, dbPath,
          backupFullPath);

      OFileUtils.deleteRecursively(oldDirectory);
    }
  }

  /**
   * Installs a database from the network.
   */
  protected void installDatabaseFromNetwork(final String dbPath, final String databaseName,
      final ODistributedDatabaseImpl distrDatabase, final String iNode, final ODistributedDatabaseChunk firstChunk,
      final boolean delta, final File uniqueClustersBackupDirectory, final OModifiableDistributedConfiguration cfg) {

    final String fileName = Orient.getTempPath() + "install_" + databaseName + ".zip";

    final String localNodeName = nodeName;

    ODistributedServerLog
        .info(this, localNodeName, iNode, DIRECTION.IN, "Copying remote database '%s' to: %s", databaseName, fileName);

    final File file = new File(fileName);
    if (file.exists())
      file.delete();

    try {
      file.getParentFile().mkdirs();
      file.createNewFile();
    } catch (IOException e) {
      throw OException.wrapException(new ODistributedException("Error on creating temp database file to install locally"), e);
    }

    // DELETE ANY PREVIOUS .COMPLETED FILE
    final File completedFile = new File(file.getAbsolutePath() + ".completed");
    if (completedFile.exists())
      completedFile.delete();

    final AtomicReference<ODistributedMomentum> momentum = new AtomicReference<ODistributedMomentum>();

    try {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.currentThread().setName("OrientDB installDatabase node=" + nodeName + " db=" + databaseName);
            ODistributedDatabaseChunk chunk = firstChunk;

            momentum.set(chunk.getMomentum());

            final OutputStream fOut = new FileOutputStream(fileName, false);
            try {

              long fileSize = writeDatabaseChunk(1, chunk, fOut);
              for (int chunkNum = 2; !chunk.last; chunkNum++) {
                final ODistributedResponse response = sendRequest(databaseName, null, OMultiValue.getSingletonList(iNode),
                    new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                    getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

                final Object result = response.getPayload();
                if (result instanceof Boolean)
                  continue;
                else if (result instanceof Exception) {
                  ODistributedServerLog
                      .error(this, nodeName, iNode, DIRECTION.IN, "error on installing database %s in %s (chunk #%d)",
                          (Exception) result, databaseName, dbPath, chunkNum);
                } else if (result instanceof ODistributedDatabaseChunk) {
                  chunk = (ODistributedDatabaseChunk) result;
                  fileSize += writeDatabaseChunk(chunkNum, chunk, fOut);
                }
              }

              fOut.flush();

              // CREATE THE .COMPLETED FILE TO SIGNAL EOF
              new File(file.getAbsolutePath() + ".completed").createNewFile();

              ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Database copied correctly, size=%s",
                  OFileUtils.getSizeAsString(fileSize));

            } finally {
              try {
                fOut.flush();
                fOut.close();
              } catch (IOException e) {
              }
            }

          } catch (Exception e) {
            ODistributedServerLog
                .error(this, nodeName, null, DIRECTION.NONE, "Error on transferring database '%s' to '%s'", e, databaseName,
                    fileName);
            throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
          }
        }
      }).start();

    } catch (Exception e) {
      ODistributedServerLog
          .error(this, nodeName, null, DIRECTION.NONE, "Error on transferring database '%s' to '%s'", e, databaseName, fileName);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    }

    final ODatabaseDocumentTx db = installDatabaseOnLocalNode(databaseName, dbPath, iNode, fileName, delta,
        uniqueClustersBackupDirectory, cfg);

    if (db != null) {
      // OVERWRITE THE MOMENTUM FROM THE ORIGINAL SERVER AND ADD LAST LOCAL LSN
      try {
        distrDatabase.getSyncConfiguration().load();
        distrDatabase.getSyncConfiguration()
            .setLastLSN(localNodeName, ((OLocalPaginatedStorage) db.getStorage().getUnderlying()).getLSN(), false);
      } catch (IOException e) {
        ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "Error on loading %s file for database '%s'", e,
            DISTRIBUTED_SYNC_JSON_FILENAME, databaseName);
      }

      try {
        rebalanceClusterOwnership(nodeName, db, cfg);
        distrDatabase.setOnline();
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }
    }

    // ASK FOR INDIVIDUAL CLUSTERS IN CASE OF SHARDING AND NO LOCAL COPY
    final Set<String> localManagedClusters = cfg.getClustersOnServer(localNodeName);
    final Set<String> sourceNodeClusters = cfg.getClustersOnServer(iNode);
    localManagedClusters.removeAll(sourceNodeClusters);

    final HashSet<String> toSynchClusters = new HashSet<String>();
    for (String cl : localManagedClusters) {
      // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
      final List<String> servers = cfg.getServers(cl, localNodeName);
      getAvailableNodes(servers, databaseName);

      if (!servers.isEmpty())
        toSynchClusters.add(cl);
    }

    // SYNC ALL THE CLUSTERS
    for (String cl : toSynchClusters) {
      // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
      OCommandExecutorSQLHASyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
    }
  }

  @Override
  public ORemoteTaskFactory getTaskFactory() {
    return taskFactory;
  }

  /**
   * Guarantees, foreach class, that has own master cluster.
   */
  @Override
  public void propagateSchemaChanges(final ODatabaseInternal iDatabase) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    for (OClass c : iDatabase.getMetadata().getSchema().getClasses()) {
      if (!(c.getClusterSelection() instanceof OLocalClusterWrapperStrategy))
        // INSTALL ONLY ON NON-ENHANCED CLASSES
        ((OClassImpl) c)
            .setClusterSelectionInternal(new OLocalClusterWrapperStrategy(this, iDatabase.getName(), c, c.getClusterSelection()));
    }
  }

  /**
   * Guarantees that each class has own master cluster.
   */
  public synchronized boolean installClustersOfClass(final ODatabaseInternal iDatabase, final OClass iClass,
      OModifiableDistributedConfiguration cfg) {

    final String databaseName = iDatabase.getName();

    if (!(iClass.getClusterSelection() instanceof OLocalClusterWrapperStrategy))
      // INJECT LOCAL CLUSTER STRATEGY
      ((OClassImpl) iClass)
          .setClusterSelectionInternal(new OLocalClusterWrapperStrategy(this, databaseName, iClass, iClass.getClusterSelection()));

    if (iClass.isAbstract())
      return false;

    // INIT THE DATABASE IF NEEDED
    getMessageService().registerDatabase(databaseName, cfg);

    return executeInDistributedDatabaseLock(databaseName, 20000, cfg,
        new OCallable<Boolean, OModifiableDistributedConfiguration>() {
          @Override
          public Boolean call(final OModifiableDistributedConfiguration lastCfg) {
            final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

            final List<String> cluster2Create = clusterAssignmentStrategy
                .assignClusterOwnershipOfClass(iDatabase, lastCfg, iClass, availableNodes);

            final Map<OClass, List<String>> cluster2CreateMap = new HashMap<OClass, List<String>>(1);
            cluster2CreateMap.put(iClass, cluster2Create);

            createClusters(iDatabase, cluster2CreateMap, lastCfg);
            return true;
          }
        });
  }

  private void createClusters(final ODatabaseInternal iDatabase, final Map<OClass, List<String>> cluster2Create,
      OModifiableDistributedConfiguration cfg) {
    if (cluster2Create.isEmpty())
      return;

    executeInDistributedDatabaseLock(iDatabase.getName(), 0, cfg, new OCallable<Object, OModifiableDistributedConfiguration>() {
      @Override
      public Object call(final OModifiableDistributedConfiguration cfg) {

        // UPDATE LAST CFG BEFORE TO MODIFY THE CLUSTERS
        updateCachedDatabaseConfiguration(iDatabase.getName(), cfg, true);

        for (Map.Entry<OClass, List<String>> entry : cluster2Create.entrySet()) {
          final OClass clazz = entry.getKey();

          // SAVE CONFIGURATION LOCALLY TO ALLOW THE CREATION OF THE CLUSTERS IF ANY
          // CHECK OWNER AFTER RE-BALANCE AND CREATE NEW CLUSTERS IF NEEDED
          for (final String newClusterName : entry.getValue()) {

            ODistributedServerLog.info(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Class '%s', creation of new local cluster '%s' (id=%d)", clazz, newClusterName,
                iDatabase.getClusterIdByName(newClusterName));

            OScenarioThreadLocal.executeAsDefault(new Callable<Object>() {
              @Override
              public Object call() throws Exception {
                try {
                  clazz.addCluster(newClusterName);
                } catch (Exception e) {
                  if (!iDatabase.getClusterNames().contains(newClusterName)) {
                    // NOT CREATED
                    ODistributedServerLog.error(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                        "Error on creating cluster '%s' in class '%s': ", newClusterName, clazz, e);
                    throw OException.wrapException(
                        new ODistributedException("Error on creating cluster '" + newClusterName + "' in class '" + clazz + "'"),
                        e);
                  }
                }
                return null;
              }
            });
          }
        }
        return null;
      }
    });
  }

  public ODistributedStrategy getDistributedStrategy() {
    return responseManagerFactory;
  }

  public void setDistributedStrategy(final ODistributedStrategy streatgy) {
    this.responseManagerFactory = streatgy;
  }

  /**
   * Executes an operation protected by a distributed lock (one per database).
   *
   * @param <T>            Return type
   * @param databaseName   Database name
   * @param timeoutLocking
   * @param iCallback      Operation @return The operation's result of type T
   */
  public <T> T executeInDistributedDatabaseLock(final String databaseName, final long timeoutLocking,
      OModifiableDistributedConfiguration lastCfg, final OCallable<T, OModifiableDistributedConfiguration> iCallback) {

    if (OScenarioThreadLocal.INSTANCE.isInDatabaseLock()) {
      // ALREADY IN LOCK
      try {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog
              .debug(this, nodeName, null, DIRECTION.NONE, "Current distributed configuration for database '%s': %s", databaseName,
                  lastCfg.getDocument().toJSON());

        return (T) iCallback.call(lastCfg);

      } finally {

        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog
              .debug(this, nodeName, null, DIRECTION.NONE, "New distributed configuration for database '%s': %s", databaseName,
                  lastCfg.getDocument().toJSON());

        // CONFIGURATION CHANGED, UPDATE IT ON THE CLUSTER AND DISK
        updateCachedDatabaseConfiguration(databaseName, lastCfg, true);
      }
    }

    lockManagerRequester.acquireExclusiveLock(databaseName, nodeName, timeoutLocking);
    try {

      OScenarioThreadLocal.INSTANCE.setInDatabaseLock(true);

      if (lastCfg == null)
        // ACQUIRE CFG INSIDE THE LOCK
        lastCfg = getDatabaseConfiguration(databaseName).modify();

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(this, nodeName, null, DIRECTION.NONE, "Current distributed configuration for database '%s': %s", databaseName,
                lastCfg.getDocument().toJSON());

      try {

        return (T) iCallback.call(lastCfg);

      } finally {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog
              .debug(this, nodeName, null, DIRECTION.NONE, "New distributed configuration for database '%s': %s", databaseName,
                  lastCfg.getDocument().toJSON());

        // CONFIGURATION CHANGED, UPDATE IT ON THE CLUSTER AND DISK
        updateCachedDatabaseConfiguration(databaseName, lastCfg, true);

        OScenarioThreadLocal.INSTANCE.setInDatabaseLock(false);
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);

    } finally {
      lockManagerRequester.releaseExclusiveLock(databaseName, nodeName);
    }
  }

  protected void onDatabaseEvent(final String nodeName, final String databaseName, final DB_STATUS status) {
    updateLastClusterChange();
    dumpServersStatus();
  }

  protected void rebalanceClusterOwnership(final String iNode, final ODatabaseInternal iDatabase,
      final OModifiableDistributedConfiguration cfg) {
    final ODistributedConfiguration.ROLES role = cfg.getServerRole(iNode);
    if (role != ODistributedConfiguration.ROLES.MASTER)
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return;

    if (iDatabase.isClosed())
      getServerInstance().openDatabase(iDatabase);

    ODistributedServerLog
        .info(this, nodeName, null, DIRECTION.NONE, "Reassigning ownership of clusters for database %s...", iDatabase.getName());

    final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

    iDatabase.activateOnCurrentThread();
    final OSchema schema = ((ODatabaseInternal<?>) iDatabase).getDatabaseOwner().getMetadata().getSchema();

    final Map<OClass, List<String>> cluster2CreateMap = new HashMap<OClass, List<String>>(1);
    for (final OClass clazz : schema.getClasses()) {
      final List<String> cluster2Create = clusterAssignmentStrategy
          .assignClusterOwnershipOfClass(iDatabase, cfg, clazz, availableNodes);

      cluster2CreateMap.put(clazz, cluster2Create);
    }

    createClusters(iDatabase, cluster2CreateMap, cfg);

    ODistributedServerLog
        .info(this, nodeName, null, DIRECTION.NONE, "Reassignment of clusters for database '%s' completed (classes=%d)",
            iDatabase.getName(), cluster2CreateMap.size());
  }

  protected void installDbClustersLocalStrategy(final ODatabaseInternal iDatabase) {
    final OSchema schema = iDatabase.getDatabaseOwner().getMetadata().getSchema();

    for (OClass c : schema.getClasses()) {
      if (!(c.getClusterSelection() instanceof OLocalClusterWrapperStrategy))
        // OVERWRITE CLUSTER SELECTION STRATEGY
        ((OClassImpl) c)
            .setClusterSelectionInternal(new OLocalClusterWrapperStrategy(this, iDatabase.getName(), c, c.getClusterSelection()));
    }
  }

  protected void assignNodeName() {
    // ORIENTDB_NODE_NAME ENV VARIABLE OR JVM SETTING
    nodeName = OSystemVariableResolver.resolveVariable(NODE_NAME_ENV);

    if (nodeName != null) {
      nodeName = nodeName.trim();
      if (nodeName.isEmpty())
        nodeName = null;
    }

    if (nodeName == null) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |         WARNING: FIRST DISTRIBUTED RUN CONFIGURATION          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | This is the first time that the server is running as          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | distributed. Please type the name you want to assign to the   |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | current server node.                                          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |                                                               |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | setting ORIENTDB_NODE_NAME to the server node name to use.    |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.print(OAnsiCode.format("\n$ANSI{yellow Node name [BLANK=auto generate it]: }"));

      OConsoleReader reader = new ODefaultConsoleReader();
      try {
        nodeName = reader.readLine();
      } catch (IOException e) {
      }
      if (nodeName != null) {
        nodeName = nodeName.trim();
        if (nodeName.isEmpty())
          nodeName = null;
      }
    }

    if (nodeName == null)
      // GENERATE NODE NAME
      this.nodeName = "node" + System.currentTimeMillis();

    OLogManager.instance().warn(this, "Assigning distributed node name: %s", this.nodeName);

    // SALVE THE NODE NAME IN CONFIGURATION
    boolean found = false;
    final OServerConfiguration cfg = serverInstance.getConfiguration();
    for (OServerHandlerConfiguration h : cfg.handlers) {
      if (h.clazz.equals(getClass().getName())) {
        for (OServerParameterConfiguration p : h.parameters) {
          if (p.name.equals("nodeName")) {
            found = true;
            p.value = this.nodeName;
            break;
          }
        }

        if (!found) {
          h.parameters = OArrays.copyOf(h.parameters, h.parameters.length + 1);
          h.parameters[h.parameters.length - 1] = new OServerParameterConfiguration("nodeName", this.nodeName);
        }

        try {
          serverInstance.saveConfiguration();
        } catch (IOException e) {
          throw OException.wrapException(new OConfigurationException("Cannot save server configuration"), e);
        }
        break;
      }
    }
  }

  protected long writeDatabaseChunk(final int iChunkId, final ODistributedDatabaseChunk chunk, final OutputStream out)
      throws IOException {

    ODistributedServerLog
        .info(this, nodeName, null, DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId, chunk.offset,
            OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }

  protected ODatabaseDocumentTx installDatabaseOnLocalNode(final String databaseName, final String dbPath, final String iNode,
      final String iDatabaseCompressedFile, final boolean delta, final File uniqueClustersBackupDirectory,
      final OModifiableDistributedConfiguration cfg) {
    ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN, "Installing database '%s' to: %s...", databaseName, dbPath);

    try {
      final File f = new File(iDatabaseCompressedFile);
      final File fCompleted = new File(iDatabaseCompressedFile + ".completed");

      new File(dbPath).mkdirs();
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath);

      // USES A CUSTOM WRAPPER OF IS TO WAIT FOR FILE IS WRITTEN (ASYNCH)
      final FileInputStream in = new FileInputStream(f) {
        @Override
        public int read() throws IOException {
          while (true) {
            final int read = super.read();
            if (read > -1)
              return read;

            if (fCompleted.exists())
              return 0;

            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
          while (true) {
            final int read = super.read(b, off, len);
            if (read > 0)
              return read;

            if (fCompleted.exists())
              return 0;

            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }

        @Override
        public int available() throws IOException {
          while (true) {
            final int avail = super.available();
            if (avail > 0)
              return avail;

            if (fCompleted.exists())
              return 0;

            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }
      };

      try {
        final ODistributedAbstractPlugin me = this;
        executeInDistributedDatabaseLock(databaseName, 0, cfg, new OCallable<Void, OModifiableDistributedConfiguration>() {
          @Override
          public Void call(final OModifiableDistributedConfiguration cfg) {
            try {
              if (delta) {

                new OIncrementalServerSync().importDelta(serverInstance, db, in, iNode);

              } else {

                // IMPORT FULL DATABASE (LISTENER ONLY FOR DEBUG PURPOSE)
                db.restore(in, null, new Callable<Object>() {
                  @Override
                  public Object call() throws Exception {
                    if (uniqueClustersBackupDirectory != null && uniqueClustersBackupDirectory.exists()) {
                      // RESTORE UNIQUE FILES FROM THE BACKUP FOLDERS. THOSE FILES ARE THE CLUSTERS OWNED EXCLUSIVELY BY CURRENT
                      // NODE THAT WOULD BE LOST IF NOT REPLACED
                      for (File f : uniqueClustersBackupDirectory.listFiles()) {
                        final File oldFile = new File(dbPath + "/" + f.getName());
                        if (oldFile.exists())
                          oldFile.delete();

                        // REPLACE IT
                        if (!f.renameTo(oldFile))
                          throw new ODistributedException(
                              "Cannot restore exclusive cluster file '" + f.getAbsolutePath() + "' into " + oldFile
                                  .getAbsolutePath());
                      }

                      uniqueClustersBackupDirectory.delete();
                    }
                    return null;
                  }
                }, ODistributedServerLog.isDebugEnabled() ? me : null);

              }
              return null;
            } catch (IOException e) {
              throw OException.wrapException(new OIOException("Error on distributed sync of database"), e);
            }
          }
        });
      } finally {
        in.close();
      }

      getServerInstance().openDatabase(db, "internal", "internal", null, true);
      db.reload();

      ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Installed database '%s' (LSN=%s)", databaseName,
          ((OAbstractPaginatedStorage) db.getStorage().getUnderlying()).getLSN());

      return db;

    } catch (IOException e) {
      ODistributedServerLog
          .warn(this, nodeName, null, DIRECTION.IN, "Error on copying database '%s' on local server", e, databaseName);
    }
    return null;
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n"))
      iText = iText.substring(2);
    else if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().debug(this, iText);
  }

  public void stopNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of stopping node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(taskFactory, nodeId, getNextMessageIdCounter(), null,
        new OStopServerTask());

    getRemoteServer(iNode).sendRequest(request);
  }

  public void restartNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of restarting node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(taskFactory, nodeId, getNextMessageIdCounter(), null,
        new ORestartServerTask());

    getRemoteServer(iNode).sendRequest(request);
  }

  public Set<String> getAvailableNodeNames(final String iDatabaseName) {
    final Set<String> nodes = new HashSet<String>();

    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        nodes.add(entry.getKey());
    }
    return nodes;
  }

  public long getNextMessageIdCounter() {
    final long v = localMessageIdCounter.getAndIncrement();
    return v;
  }

  public void closeRemoteServer(final String node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  protected boolean isRelatedToLocalServer(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    // Check for the system database.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      return false;

    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return false;
    } else if (dbUrl.startsWith("remote:"))
      return false;

    return true;
  }

  /**
   * Avoids to dump the same configuration twice if it's unchanged since the last time.
   */
  protected void dumpServersStatus() {
    final ODocument cfg = getClusterConfiguration();

    final String compactStatus = ODistributedOutput.getCompactServerStatus(this, cfg);

    if (!lastServerDump.equals(compactStatus)) {
      lastServerDump = compactStatus;

      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Distributed servers status:\n%s",
          ODistributedOutput.formatServerStatus(this, cfg));
    }
  }

  public ODistributedStorage getStorageIfExists(final String dbName) {
    return storages.get(dbName);
  }

  public ODistributedStorage getStorage(final String dbName) {
    ODistributedStorage storage = storages.get(dbName);
    if (storage == null) {
      storage = new ODistributedStorage(serverInstance, dbName);

      final ODistributedStorage oldStorage = storages.putIfAbsent(dbName, storage);
      if (oldStorage != null)
        storage = oldStorage;
    }
    return storage;
  }

  @Override
  public ODistributedConflictResolverFactory getConflictResolverFactory() {
    return conflictResolverFactory;
  }

  public static String getListeningBinaryAddress(final ODocument cfg) {
    String url = cfg.field("publicAddress");

    final Collection<Map<String, Object>> listeners = (Collection<Map<String, Object>>) cfg.field("listeners");
    if (listeners == null)
      throw new ODatabaseException(
          "Cannot connect to a remote node because bad distributed configuration: missing 'listeners' array field");
    String listenUrl = null;
    for (Map<String, Object> listener : listeners) {
      if ((listener.get("protocol")).equals("ONetworkProtocolBinary")) {
        listenUrl = (String) listener.get("listen");
        break;
      }
    }
    if (url == null)
      url = listenUrl;
    else {
      int pos;
      String port;
      if ((pos = listenUrl.lastIndexOf(":")) != -1) {
        port = listenUrl.substring(pos + 1);
      } else {
        port = "2424";
      }
      url += ":" + port;
    }
    return url;
  }
}
