/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_MAX_STARTUP_DELAY;
import static com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION.OUT;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
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
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedLockManager;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManagerFactory;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManagerImpl;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStartupException;
import com.orientechnologies.orient.server.distributed.ODistributedStrategy;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.ORemoteServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactoryManager;
import com.orientechnologies.orient.server.distributed.impl.task.ODropDatabaseTask;
import com.orientechnologies.orient.server.distributed.impl.task.ONewDeltaTaskResponse;
import com.orientechnologies.orient.server.distributed.impl.task.ORemoteTaskFactoryManagerImpl;
import com.orientechnologies.orient.server.distributed.impl.task.ORestartServerTask;
import com.orientechnologies.orient.server.distributed.impl.task.OStopServerTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncDatabaseNewDeltaTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncDatabaseTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseConfigurationTask;
import com.orientechnologies.orient.server.distributed.sql.OCommandExecutorSQLHASyncCluster;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ODatabaseIsOldException;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastClusterMetadataManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import sun.misc.Signal;

/**
 * Plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedPlugin extends OServerPluginAbstract
    implements ODistributedServerManager, ODatabaseLifecycleListener, OCommandOutputListener {
  public static final String REPLICATOR_USER = "_CrossServerTempUser";

  protected static final String PAR_DEF_DISTRIB_DB_CONFIG = "configuration.db.default";
  protected static final String NODE_NAME_ENV = "ORIENTDB_NODE_NAME";

  private OServer serverInstance;
  private String nodeName = null;
  protected File defaultDatabaseConfigFile;
  protected List<ODistributedLifecycleListener> listeners = new ArrayList<>();
  protected ORemoteServerManager remoteServerManager;

  // LOCAL MSG COUNTER
  protected AtomicLong localMessageIdCounter = new AtomicLong();
  protected OClusterOwnershipAssignmentStrategy clusterAssignmentStrategy =
      new ODefaultClusterOwnershipAssignmentStrategy(this);

  protected static final int DEPLOY_DB_MAX_RETRIES = 10;
  protected Set<String> installingDatabases =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  protected volatile ODistributedMessageServiceImpl messageService;
  protected Date startedOn = new Date();
  protected ODistributedStrategy responseManagerFactory = new ODefaultDistributedStrategy();
  protected ORemoteTaskFactoryManager taskFactoryManager = new ORemoteTaskFactoryManagerImpl(this);

  private volatile String lastServerDump = "";
  protected CountDownLatch serverStarted = new CountDownLatch(1);

  private TimerTask haStatsTask = null;
  private TimerTask healthCheckerTask = null;
  protected OSignalHandler.OSignalListener signalListener;

  private final OHazelcastClusterMetadataManager clusterManager;

  protected ODistributedPlugin() {
    clusterManager = new OHazelcastClusterMetadataManager(this);
  }

  public void waitUntilNodeOnline() throws InterruptedException {
    serverStarted.await();
  }

  public void waitUntilNodeOnline(final String nodeName, final String databaseName)
      throws InterruptedException {
    while (messageService == null
        || messageService.getDatabase(databaseName) == null
        || !isNodeOnline(nodeName, databaseName)) Thread.sleep(100);
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
          throw new OConfigurationException(
              "Illegal node name '" + nodeName + "'. '.' is not allowed in node name");
      } else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        setDefaultDatabaseConfigFile(param.value);
      }
    }

    this.remoteServerManager =
        new ORemoteServerManager(
            nodeName,
            new ORemoteServerAvailabilityCheck() {
              @Override
              public boolean isNodeAvailable(String node) {
                return ODistributedPlugin.this.isNodeAvailable(node);
              }

              @Override
              public void nodeDisconnected(String node) {
                ODistributedPlugin.this.removeServer(node, true);
              }
            });
    if (nodeName == null) assignNodeName();
    clusterManager.configHazelcastPlugin(oServer, iParams, nodeName);
  }

  @Override
  @Deprecated
  public String getCoordinatorServer() {
    return "";
  }

  public File getDefaultDatabaseConfigFile() {
    return defaultDatabaseConfigFile;
  }

  @Override
  public ODistributedLockManager getLockManagerRequester() {
    return clusterManager.getLockManagerRequester();
  }

  @Override
  public ODistributedLockManager getLockManagerExecutor() {
    return clusterManager.getLockManagerExecutor();
  }

  @Override
  public <T> T executeInDistributedDatabaseLock(
      String databaseName,
      long timeoutLocking,
      OModifiableDistributedConfiguration lastCfg,
      OCallable<T, OModifiableDistributedConfiguration> iCallback) {
    return clusterManager.executeInDistributedDatabaseLock(
        databaseName, timeoutLocking, lastCfg, iCallback);
  }

  @Override
  public boolean isWriteQuorumPresent(String databaseName) {
    return clusterManager.isWriteQuorumPresent(databaseName);
  }

  public void setDefaultDatabaseConfigFile(final String iFile) {
    defaultDatabaseConfigFile = new File(OSystemVariableResolver.resolveSystemVariables(iFile));
    if (!defaultDatabaseConfigFile.exists())
      throw new OConfigurationException(
          "Cannot find distributed database config file: " + defaultDatabaseConfigFile);
  }

  @Override
  public void startup() {
    if (!enabled) return;
    if (serverInstance.getDatabases() instanceof OrientDBDistributed)
      ((OrientDBDistributed) serverInstance.getDatabases()).setPlugin(this);

    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    serverInstance.addTemporaryUser(REPLICATOR_USER, "" + new SecureRandom().nextLong(), "*");

    Orient.instance().addDbLifecycleListener(this);

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    remoteServerManager.closeAll();

    messageService = new ODistributedMessageServiceImpl(this);

    try {
      clusterManager.startupHazelcastPlugin();

      final long statsDelay = OGlobalConfiguration.DISTRIBUTED_DUMP_STATS_EVERY.getValueAsLong();
      if (statsDelay > 0) {
        haStatsTask = Orient.instance().scheduleTask(this::dumpStats, statsDelay, statsDelay);
      }

      final long healthChecker =
          OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY.getValueAsLong();
      if (healthChecker > 0) {
        healthCheckerTask =
            Orient.instance()
                .scheduleTask(
                    new OClusterHealthChecker(this, healthChecker), healthChecker, healthChecker);
      }

      signalListener =
          new OSignalHandler.OSignalListener() {
            @Override
            public void onSignal(final Signal signal) {
              if (signal.toString().trim().equalsIgnoreCase("SIGTRAP")) dumpStats();
            }
          };
      Orient.instance().getSignalHandler().registerListener(signalListener);
    } catch (Exception e) {
      ODistributedServerLog.error(
          this, nodeName, null, DIRECTION.NONE, "Error on starting distributed plugin", e);
      throw OException.wrapException(
          new ODistributedStartupException("Error on starting distributed plugin"), e);
    }

    dumpServersStatus();
  }

  @Override
  public ODistributedPlugin registerLifecycleListener(
      final ODistributedLifecycleListener iListener) {
    if (iListener == null) {
      throw new NullPointerException();
    }
    listeners.add(iListener);
    return this;
  }

  @Override
  public ODistributedPlugin unregisterLifecycleListener(
      final ODistributedLifecycleListener iListener) {
    listeners.remove(iListener);
    return this;
  }

  @Override
  public void shutdown() {
    if (!enabled) return;
    OSignalHandler signalHandler = Orient.instance().getSignalHandler();
    if (signalHandler != null) signalHandler.unregisterListener(signalListener);

    OLogManager.instance().warn(this, "Shutting down node '%s'...", nodeName);
    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    clusterManager.prepareHazelcastPluginShutdown();
    try {
      if (healthCheckerTask != null) healthCheckerTask.cancel();
      if (haStatsTask != null) haStatsTask.cancel();

      // CLOSE ALL CONNECTIONS TO THE SERVERS
      remoteServerManager.closeAll();

      if (messageService != null) messageService.shutdown();

      setNodeStatus(NODE_STATUS.OFFLINE);

      Orient.instance().removeDbLifecycleListener(this);
    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }
    clusterManager.hazelcastPluginShutdown();
  }

  /** Auto register myself as hook. */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {}

  public void registerNewDatabaseIfNeeded(String dbName) {
    ODistributedDatabaseImpl distribDatabase = getMessageService().getDatabase(dbName);
    if (distribDatabase == null) {
      // CHECK TO PUBLISH IT TO THE CLUSTER
      distribDatabase = messageService.registerDatabase(dbName);
      distribDatabase.checkNodeInConfiguration(getLocalNodeName());
      distribDatabase.resume();
      distribDatabase.setOnline();
    }
  }

  /** Remove myself as hook. */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase)) return;

    final String dbName = iDatabase.getName();

    ODistributedServerLog.info(
        this, getLocalNodeName(), null, DIRECTION.NONE, "Dropping database %s...", dbName);

    final ODistributedMessageService msgService = getMessageService();
    if (msgService != null) {
      msgService.unregisterDatabase(iDatabase.getName());
    }

    clusterManager.removeDbFromClusterMetadata(iDatabase);
  }

  public void dropOnAllServers(final String dbName) {
    Set<String> servers = clusterManager.dropDbFromConfiguration(dbName);
    if (!servers.isEmpty() && messageService.getDatabase(dbName) != null) {
      sendRequest(
          dbName,
          null,
          servers,
          new ODropDatabaseTask(),
          getNextMessageIdCounter(),
          ODistributedRequest.EXECUTION_MODE.RESPONSE,
          null);
    }
  }

  public void dropConfig(String dbName) {
    clusterManager.dropDatabaseConfiguration(dbName);
  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {}

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();
    nodeCfg.setTrackingChanges(false);

    nodeCfg.field("id", getLocalNodeId());
    nodeCfg.field("uuid", clusterManager.getLocalNodeUuid());
    nodeCfg.field("name", nodeName);
    nodeCfg.field("version", OConstants.getRawVersion());
    nodeCfg.field("publicAddress", clusterManager.getPublicAddress());
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
    final OSecurityUser user = serverInstance.getSecurity().getUser(REPLICATOR_USER);
    if (user != null)
      nodeCfg.field(
          "user_replicator", serverInstance.getSecurity().getUser(REPLICATOR_USER).getPassword());

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

    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners();
        it.hasNext(); ) {
      final ODatabaseLifecycleListener listener = it.next();
      if (listener != null) listener.onLocalNodeConfigurationRequest(nodeCfg);
    }

    return nodeCfg;
  }

  @Override
  public ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName) {
    return clusterManager.getDatabaseConfiguration(iDatabaseName);
  }

  @Override
  public ODistributedConfiguration getDatabaseConfiguration(
      String iDatabaseName, boolean createIfNotPresent) {
    return clusterManager.getDatabaseConfiguration(iDatabaseName, createIfNotPresent);
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public ODistributedResponse sendRequest(
      final String iDatabaseName,
      final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes,
      final ORemoteTask iTask,
      final long reqId,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult) {
    return sendRequest(
        iDatabaseName,
        iClusterNames,
        iTargetNodes,
        iTask,
        reqId,
        iExecutionMode,
        localResult,
        null);
  }

  public ODistributedResponse sendRequest(
      final String iDatabaseName,
      final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes,
      final ORemoteTask iTask,
      final long reqId,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {

    final ODistributedRequest req =
        new ODistributedRequest(this, getLocalNodeId(), reqId, iDatabaseName, iTask);

    final ODatabaseDocument currentDatabase = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (currentDatabase != null
        && currentDatabase.getUser() != null
        && currentDatabase.getUser().getIdentity().getIdentity().isValid())
      // SET CURRENT DATABASE NAME
      req.setUserRID((ORecordId) currentDatabase.getUser().getIdentity().getIdentity());

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      ODistributedServerLog.error(
          this,
          nodeName,
          null,
          DIRECTION.OUT,
          "No nodes configured for partition '%s.%s' request: %s",
          iDatabaseName,
          iClusterNames,
          req);
      throw new ODistributedException(
          "No nodes configured '" + iDatabaseName + "." + iClusterNames + "' request: " + req);
    }

    messageService.updateMessageStats(iTask.getName());
    if (responseManagerFactory != null) {
      return send2Nodes(
          req, iClusterNames, iTargetNodes, iExecutionMode, localResult, responseManagerFactory);
    } else {
      return send2Nodes(req, iClusterNames, iTargetNodes, iExecutionMode, localResult);
    }
  }

  protected void checkForServerOnline(final ODistributedRequest iRequest)
      throws ODistributedException {
    final ODistributedServerManager.NODE_STATUS srvStatus = getNodeStatus();
    if (srvStatus == ODistributedServerManager.NODE_STATUS.OFFLINE
        || srvStatus == ODistributedServerManager.NODE_STATUS.SHUTTINGDOWN) {
      ODistributedServerLog.error(
          this,
          this.nodeName,
          null,
          OUT,
          "Local server is not online (status='%s'). Request %s will be ignored",
          srvStatus,
          iRequest);
      throw new OOfflineNodeException(
          "Local server is not online (status='"
              + srvStatus
              + "'). Request "
              + iRequest
              + " will be ignored");
    }
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      final Collection<String> iClusterNames,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    try {
      checkForServerOnline(iRequest);

      final String databaseName = iRequest.getDatabaseName();

      if (iNodes.isEmpty()) {
        ODistributedServerLog.error(
            this,
            this.nodeName,
            null,
            OUT,
            "No nodes configured for database '%s' request: %s",
            databaseName,
            iRequest);
        throw new ODistributedException(
            "No nodes configured for partition '" + databaseName + "' request: " + iRequest);
      }
      final ORemoteTask task = iRequest.getTask();
      final boolean checkNodesAreOnline = task.isNodeOnlineRequired();

      final ODistributedConfiguration cfg;
      final Set<String> nodesConcurToTheQuorum;
      int availableNodes = iNodes.size();
      int onlineMasters;
      if (databaseName != null) {
        cfg = getDatabaseConfiguration(databaseName);
        nodesConcurToTheQuorum =
            getDistributedStrategy()
                .getNodesConcurInQuorum(this, cfg, iRequest, iNodes, localResult);

        // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE
        // NODES

        if (checkNodesAreOnline) {
          availableNodes =
              getNodesWithStatus(
                  iNodes,
                  databaseName,
                  ODistributedServerManager.DB_STATUS.ONLINE,
                  ODistributedServerManager.DB_STATUS.BACKUP,
                  ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
        }

        // all online masters
        onlineMasters =
            getOnlineNodes(databaseName).stream()
                .filter(f -> cfg.getServerRole(f) == ODistributedConfiguration.ROLES.MASTER)
                .collect(Collectors.toSet())
                .size();

      } else {
        cfg = null;
        nodesConcurToTheQuorum = new HashSet<String>(iNodes);
        onlineMasters = availableNodes;
      }

      final int expectedResponses = localResult != null ? availableNodes + 1 : availableNodes;

      final int quorum =
          calculateQuorum(
              task.getQuorumType(),
              iClusterNames,
              cfg,
              expectedResponses,
              nodesConcurToTheQuorum.size(),
              onlineMasters,
              checkNodesAreOnline,
              this.nodeName);

      final boolean groupByResponse =
          task.getResultStrategy() != OAbstractRemoteTask.RESULT_STRATEGY.UNION;

      final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

      // CREATE THE RESPONSE MANAGER
      final ODistributedResponseManager currentResponseMgr =
          responseManagerFactory.newResponseManager(
              iRequest,
              iNodes,
              task,
              nodesConcurToTheQuorum,
              availableNodes,
              expectedResponses,
              quorum,
              groupByResponse,
              waitLocalNode);

      if (localResult != null && currentResponseMgr.setLocalResult(this.nodeName, localResult)) {
        // COLLECT LOCAL RESULT ONLY
        return currentResponseMgr.getFinalResponse();
      }

      // SORT THE NODE TO GUARANTEE THE SAME ORDER OF DELIVERY
      if (!(iNodes instanceof List)) iNodes = new ArrayList<String>(iNodes);
      if (iNodes.size() > 1) Collections.sort((List<String>) iNodes);

      this.messageService.registerRequest(iRequest.getId().getMessageId(), currentResponseMgr);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(
            this, this.nodeName, iNodes.toString(), OUT, "Sending request %s...", iRequest);

      for (String node : iNodes) {
        // CATCH ANY EXCEPTION LOG IT AND IGNORE TO CONTINUE SENDING REQUESTS TO OTHER NODES
        try {
          final ORemoteServerController remoteServer = getRemoteServer(node);

          remoteServer.sendRequest(iRequest);

        } catch (Exception e) {
          currentResponseMgr.removeServerBecauseUnreachable(node);

          String reason = e.getMessage();
          if (e instanceof ODistributedException && e.getCause() instanceof IOException) {
            // CONNECTION ERROR: REMOVE THE CONNECTION
            reason = e.getCause().getMessage();
            closeRemoteServer(node);

          } else if (e instanceof OSecurityAccessException) {
            // THE CONNECTION COULD BE STALE, CREATE A NEW ONE AND RETRY
            closeRemoteServer(node);
            try {
              final ORemoteServerController remoteServer = getRemoteServer(node);
              remoteServer.sendRequest(iRequest);
              continue;

            } catch (Exception ex) {
              // IGNORE IT BECAUSE MANAGED BELOW
            }
          }

          if (!isNodeAvailable(node))
            // NODE IS NOT AVAILABLE
            ODistributedServerLog.debug(
                this,
                this.nodeName,
                node,
                OUT,
                "Error on sending distributed request %s. The target node is not available. Active nodes: %s",
                e,
                iRequest,
                getAvailableNodeNames(databaseName));
          else
            ODistributedServerLog.error(
                this,
                this.nodeName,
                node,
                OUT,
                "Error on sending distributed request %s (err=%s). Active nodes: %s",
                iRequest,
                reason,
                getAvailableNodeNames(databaseName));
        }
      }

      if (currentResponseMgr.getExpectedNodes().isEmpty())
        // NO SERVER TO SEND A MESSAGE
        throw new ODistributedException(
            "No server active for distributed request ("
                + iRequest
                + ") against database '"
                + databaseName
                + (iClusterNames != null ? "." + iClusterNames : "")
                + "' to nodes "
                + iNodes);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(
            this, this.nodeName, iNodes.toString(), OUT, "Sent request %s", iRequest);

      if (databaseName != null) {
        ODistributedDatabaseImpl shared = getMessageService().getDatabase(databaseName);
        if (shared != null) {
          shared.incSentRequest();
        }
      }

      if (iExecutionMode == ODistributedRequest.EXECUTION_MODE.RESPONSE)
        return waitForResponse(iRequest, currentResponseMgr);

      return null;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      String names = iClusterNames != null ? "." + iClusterNames : "";
      throw OException.wrapException(
          new ODistributedException(
              "Error on executing distributed request ("
                  + iRequest
                  + ") against database '"
                  + this.nodeName
                  + names
                  + "' to nodes "
                  + iNodes),
          e);
    }
  }

  protected ODistributedResponse waitForResponse(
      final ODistributedRequest iRequest, final ODistributedResponseManager currentResponseMgr)
      throws InterruptedException {
    final long beginTime = System.currentTimeMillis();

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (QUORUM)
    if (!currentResponseMgr.waitForSynchronousResponses()) {
      final long elapsed = System.currentTimeMillis() - beginTime;

      if (elapsed > currentResponseMgr.getSynchTimeout()) {

        ODistributedServerLog.warn(
            this,
            this.nodeName,
            null,
            DIRECTION.IN,
            "Timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=(%s)",
            elapsed,
            currentResponseMgr.getExpectedNodes(),
            currentResponseMgr.getRespondingNodes(),
            iRequest);
      }
    }

    return currentResponseMgr.getFinalResponse();
  }

  protected int calculateQuorum(
      final OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType,
      Collection<String> clusterNames,
      final ODistributedConfiguration cfg,
      final int totalServers,
      final int totalMasterServers,
      int onlineMasters,
      final boolean checkNodesAreOnline,
      final String localNodeName) {

    int quorum = 1;

    if (clusterNames == null || clusterNames.isEmpty()) {
      clusterNames = new ArrayList<String>(1);
      clusterNames.add(null);
    }

    int totalServerInQuorum = totalServers;
    for (String cluster : clusterNames) {
      int clusterQuorum = 0;
      switch (quorumType) {
        case NONE:
          // IGNORE IT
          break;
        case READ:
          if (cfg != null) {
            clusterQuorum = cfg.getReadQuorum(cluster, totalServers, localNodeName);
          } else {
            clusterQuorum = 1;
          }
          break;
        case WRITE:
          if (cfg != null) {
            clusterQuorum = cfg.getWriteQuorum(cluster, totalMasterServers, localNodeName);
            totalServerInQuorum = totalMasterServers;
          } else {
            clusterQuorum = totalMasterServers / 2 + 1;
            totalServerInQuorum = totalMasterServers;
          }
          break;
        case WRITE_ALL_MASTERS:
          if (cfg != null) {
            int cfgQuorum = cfg.getWriteQuorum(cluster, totalMasterServers, localNodeName);
            clusterQuorum = Math.max(cfgQuorum, onlineMasters);
          } else {
            clusterQuorum = totalMasterServers;
            totalServerInQuorum = totalMasterServers;
          }
          break;
        case ALL:
          clusterQuorum = totalServers;
          break;
      }

      quorum = Math.max(quorum, clusterQuorum);
    }

    if (quorum < 0) quorum = 0;

    if (checkNodesAreOnline && quorum > totalServerInQuorum)
      throw new ODistributedException(
          "Quorum ("
              + quorum
              + ") cannot be reached on server '"
              + localNodeName
              + "' database '"
              + this.nodeName
              + "' because it is major than available nodes ("
              + totalServerInQuorum
              + ")");

    return quorum;
  }

  private long adjustTimeoutWithLatency(
      final Collection<String> iNodes, final long timeout, final ODistributedRequestId requestId) {
    long delta = 0;
    if (iNodes != null)
      for (String n : iNodes) {
        // UPDATE THE TIMEOUT WITH THE CURRENT SERVER LATENCY
        final long l = messageService.getCurrentLatency(n);
        delta = Math.max(delta, l);
      }

    if (delta > 500)
      ODistributedServerLog.debug(
          this,
          this.nodeName,
          iNodes.toString(),
          OUT,
          "Adjusted timeouts by adding +%dms because this is the maximum latency recorded against servers %s (reqId=%s)",
          delta,
          iNodes,
          requestId);

    return timeout + delta;
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      final Collection<String> iClusterNames,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult) {
    return send2Nodes(
        iRequest,
        iClusterNames,
        iNodes,
        iExecutionMode,
        localResult,
        (iRequest1,
            iNodes1,
            task,
            nodesConcurToTheQuorum,
            availableNodes,
            expectedResponses,
            quorum,
            groupByResponse,
            waitLocalNode) -> {
          return new ODistributedResponseManagerImpl(
              this,
              iRequest,
              iNodes,
              nodesConcurToTheQuorum,
              expectedResponses,
              quorum,
              waitLocalNode,
              adjustTimeoutWithLatency(
                  iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
              adjustTimeoutWithLatency(
                  iNodes, task.getTotalTimeout(availableNodes), iRequest.getId()),
              groupByResponse);
        });
  }

  protected boolean waitForLocalNode(
      final ODistributedConfiguration cfg,
      final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(this.nodeName)) {
      if (cfg != null) {
        if (iClusterNames == null || iClusterNames.isEmpty()) {
          // DEFAULT CLUSTER (*)
          if (cfg.isReadYourWrites(null)) waitLocalNode = true;
        } else {
          // BROWSE FOR ALL CLUSTER TO GET THE FIRST 'waitLocalNode'
          for (String clName : iClusterNames) {
            if (cfg.isReadYourWrites(clName)) {
              waitLocalNode = true;
              break;
            }
          }
        }
      } else {
        waitLocalNode = true;
      }
    }
    return waitLocalNode;
  }

  @Override
  public void executeOnLocalNodeFromRemote(ODistributedRequest request) {
    Object response = executeOnLocalNode(request.getId(), request.getTask(), null);
    ODistributedDatabaseImpl.sendResponseBack(this, this, request.getId(), response);
  }

  /** Executes the request on local node. In case of error returns the Exception itself */
  @Override
  public Object executeOnLocalNode(
      final ODistributedRequestId reqId,
      final ORemoteTask task,
      final ODatabaseDocumentInternal database) {

    final ODistributedPlugin manager = this;

    return OScenarioThreadLocal.executeAsDistributed(
        new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            try {
              final Object result = task.execute(reqId, serverInstance, manager, database);

              if (result instanceof Throwable && !(result instanceof OException))
                // EXCEPTION
                ODistributedServerLog.debug(
                    this,
                    nodeName,
                    getNodeNameById(reqId.getNodeId()),
                    DIRECTION.IN,
                    "Error on executing request %d (%s) on local node: ",
                    (Throwable) result,
                    reqId,
                    task);

              return result;

            } catch (InterruptedException e) {
              // IGNORE IT
              ODistributedServerLog.debug(
                  this,
                  nodeName,
                  getNodeNameById(reqId.getNodeId()),
                  DIRECTION.IN,
                  "Interrupted execution on executing distributed request %s on local node: %s",
                  e,
                  reqId,
                  task);
              return e;

            } catch (Exception e) {
              if (!(e instanceof OException))
                ODistributedServerLog.error(
                    this,
                    nodeName,
                    getNodeNameById(reqId.getNodeId()),
                    DIRECTION.IN,
                    "Error on executing distributed request %s on local node: %s",
                    e,
                    reqId,
                    task);

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
  public void onLocalNodeConfigurationRequest(final ODocument iConfiguration) {}

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
    if (((ODatabaseDocumentInternal) iDatabase).isLocalEnv()) return;

    if (isOffline() && getNodeStatus() != NODE_STATUS.STARTING) return;

    // RUN ONLY IN NON-DISTRIBUTED MODE
    if (!isRelatedToLocalServer(iDatabase)) return;

    if (messageService == null || messageService.getDatabase(iDatabase.getName()) == null)
      // NOT INITIALIZED YET
      return;

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());

    installClustersOfClass(iDatabase, iClass, cfg.modify());
  }

  public void onCreateView(final ODatabaseInternal iDatabase, final OView view) {
    // TODO implement this!
    OLogManager.instance()
        .error(this, "Implement ODistributedAbstractPlugin.onCreateView()!!!", null);
  }

  @SuppressWarnings("unchecked")
  public ODocument getStats() {
    final ODocument doc = new ODocument();

    final Map<String, HashMap<String, Object>> nodes =
        new HashMap<String, HashMap<String, Object>>();
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
  public Throwable convertException(final Throwable original) {
    if (!Orient.instance().isActive() || isOffline())
      return new OOfflineNodeException("Server " + nodeName + " is offline");

    if (original instanceof HazelcastException
        || original instanceof HazelcastInstanceNotActiveException)
      return new IOException(
          "Hazelcast wrapped exception: " + original.getMessage(), original.getCause());

    if (original instanceof IllegalMonitorStateException)
      // THIS IS RAISED WHEN INTERNAL LOCKING IS BROKEN BECAUSE HARD SHUTDOWN
      return new IOException(
          "Illegal monitor state: " + original.getMessage(), original.getCause());

    return original;
  }

  @Override
  public List<String> getOnlineNodes(String iDatabaseName) {
    return clusterManager.getOnlineNodes(iDatabaseName);
  }

  @Override
  public void reassignClustersOwnership(
      final String iNode,
      final String databaseName,
      final OModifiableDistributedConfiguration cfg,
      final boolean canCreateNewClusters) {

    // REASSIGN CLUSTERS WITHOUT AN OWNER, AVOIDING TO REBALANCE EXISTENT
    executeInDistributedDatabaseLock(
        databaseName,
        20000,
        cfg,
        new OCallable<Boolean, OModifiableDistributedConfiguration>() {
          @Override
          public Boolean call(final OModifiableDistributedConfiguration cfg) {
            rebalanceClusterOwnership(iNode, databaseName, cfg, canCreateNewClusters);
            return null;
          }
        });
  }

  @Override
  public boolean isNodeAvailable(String iNodeName, String databaseName) {
    return clusterManager.isNodeAvailable(iNodeName, databaseName);
  }

  @Override
  public boolean isNodeOnline(String iNodeName, String databaseName) {
    return clusterManager.isNodeOnline(iNodeName, databaseName);
  }

  @Override
  public boolean isNodeStatusEqualsTo(
      final String iNodeName, final String iDatabaseName, final DB_STATUS... statuses) {
    final DB_STATUS s = getDatabaseStatus(iNodeName, iDatabaseName);
    for (DB_STATUS st : statuses) {
      if (s == st) return true;
    }
    return false;
  }

  @Override
  public boolean isNodeAvailable(String iNodeName) {
    return clusterManager.isNodeAvailable(iNodeName);
  }

  @Override
  public Set<String> getAvailableNodeNames(String databaseName) {
    return clusterManager.getAvailableNodeNames(databaseName);
  }

  public boolean isOffline() {
    return getNodeStatus() != NODE_STATUS.ONLINE;
  }

  @Override
  public int getLocalNodeId() {
    return clusterManager.getLocalNodeId();
  }

  /** Returns the nodes with the requested status. */
  @Override
  public int getNodesWithStatus(
      final Collection<String> iNodes, final String databaseName, final DB_STATUS... statuses) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();

      if (!isNodeStatusEqualsTo(node, databaseName, statuses)) it.remove();
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
        Thread.currentThread().interrupt();
        throw OException.wrapException(
            new OOfflineNodeException("Message Service is not available"), e);
      }
    return messageService;
  }

  @Override
  public int getTotalNodes(final String iDatabaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabaseName);
    if (cfg != null) return cfg.getAllConfiguredServers().size();
    return 0;
  }

  @Override
  public int getAvailableNodes(String iDatabaseName) {
    return clusterManager.getAvailableNodes(iDatabaseName);
  }

  @Override
  public int getAvailableNodes(Collection<String> iNodes, String databaseName) {
    return clusterManager.getAvailableNodes(iNodes, databaseName);
  }

  @Override
  public boolean installDatabase(
      final boolean iStartup,
      final String databaseName,
      final boolean forceDeployment,
      final boolean tryWithDeltaFirst) {
    if (getDatabaseStatus(getLocalNodeName(), databaseName) == DB_STATUS.OFFLINE)
      // OFFLINE: AVOID TO INSTALL IT
      return false;

    if (databaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
      // DON'T REPLICATE SYSTEM BECAUSE IS DIFFERENT AND PER SERVER
      return false;

    if (installingDatabases.contains(databaseName)) {
      return false;
    }

    final ODistributedDatabaseImpl distrDatabase = messageService.registerDatabase(databaseName);

    try {
      installingDatabases.add(databaseName);
      return executeInDistributedDatabaseLock(
          databaseName,
          20000,
          null,
          new OCallable<Boolean, OModifiableDistributedConfiguration>() {
            @Override
            public Boolean call(OModifiableDistributedConfiguration cfg) {

              distrDatabase.checkNodeInConfiguration(nodeName, cfg);

              // GET ALL THE OTHER SERVERS
              final Collection<String> nodes = cfg.getServers(null, nodeName);
              getAvailableNodes(nodes, databaseName);
              if (nodes.size() == 0) {
                ODistributedServerLog.error(
                    this,
                    nodeName,
                    null,
                    DIRECTION.NONE,
                    "Cannot install database '%s' on local node, because no servers are available",
                    databaseName);
                return false;
              }

              ODistributedServerLog.info(
                  this,
                  nodeName,
                  null,
                  DIRECTION.NONE,
                  "Current node is a %s for database '%s'",
                  cfg.getServerRole(nodeName),
                  databaseName);

              if (!forceDeployment
                  && getDatabaseStatus(getLocalNodeName(), databaseName) == DB_STATUS.ONLINE)
                return false;

              // INIT STORAGE + UPDATE LOCAL FILE ONLY
              distrDatabase.setDistributedConfiguration(cfg);

              // DISCARD MESSAGES DURING THE REQUEST OF DATABASE INSTALLATION
              distrDatabase.suspend();

              final Boolean deploy = forceDeployment ? Boolean.TRUE : (Boolean) cfg.isAutoDeploy();

              boolean databaseInstalled;

              try {

                // CREATE THE DISTRIBUTED QUEUE
                // TODO: This should check also but can't do it now
                // storage.getLastMetadata().isPresent();
                if (!distrDatabase.exists()) {

                  if (deploy == null || !deploy) {
                    // NO AUTO DEPLOY
                    ODistributedServerLog.debug(
                        this,
                        nodeName,
                        null,
                        DIRECTION.NONE,
                        "Skipping download of database '%s' from the cluster because autoDeploy=false",
                        databaseName);

                    distrDatabase.setOnline();
                    distrDatabase.resume();
                    return false;
                  }

                  // FIRST TIME, ASK FOR FULL REPLICA
                  databaseInstalled =
                      requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);

                } else {
                  if (tryWithDeltaFirst) {
                    try {

                      // TRY WITH DELTA SYNC
                      // databaseInstalled = requestDatabaseDelta(distrDatabase, databaseName, cfg);
                      databaseInstalled = requestNewDatabaseDelta(distrDatabase, databaseName, cfg);

                    } catch (ODistributedDatabaseDeltaSyncException e) {
                      if (deploy == null || !deploy) {
                        // NO AUTO DEPLOY
                        ODistributedServerLog.debug(
                            this,
                            nodeName,
                            null,
                            DIRECTION.NONE,
                            "Skipping download of the entire database '%s' from the cluster because autoDeploy=false",
                            databaseName);

                        distrDatabase.setOnline();
                        distrDatabase.resume();
                        return false;
                      }

                      databaseInstalled =
                          requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);
                    }
                  } else
                    // SKIP DELTA AND EXECUTE FULL BACKUP
                    databaseInstalled =
                        requestFullDatabase(distrDatabase, databaseName, iStartup, cfg);
                }

                if (!databaseInstalled) {
                  setDatabaseStatus(getLocalNodeName(), databaseName, DB_STATUS.NOT_AVAILABLE);
                }

              } catch (ODatabaseIsOldException e) {
                // CURRENT DATABASE IS NEWER, SET ALL OTHER DATABASES AS NOT_AVAILABLE TO FORCE THEM
                // TO ASK FOR THE CURRENT DATABASE
                distrDatabase.setOnline();

                ODistributedServerLog.info(
                    this,
                    nodeName,
                    null,
                    DIRECTION.OUT,
                    "Current copy of database '%s' is newer than the copy present in the cluster. Use the local copy and force other nodes to download this",
                    databaseName);

                databaseInstalled = true;
                distrDatabase.resume();
              } catch (RuntimeException e) {
                // UNLOCK ACCEPTING REQUESTS EVEN IN CASE OF ERROR.
                distrDatabase.resume();
                throw e;
              }

              return databaseInstalled;
            }
          });
    } finally {
      installingDatabases.remove(databaseName);
    }
  }

  private boolean requestNewDatabaseDelta(
      ODistributedDatabaseImpl distrDatabase,
      String databaseName,
      OModifiableDistributedConfiguration cfg) {
    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, nodeName);
    getAvailableNodes(nodes, databaseName);
    if (nodes.size() == 0) {
      return false;
    }

    ODistributedServerLog.warn(
        this,
        nodeName,
        nodes.toString(),
        DIRECTION.OUT,
        "requesting delta database sync for '%s' on local server...",
        databaseName);

    boolean databaseInstalledCorrectly = false;

    for (String targetNode : nodes) {

      if (!isNodeOnline(targetNode, databaseName)) {
        continue;
      }
      OTxMetadataHolder metadata;
      try (ODatabaseDocumentInternal inst = distrDatabase.getDatabaseInstance()) {
        Optional<byte[]> read = ((OAbstractPaginatedStorage) inst.getStorage()).getLastMetadata();
        if (read.isPresent()) {
          metadata = OTxMetadataHolderImpl.read(read.get());
        } else {
          throw new ODistributedDatabaseDeltaSyncException("Trigger full sync");
        }
      }
      final OSyncDatabaseNewDeltaTask deployTask =
          new OSyncDatabaseNewDeltaTask(metadata.getStatus());

      final List<String> targetNodes = new ArrayList<String>(1);
      targetNodes.add(targetNode);
      try {
        final ODistributedResponse response =
            sendRequest(
                databaseName,
                null,
                targetNodes,
                deployTask,
                getNextMessageIdCounter(),
                ODistributedRequest.EXECUTION_MODE.RESPONSE,
                null);

        if (response == null)
          throw new ODistributedDatabaseDeltaSyncException("Error requesting delta sync");

        databaseInstalledCorrectly =
            installResponseNewDeltaSync(
                distrDatabase,
                databaseName,
                cfg,
                targetNode,
                (ONewDeltaTaskResponse) response.getPayload());

      } catch (ODistributedDatabaseDeltaSyncException e) {
        // RE-THROW IT
        throw e;
      } catch (Exception e) {
        ODistributedServerLog.error(
            this,
            nodeName,
            targetNode,
            DIRECTION.OUT,
            "Error on asking delta backup of database '%s' (err=%s)",
            databaseName,
            e.getMessage());
        throw OException.wrapException(new ODistributedDatabaseDeltaSyncException(e.toString()), e);
      }

      if (databaseInstalledCorrectly) {
        distrDatabase.resume();
        return true;
      }
    }

    throw new ODistributedDatabaseDeltaSyncException("Requested database delta sync error");
  }

  protected boolean requestFullDatabase(
      final ODistributedDatabaseImpl distrDatabase,
      final String databaseName,
      final boolean backupDatabase,
      final OModifiableDistributedConfiguration cfg) {
    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        DIRECTION.NONE,
        "Requesting full sync for database '%s'...",
        databaseName);

    for (int retry = 0; retry < DEPLOY_DB_MAX_RETRIES; ++retry) {
      // ASK DATABASE TO THE FIRST NODE, THE FIRST ATTEMPT, OTHERWISE ASK TO EVERYONE
      if (requestDatabaseFullSync(distrDatabase, backupDatabase, databaseName, retry > 0, cfg))
        // DEPLOYED
        return true;
      try {
        Thread.sleep(
            serverInstance.getContextConfiguration().getValueAsLong(DISTRIBUTED_MAX_STARTUP_DELAY));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    // RETRY COUNTER EXCEED
    return false;
  }

  private boolean installResponseNewDeltaSync(
      ODistributedDatabaseImpl distrDatabase,
      String databaseName,
      OModifiableDistributedConfiguration cfg,
      String targetNode,
      ONewDeltaTaskResponse results) {
    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;
    boolean databaseInstalledCorrectly = false;
    // EXTRACT THE REAL RESULT
    if (results.getResponseType() == ONewDeltaTaskResponse.ResponseType.CHUNK) {
      ODistributedDatabaseChunk firstChunk = results.getChunk().get();
      try {

        OSyncReceiver receiver =
            new OSyncReceiver(this, databaseName, firstChunk, targetNode, dbPath);
        receiver.spawnReceiverThread();
        receiver.getStarted().await();

        executeInDistributedDatabaseLock(
            databaseName,
            20000,
            cfg,
            (OCallable<Void, OModifiableDistributedConfiguration>)
                cfg1 -> {
                  try (InputStream in = receiver.getInputStream()) {
                    new ONewDeltaSyncImporter()
                        .importDelta(serverInstance, databaseName, in, targetNode);
                  } catch (IOException e) {
                    throw OException.wrapException(
                        new OIOException("Error on distributed sync of database"), e);
                  }
                  return null;
                });

        distrDatabase.setOnline();

        try {
          rebalanceClusterOwnership(nodeName, databaseName, cfg, false);
        } catch (Exception e) {
          // HANDLE IT AS WARNING
          ODistributedServerLog.warn(
              this,
              nodeName,
              null,
              DIRECTION.NONE,
              "Error on re-balancing the cluster for database '%s'",
              e,
              databaseName);
          // NOT CRITICAL, CONTINUE
        }

        ODistributedServerLog.info(
            this,
            nodeName,
            targetNode,
            DIRECTION.IN,
            "Installed delta of database '%s'",
            databaseName);

        // DATABASE INSTALLED CORRECTLY
        databaseInstalledCorrectly = true;
      } catch (OException | InterruptedException e) {
        OLogManager.instance().error(this, "Error installing database from network", e);
        databaseInstalledCorrectly = false;
      }
    } else if (results.getResponseType() == ONewDeltaTaskResponse.ResponseType.FULL_SYNC) {
      throw new ODistributedDatabaseDeltaSyncException("Full sync required");
    } else if (results.getResponseType() == ONewDeltaTaskResponse.ResponseType.NO_CHANGES) {
      distrDatabase.setOnline();
      return true;
    }
    return databaseInstalledCorrectly;
  }

  protected boolean requestDatabaseFullSync(
      final ODistributedDatabaseImpl distrDatabase,
      final boolean backupDatabase,
      final String databaseName,
      final boolean iAskToAllNodes,
      final OModifiableDistributedConfiguration cfg) {
    // GET ALL THE OTHER SERVERS
    Collection<String> nodes = cfg.getServers(null, nodeName);
    if (nodes.isEmpty()) {
      ODistributedServerLog.warn(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Cannot request full deploy of database '%s' because there are no nodes available with such database",
          databaseName);
      return false;
    }

    final List<String> selectedNodes = new ArrayList<String>();

    if (!iAskToAllNodes) {
      // GET THE FIRST ONE IN BACKUP STATUS. THIS FORCES TO HAVE ONE NODE TO DO BACKUP SAVING
      // RESOURCES IN CASE BACKUP IS STILL
      // VALID FOR FURTHER NODES
      for (String n : nodes) {
        if (isNodeStatusEqualsTo(n, databaseName, DB_STATUS.BACKUP)) {
          // SERVER ALREADY IN BACKUP: USE IT
          selectedNodes.add(n);
          break;
        }
      }

      if (selectedNodes.isEmpty()) {
        // GET THE FIRST ONE TO ASK FOR DATABASE. THIS FORCES TO HAVE ONE NODE TO DO BACKUP SAVING
        // RESOURCES IN CASE BACKUP IS STILL
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
    Iterator<String> iter = selectedNodes.iterator();
    while (iter.hasNext()) {
      if (!isNodeAvailable(iter.next())) iter.remove();
    }

    ODistributedServerLog.info(
        this,
        nodeName,
        selectedNodes.toString(),
        DIRECTION.OUT,
        "Requesting deploy of database '%s' on local server...",
        databaseName);
    for (String noteToSend : selectedNodes) {
      OSyncDatabaseTask deployTask = new OSyncDatabaseTask();
      List<String> singleNode = new ArrayList<>();
      singleNode.add(noteToSend);
      final Map<String, Object> results =
          (Map<String, Object>)
              sendRequest(
                      databaseName,
                      null,
                      singleNode,
                      deployTask,
                      getNextMessageIdCounter(),
                      ODistributedRequest.EXECUTION_MODE.RESPONSE,
                      null)
                  .getPayload();

      if (results == null) {
        ODistributedServerLog.error(
            this,
            nodeName,
            selectedNodes.toString(),
            DIRECTION.IN,
            "Timeout waiting the sync database please set the `distributed.deployDbTaskTimeout` to appropriate value");
        setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);
        return false;
      }
      ODistributedServerLog.debug(
          this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Deploy returned: %s", results);

      final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

      // EXTRACT THE REAL RESULT
      for (Map.Entry<String, Object> r : results.entrySet()) {
        final Object value = r.getValue();

        if (value instanceof Boolean) {
          distrDatabase.setOnline();
          continue;
        } else if (value instanceof ODatabaseIsOldException) {

          // MANAGE THIS EXCEPTION AT UPPER LEVEL
          throw (ODatabaseIsOldException) value;

        } else if (value instanceof Throwable) {
          ODistributedServerLog.error(
              this,
              nodeName,
              r.getKey(),
              DIRECTION.IN,
              "Error on installing database '%s' in %s",
              (Throwable) value,
              databaseName,
              dbPath);

          setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);

          if (value instanceof ODistributedException) throw (ODistributedException) value;

        } else if (value instanceof ODistributedDatabaseChunk) {

          // DISABLED BECAUSE MOMENTUM IS NOT RELIABLE YET
          // distrDatabase.filterBeforeThisMomentum(((ODistributedDatabaseChunk)
          // value).getMomentum());
          final File uniqueClustersBackupDirectory =
              getClusterOwnedExclusivelyByCurrentNode(dbPath, databaseName);
          if (backupDatabase) backupCurrentDatabase(databaseName);

          try {
            installDatabaseFromNetwork(
                dbPath,
                databaseName,
                distrDatabase,
                r.getKey(),
                (ODistributedDatabaseChunk) value,
                uniqueClustersBackupDirectory,
                cfg);
          } catch (OException e) {
            OLogManager.instance().error(this, "Error installing database from network", e);
            return false;
          }

          distrDatabase.resume();

          return true;

        } else throw new IllegalArgumentException("Type " + value + " not supported");
      }
    }

    throw new ODistributedException(
        "No response received from remote nodes for auto-deploy of database '"
            + databaseName
            + "'");
  }

  protected File getClusterOwnedExclusivelyByCurrentNode(
      final String dbPath, final String iDatabaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabaseName);

    final HashSet<String> clusters = new HashSet<String>();

    for (String clName : cfg.getClusterNames()) {
      final List<String> servers = cfg.getServers(clName, null);
      if (servers != null) {
        if (servers.size() == 1 && servers.get(0).equals(getLocalNodeName())) clusters.add(clName);
      }
    }

    if (!clusters.isEmpty()) {
      // COPY FILES IN A SAFE LOCATION TO BE REPLACED AFTER THE DATABASE RESTORE

      // MOVE DIRECTORY TO ../backup/databases/<db-name>
      final String backupDirectory = Orient.instance().getHomePath() + "/temp/db_" + iDatabaseName;
      final File backupFullPath = new File(backupDirectory);
      if (backupFullPath.exists()) OFileUtils.deleteRecursively(backupFullPath);
      else backupFullPath.mkdirs();

      // MOVE THE DATABASE ON CURRENT NODE
      ODistributedServerLog.warn(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Saving clusters %s to directory '%s' to be replaced after distributed full backup...",
          clusters,
          backupFullPath);

      for (String clName : clusters) {
        // MOVE .PCL and .PCM FILES
        {
          final File oldFile = new File(dbPath + "/" + clName + OPaginatedCluster.DEF_EXTENSION);
          final File newFile =
              new File(backupFullPath + "/" + clName + OPaginatedCluster.DEF_EXTENSION);

          if (oldFile.exists()) {
            if (!oldFile.renameTo(newFile)) {
              ODistributedServerLog.error(
                  this,
                  nodeName,
                  null,
                  DIRECTION.NONE,
                  "Cannot make a safe copy of exclusive clusters. Error on moving file %s -> %s: restore of database '%s' has been aborted because unsafe",
                  oldFile,
                  newFile,
                  iDatabaseName);
              throw new ODistributedException("Cannot make a safe copy of exclusive clusters");
            }
          }
        }

        {
          final File oldFile = new File(dbPath + "/" + clName + OClusterPositionMap.DEF_EXTENSION);
          final File newFile =
              new File(backupFullPath + "/" + clName + OClusterPositionMap.DEF_EXTENSION);

          if (oldFile.exists()) {
            if (!oldFile.renameTo(newFile)) {
              ODistributedServerLog.error(
                  this,
                  nodeName,
                  null,
                  DIRECTION.NONE,
                  "Cannot make a safe copy of exclusive clusters. Error on moving file %s -> %s: restore of database '%s' has been aborted because unsafe",
                  oldFile,
                  newFile,
                  iDatabaseName);
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
    serverInstance.getDatabases().forceDatabaseClose(iDatabaseName);

    // move directory to ../backup/databases/<db-name>
    final String backupDirectory =
        serverInstance
            .getContextConfiguration()
            .getValueAsString(OGlobalConfiguration.DISTRIBUTED_BACKUP_DIRECTORY);

    if (backupDirectory == null || OIOUtils.getStringContent(backupDirectory).trim().isEmpty())
      // skip backup
      return;

    String backupPath;

    if (backupDirectory.startsWith("/")) backupPath = backupDirectory;
    else {
      if (backupDirectory.startsWith("../")) {
        backupPath =
            new File(serverInstance.getDatabaseDirectory()).getParent()
                + backupDirectory.substring("..".length());
      } else {
        backupPath = serverInstance.getDatabaseDirectory() + backupDirectory;
      }
    }

    if (!backupPath.endsWith("/")) {
      backupPath += "/";
    }

    backupPath += iDatabaseName;

    final String dbpath = serverInstance.getDatabaseDirectory() + iDatabaseName;
    final File backupFullPath = new File(backupPath);
    try {
      if (backupFullPath.exists()) {
        deleteRecursively(backupFullPath);
      }

      Files.createDirectories(backupFullPath.toPath());

      // move the database on current node
      ODistributedServerLog.warn(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Moving existent database '%s' in '%s' to '%s' and get a fresh copy from a remote node...",
          iDatabaseName,
          dbpath,
          backupPath);

      final File oldDirectory = new File(dbpath);
      if (oldDirectory.exists() && oldDirectory.isDirectory()) {
        if (oldDirectory.getCanonicalPath().equals(backupFullPath.getCanonicalPath())) {
          throw new ODistributedException(
              String.format(
                  "Backup folder configured as same of database folder:'%s'",
                  oldDirectory.getAbsolutePath()));
        }
        try {
          try {
            Files.move(
                oldDirectory.toPath(), backupFullPath.toPath(), StandardCopyOption.ATOMIC_MOVE);
          } catch (AtomicMoveNotSupportedException e) {
            OLogManager.instance()
                .errorNoDb(
                    this,
                    "Atomic moves not supported during database backup, will try not atomic move",
                    null);
            if (backupFullPath.exists()) {
              deleteRecursively(backupFullPath);
            }
            Files.createDirectories(backupFullPath.toPath());

            Files.move(oldDirectory.toPath(), Paths.get(backupPath, oldDirectory.getName()));
          }
        } catch (DirectoryNotEmptyException e) {
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "File rename not supported during database backup, will try coping files",
                  null);
          if (backupFullPath.exists()) {
            deleteRecursively(backupFullPath);
          }
          Files.createDirectories(backupFullPath.toPath());
          try {
            OFileUtils.copyDirectory(
                oldDirectory, Paths.get(backupPath, oldDirectory.getName()).toFile());
            deleteRecursively(oldDirectory);
          } catch (IOException ioe) {
            OLogManager.instance().errorNoDb(this, "Error moving old database removing it", ioe);
            deleteRecursively(oldDirectory);
          }
        }
      }
    } catch (IOException e) {
      ODistributedServerLog.warn(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Error on moving existent database '%s' located in '%s' to '%s' (error=%s).",
          e,
          iDatabaseName,
          dbpath,
          backupFullPath,
          e);
    }
  }

  private void deleteRecursively(final File path) throws IOException {
    // delete directory and its content
    Files.walkFileTree(
        path.toPath(),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  /** Installs a database from the network. */
  protected void installDatabaseFromNetwork(
      final String dbPath,
      final String databaseName,
      final ODistributedDatabaseImpl distrDatabase,
      final String iNode,
      final ODistributedDatabaseChunk firstChunk,
      final File uniqueClustersBackupDirectory,
      final OModifiableDistributedConfiguration cfg) {

    final String localNodeName = nodeName;

    OSyncReceiver receiver = new OSyncReceiver(this, databaseName, firstChunk, iNode, dbPath);
    receiver.spawnReceiverThread();

    installDatabaseOnLocalNode(
        databaseName,
        dbPath,
        iNode,
        uniqueClustersBackupDirectory,
        cfg,
        firstChunk.incremental,
        receiver);
    receiver.close();

    distrDatabase.setOnline();

    // ASK FOR INDIVIDUAL CLUSTERS IN CASE OF SHARDING AND NO LOCAL COPY
    final Set<String> localManagedClusters = cfg.getClustersOnServer(localNodeName);
    final Set<String> sourceNodeClusters = cfg.getClustersOnServer(iNode);
    localManagedClusters.removeAll(sourceNodeClusters);

    final HashSet<String> toSynchClusters = new HashSet<String>();
    for (String cl : localManagedClusters) {
      // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
      final List<String> servers = cfg.getServers(cl, localNodeName);
      getAvailableNodes(servers, databaseName);

      if (!servers.isEmpty()) toSynchClusters.add(cl);
    }

    // SYNC ALL THE CLUSTERS
    for (String cl : toSynchClusters) {
      // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
      OCommandExecutorSQLHASyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
    }

    try {
      rebalanceClusterOwnership(nodeName, databaseName, cfg, false);
    } catch (Exception e) {
      // HANDLE IT AS WARNING
      ODistributedServerLog.warn(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Error on re-balancing the cluster for database '%s'",
          e,
          databaseName);
      // NOT CRITICAL, CONTINUE
    }
  }

  @Override
  public ORemoteTaskFactoryManager getTaskFactoryManager() {
    return taskFactoryManager;
  }

  @Override
  public Set<String> getActiveServers() {
    return clusterManager.getActiveServers();
  }

  /** Guarantees that each class has own master cluster. */
  public boolean installClustersOfClass(
      final ODatabaseInternal iDatabase,
      final OClass iClass,
      OModifiableDistributedConfiguration cfg) {

    final String databaseName = iDatabase.getName();
    if (iClass.isAbstract()) return false;

    // INIT THE DATABASE IF NEEDED
    getMessageService().registerDatabase(databaseName);

    return executeInDistributedDatabaseLock(
        databaseName,
        20000,
        cfg,
        new OCallable<Boolean, OModifiableDistributedConfiguration>() {
          @Override
          public Boolean call(final OModifiableDistributedConfiguration lastCfg) {
            final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

            final List<String> cluster2Create =
                clusterAssignmentStrategy.assignClusterOwnershipOfClass(
                    iDatabase, lastCfg, iClass, availableNodes, true);

            final Map<OClass, List<String>> cluster2CreateMap =
                new HashMap<OClass, List<String>>(1);
            cluster2CreateMap.put(iClass, cluster2Create);

            createClusters(iDatabase, cluster2CreateMap, lastCfg);
            return true;
          }
        });
  }

  private void createClusters(
      final ODatabaseInternal iDatabase,
      final Map<OClass, List<String>> cluster2Create,
      OModifiableDistributedConfiguration cfg) {
    if (cluster2Create.isEmpty()) return;

    executeInDistributedDatabaseLock(
        iDatabase.getName(),
        20000,
        cfg,
        new OCallable<Object, OModifiableDistributedConfiguration>() {
          @Override
          public Object call(final OModifiableDistributedConfiguration cfg) {

            // UPDATE LAST CFG BEFORE TO MODIFY THE CLUSTERS
            updateCachedDatabaseConfiguration(iDatabase.getName(), cfg);

            for (Map.Entry<OClass, List<String>> entry : cluster2Create.entrySet()) {
              final OClass clazz = entry.getKey();

              // SAVE CONFIGURATION LOCALLY TO ALLOW THE CREATION OF THE CLUSTERS IF ANY
              // CHECK OWNER AFTER RE-BALANCE AND CREATE NEW CLUSTERS IF NEEDED
              for (final String newClusterName : entry.getValue()) {

                ODistributedServerLog.info(
                    this,
                    getLocalNodeName(),
                    null,
                    ODistributedServerLog.DIRECTION.NONE,
                    "Class '%s', creation of new local cluster '%s' (id=%d)",
                    clazz,
                    newClusterName,
                    iDatabase.getClusterIdByName(newClusterName));

                OScenarioThreadLocal.executeAsDefault(
                    new Callable<Object>() {
                      @Override
                      public Object call() throws Exception {
                        try {
                          clazz.addCluster(newClusterName);
                        } catch (Exception e) {
                          if (!iDatabase.getClusterNames().contains(newClusterName)) {
                            // NOT CREATED
                            ODistributedServerLog.error(
                                this,
                                getLocalNodeName(),
                                null,
                                ODistributedServerLog.DIRECTION.NONE,
                                "Error on creating cluster '%s' in class '%s': ",
                                newClusterName,
                                clazz,
                                e);
                            throw OException.wrapException(
                                new ODistributedException(
                                    "Error on creating cluster '"
                                        + newClusterName
                                        + "' in class '"
                                        + clazz
                                        + "'"),
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

  @Override
  public boolean updateCachedDatabaseConfiguration(
      String iDatabaseName, OModifiableDistributedConfiguration cfg) {
    return clusterManager.updateCachedDatabaseConfiguration(iDatabaseName, cfg);
  }

  public void notifyClients(String databaseName) {
    List<String> hosts = new ArrayList<>();
    for (String name : getActiveServers()) {
      ODocument memberConfig = clusterManager.getNodeConfigurationByName(name, true);
      if (memberConfig != null) {
        final String nodeStatus = memberConfig.field("status");

        if (memberConfig != null && !"OFFLINE".equals(nodeStatus)) {
          final Collection<Map<String, Object>> listeners = memberConfig.field("listeners");
          if (listeners != null)
            for (Map<String, Object> listener : listeners) {
              if (listener.get("protocol").equals("ONetworkProtocolBinary")) {
                String url = (String) listener.get("listen");
                hosts.add(url);
              }
            }
        }
      }
    }
    serverInstance.getPushManager().pushDistributedConfig(databaseName, hosts);
  }

  public void onDatabaseEvent(
      final String nodeName, final String databaseName, final DB_STATUS status) {
    notifyClients(databaseName);
    updateLastClusterChange();
    dumpServersStatus();
  }

  public void invokeOnDatabaseStatusChange(
      final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      try {
        l.onDatabaseChangeStatus(iNode, iDatabaseName, iStatus);
      } catch (Exception e) {
        // IGNORE IT
      }
    }
  }

  protected void rebalanceClusterOwnership(
      final String iNode,
      String databaseName,
      final OModifiableDistributedConfiguration cfg,
      final boolean canCreateNewClusters) {
    final ODistributedConfiguration.ROLES role = cfg.getServerRole(iNode);
    if (role != ODistributedConfiguration.ROLES.MASTER)
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return;

    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try (ODatabaseDocumentInternal iDatabase = getServerInstance().openDatabase(databaseName)) {

      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Reassigning ownership of clusters for database %s...",
          iDatabase.getName());

      final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

      iDatabase.activateOnCurrentThread();
      final OSchema schema = iDatabase.getDatabaseOwner().getMetadata().getSchema();

      final Map<OClass, List<String>> cluster2CreateMap = new HashMap<OClass, List<String>>(1);
      for (final OClass clazz : schema.getClasses()) {
        final List<String> cluster2Create =
            clusterAssignmentStrategy.assignClusterOwnershipOfClass(
                iDatabase, cfg, clazz, availableNodes, canCreateNewClusters);

        cluster2CreateMap.put(clazz, cluster2Create);
      }

      if (canCreateNewClusters) createClusters(iDatabase, cluster2CreateMap, cfg);

      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          DIRECTION.NONE,
          "Reassignment of clusters for database '%s' completed (classes=%d)",
          iDatabase.getName(),
          cluster2CreateMap.size());
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }
  }

  protected void assignNodeName() {
    // ORIENTDB_NODE_NAME ENV VARIABLE OR JVM SETTING
    nodeName = OSystemVariableResolver.resolveVariable(NODE_NAME_ENV);

    if (nodeName != null) {
      nodeName = nodeName.trim();
      if (nodeName.isEmpty()) nodeName = null;
    }

    if (nodeName == null) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |         WARNING: FIRST DISTRIBUTED RUN CONFIGURATION          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | This is the first time that the server is running as          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | distributed. Please type the name you want to assign to the   |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | current server node.                                          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |                                                               |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | setting ORIENTDB_NODE_NAME to the server node name to use.    |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.print(OAnsiCode.format("\n$ANSI{yellow Node name [BLANK=auto generate it]: }"));

      OConsoleReader reader = new ODefaultConsoleReader();
      try {
        nodeName = reader.readLine();
      } catch (IOException e) {
      }
      if (nodeName != null) {
        nodeName = nodeName.trim();
        if (nodeName.isEmpty()) nodeName = null;
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
          h.parameters[h.parameters.length - 1] =
              new OServerParameterConfiguration("nodeName", this.nodeName);
        }

        try {
          serverInstance.saveConfiguration();
        } catch (IOException e) {
          throw OException.wrapException(
              new OConfigurationException("Cannot save server configuration"), e);
        }
        break;
      }
    }
  }

  protected void installDatabaseOnLocalNode(
      final String databaseName,
      final String dbPath,
      final String iNode,
      final File uniqueClustersBackupDirectory,
      final OModifiableDistributedConfiguration cfg,
      boolean incremental,
      OSyncReceiver receiver) {
    ODistributedServerLog.info(
        this,
        nodeName,
        iNode,
        DIRECTION.IN,
        "Installing database '%s' to: %s...",
        databaseName,
        dbPath);

    new File(dbPath).mkdirs();
    try {
      receiver.getStarted().await();
    } catch (InterruptedException e) {
      throw OException.wrapException(
          new OInterruptedException("Interrupted waiting receive of sync"), e);
    }

    final ODistributedPlugin me = this;
    executeInDistributedDatabaseLock(
        databaseName,
        20000,
        cfg,
        new OCallable<Void, OModifiableDistributedConfiguration>() {
          @Override
          public Void call(final OModifiableDistributedConfiguration cfg) {
            try {
              if (incremental) {
                OStorage storage =
                    serverInstance
                        .getDatabases()
                        .fullSync(
                            databaseName,
                            receiver.getInputStream(),
                            OrientDBConfig.defaultConfig());
                ODistributedDatabaseImpl distrDatabase = messageService.getDatabase(databaseName);
                distrDatabase.saveDatabaseConfiguration();
                if (uniqueClustersBackupDirectory != null
                    && uniqueClustersBackupDirectory.exists()) {
                  // RESTORE UNIQUE FILES FROM THE BACKUP FOLDERS. THOSE FILES ARE THE CLUSTERS
                  // OWNED EXCLUSIVELY BY CURRENT
                  // NODE THAT WOULD BE LOST IF NOT REPLACED
                  for (File f : uniqueClustersBackupDirectory.listFiles()) {
                    final File oldFile = new File(dbPath + "/" + f.getName());
                    if (oldFile.exists()) oldFile.delete();

                    // REPLACE IT
                    if (!f.renameTo(oldFile))
                      throw new ODistributedException(
                          "Cannot restore exclusive cluster file '"
                              + f.getAbsolutePath()
                              + "' into "
                              + oldFile.getAbsolutePath());
                  }

                  uniqueClustersBackupDirectory.delete();
                }

                try (ODatabaseDocumentInternal inst = distrDatabase.getDatabaseInstance()) {
                  Optional<byte[]> read =
                      ((OAbstractPaginatedStorage) inst.getStorage()).getLastMetadata();
                  if (read.isPresent()) {
                    OTxMetadataHolder metadata = OTxMetadataHolderImpl.read(read.get());
                    final OSyncDatabaseNewDeltaTask deployTask =
                        new OSyncDatabaseNewDeltaTask(metadata.getStatus());

                    final List<String> targetNodes = new ArrayList<String>(1);
                    targetNodes.add(iNode);
                    final ODistributedResponse response =
                        sendRequest(
                            databaseName,
                            null,
                            targetNodes,
                            deployTask,
                            getNextMessageIdCounter(),
                            ODistributedRequest.EXECUTION_MODE.RESPONSE,
                            null);
                    if (response == null)
                      throw new ODistributedDatabaseDeltaSyncException(
                          "Error Requesting delta sync");
                    installResponseNewDeltaSync(
                        distrDatabase,
                        databaseName,
                        cfg,
                        iNode,
                        (ONewDeltaTaskResponse) response.getPayload());
                  }
                }
              } else {

                // USES A CUSTOM WRAPPER OF IS TO WAIT FOR FILE IS WRITTEN (ASYNCH)
                try (InputStream in = receiver.getInputStream()) {

                  // IMPORT FULL DATABASE (LISTENER ONLY FOR DEBUG PURPOSE)
                  serverInstance
                      .getDatabases()
                      .networkRestore(
                          databaseName,
                          in,
                          new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                              if (uniqueClustersBackupDirectory != null
                                  && uniqueClustersBackupDirectory.exists()) {
                                // RESTORE UNIQUE FILES FROM THE BACKUP FOLDERS. THOSE FILES ARE THE
                                // CLUSTERS OWNED EXCLUSIVELY BY CURRENT
                                // NODE THAT WOULD BE LOST IF NOT REPLACED
                                for (File f : uniqueClustersBackupDirectory.listFiles()) {
                                  final File oldFile = new File(dbPath + "/" + f.getName());
                                  if (oldFile.exists()) oldFile.delete();

                                  // REPLACE IT
                                  if (!f.renameTo(oldFile))
                                    throw new ODistributedException(
                                        "Cannot restore exclusive cluster file '"
                                            + f.getAbsolutePath()
                                            + "' into "
                                            + oldFile.getAbsolutePath());
                                }

                                uniqueClustersBackupDirectory.delete();
                              }
                              return null;
                            }
                          });
                }
              }
              return null;
            } catch (IOException e) {
              throw OException.wrapException(
                  new OIOException("Error on distributed sync of database"), e);
            }
          }
        });
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n")) iText = iText.substring(2);
    else if (iText.startsWith("\n")) iText = iText.substring(1);

    OLogManager.instance().debug(this, iText);
  }

  public void stopNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(
        this, nodeName, null, DIRECTION.NONE, "Sending request of stopping node '%s'...", iNode);

    final ODistributedRequest request =
        new ODistributedRequest(
            this,
            getLocalNodeId(),
            getNextMessageIdCounter(),
            null,
            getTaskFactoryManager()
                .getFactoryByServerName(iNode)
                .createTask(OStopServerTask.FACTORYID));

    getRemoteServer(iNode).sendRequest(request);
  }

  public void restartNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(
        this, nodeName, null, DIRECTION.NONE, "Sending request of restarting node '%s'...", iNode);

    final ODistributedRequest request =
        new ODistributedRequest(
            this,
            getLocalNodeId(),
            getNextMessageIdCounter(),
            null,
            getTaskFactoryManager()
                .getFactoryByServerName(iNode)
                .createTask(ORestartServerTask.FACTORYID));

    getRemoteServer(iNode).sendRequest(request);
  }

  public long getNextMessageIdCounter() {
    return localMessageIdCounter.getAndIncrement();
  }

  @Override
  public String getNodeUuidByName(String name) {
    return clusterManager.getNodeUuidByName(name);
  }

  @Override
  public void updateLastClusterChange() {
    clusterManager.updateLastClusterChange();
  }

  public void closeRemoteServer(final String node) {
    remoteServerManager.closeRemoteServer(node);
  }

  protected boolean isRelatedToLocalServer(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    // Check for the system database.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME)) return false;

    if (dbUrl.startsWith("plocal:")) {
      final OLocalPaginatedStorage paginatedStorage =
          (OLocalPaginatedStorage) iDatabase.getStorage();

      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final Path storagePath = paginatedStorage.getStoragePath();
      final Path dbDirectoryPath = Paths.get(serverInstance.getDatabaseDirectory());

      // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
      return storagePath.startsWith(dbDirectoryPath);
    } else return !dbUrl.startsWith("remote:");
  }

  /** Avoids to dump the same configuration twice if it's unchanged since the last time. */
  public void dumpServersStatus() {
    final ODocument cfg = getClusterConfiguration();

    final String compactStatus = ODistributedOutput.getCompactServerStatus(this, cfg);

    if (!lastServerDump.equals(compactStatus)) {
      lastServerDump = compactStatus;

      ODistributedServerLog.info(
          this,
          getLocalNodeName(),
          null,
          DIRECTION.NONE,
          "Distributed servers status (*=current):\n%s",
          ODistributedOutput.formatServerStatus(this, cfg));
    }
  }

  @Override
  public long getClusterTime() {
    return clusterManager.getClusterTime();
  }

  public static String getListeningBinaryAddress(final ODocument cfg) {
    if (cfg == null) return null;

    String url = cfg.field("publicAddress");

    final Collection<Map<String, Object>> listeners = cfg.field("listeners");
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
    if (url == null) url = listenUrl;
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

  @Override
  public void messageReceived(ODistributedRequest request) {

    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageReceived(request);
    }
  }

  @Override
  public void messagePartitionCalculate(
      ODistributedRequest request, Set<Integer> involvedWorkerQueues) {

    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessagePartitionCalculated(request, involvedWorkerQueues);
    }
  }

  @Override
  public void messageBeforeOp(String op, ODistributedRequestId request) {

    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageBeforeOp(op, request);
    }
  }

  @Override
  public void messageAfterOp(String op, ODistributedRequestId request) {
    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageAfterOp(op, request);
    }
  }

  @Override
  public void messageCurrentPayload(ODistributedRequestId requestId, Object responsePayload) {
    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageCurrentPayload(requestId, responsePayload);
    }
  }

  @Override
  public void messageProcessStart(ODistributedRequest message) {
    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageProcessStart(message);
    }
  }

  @Override
  public void messageProcessEnd(ODistributedRequest iRequest, Object responsePayload) {
    for (ODistributedLifecycleListener listener : listeners) {
      listener.onMessageProcessEnd(iRequest, responsePayload);
    }
  }

  /** Initializes all the available server's databases as distributed. */
  public void loadLocalDatabases() {
    final List<String> dbs =
        new ArrayList<String>(serverInstance.getAvailableStorageNames().keySet());
    Collections.sort(dbs);

    for (final String databaseName : dbs) {
      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.info(
            this, nodeName, null, DIRECTION.NONE, "Opening database '%s'...", databaseName);

        // INIT THE STORAGE
        final ODistributedDatabaseImpl ddb = messageService.registerDatabase(databaseName);

        executeInDistributedDatabaseLock(
            databaseName,
            60000,
            null,
            new OCallable<Object, OModifiableDistributedConfiguration>() {
              @Override
              public Object call(OModifiableDistributedConfiguration cfg) {
                ODistributedServerLog.info(
                    this,
                    nodeName,
                    null,
                    DIRECTION.NONE,
                    "Current node started as %s for database '%s'",
                    cfg.getServerRole(nodeName),
                    databaseName);

                ddb.resume();

                // 1ST NODE TO HAVE THE DATABASE
                cfg.addNewNodeInServerList(nodeName);

                // COLLECT ALL THE CLUSTERS WITH REMOVED NODE AS OWNER
                reassignClustersOwnership(nodeName, databaseName, cfg, true);

                ddb.setOnline();

                return null;
              }
            });
      }
    }
  }

  public void installNewDatabasesFromCluster() {
    if (getActiveServers().size() <= 1) {
      // NO OTHER NODES WHERE ALIGN
      return;
    }

    final List<String> dbs = new ArrayList<>(clusterManager.getDatabases());
    Collections.sort(dbs);

    for (String databaseName : dbs) {
      final Set<String> availableServers = getAvailableNodeNames(databaseName);
      if (availableServers.isEmpty())
        // NO NODE HAS THIS DATABASE AVAILABLE
        continue;

      final DB_STATUS currStatus = getDatabaseStatus(nodeName, databaseName);
      if (currStatus == DB_STATUS.SYNCHRONIZING
          || currStatus == DB_STATUS.ONLINE
          || currStatus == DB_STATUS.BACKUP)
        // FIX PREVIOUS STATUS OF DATABASE
        setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);

      try {
        if (!installDatabase(
            true,
            databaseName,
            false,
            OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean())) {
          setDatabaseStatus(getLocalNodeName(), databaseName, DB_STATUS.NOT_AVAILABLE);
        }
      } catch (Exception e) {
        ODistributedServerLog.error(
            this,
            getLocalNodeName(),
            null,
            DIRECTION.IN,
            "Error on installing database '%s' on local node (error=%s)",
            e,
            databaseName,
            e.toString());
      }
    }
  }

  public void notifyStarted() {
    serverStarted.countDown();
  }

  protected void dumpStats() {
    try {
      final ODocument clusterCfg = getClusterConfiguration();

      final Set<String> dbs = getManagedDatabases();

      final StringBuilder buffer = new StringBuilder(8192);

      buffer.append(ODistributedOutput.formatLatency(this, clusterCfg));
      buffer.append(ODistributedOutput.formatMessages(this, clusterCfg));

      OLogManager.instance().flush();
      for (String db : dbs) {
        buffer.append(messageService.getDatabase(db).dump());
      }

      // DUMP HA STATS
      System.out.println(buffer);

    } catch (Exception e) {
      ODistributedServerLog.error(
          this, nodeName, null, DIRECTION.NONE, "Error on printing HA stats", e);
    }
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) throws IOException {
    if (rNodeName == null) throw new IllegalArgumentException("Server name is NULL");

    // TODO: check if it's possible to bypass remote call
    //    if (rNodeName.equalsIgnoreCase(getLocalNodeName()))
    //      throw new IllegalArgumentException("Cannot send remote message to the local server");

    ORemoteServerController remoteServer = remoteServerManager.getRemoteServer(rNodeName);
    if (remoteServer == null) {
      Member member = clusterManager.getClusterMemberByName(rNodeName);

      for (int retry = 0; retry < 20; ++retry) {
        ODocument cfg = getNodeConfigurationByUuid(member.getUuid(), false);
        if (cfg == null || cfg.field("listeners") == null) {
          try {
            Thread.sleep(100);
            member = clusterManager.getClusterMemberByName(rNodeName);
            continue;

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw OException.wrapException(
                new ODistributedException("Cannot find node '" + rNodeName + "'"), e);
          }
        }

        final String url = ODistributedPlugin.getListeningBinaryAddress(cfg);

        if (url == null) {
          closeRemoteServer(rNodeName);
          throw new ODatabaseException(
              "Cannot connect to a remote node because the url was not found");
        }

        final String userPassword = cfg.field("user_replicator");

        if (userPassword != null) {
          remoteServer =
              remoteServerManager.connectRemoteServer(
                  rNodeName, url, REPLICATOR_USER, userPassword);
          break;
        }

        // RETRY TO GET USR+PASSWORD IN A WHILE
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException.wrapException(
              new OInterruptedException("Cannot connect to remote server " + rNodeName), e);
        }
      }
    }

    if (remoteServer == null)
      throw new ODistributedException("Cannot find node '" + rNodeName + "'");

    return remoteServer;
  }

  @Override
  public long getLastClusterChangeOn() {
    return clusterManager.getLastClusterChangeOn();
  }

  @Override
  public NODE_STATUS getNodeStatus() {
    return clusterManager.getNodeStatus();
  }

  @Override
  public void setNodeStatus(NODE_STATUS iStatus) {
    clusterManager.setNodeStatus(iStatus);
  }

  @Override
  public boolean checkNodeStatus(NODE_STATUS status) {
    return clusterManager.checkNodeStatus(status);
  }

  public void onNodeJoined(String joinedNodeName, Member member) {
    try {
      getRemoteServer(joinedNodeName);
    } catch (IOException e) {
      ODistributedServerLog.error(
          this,
          nodeName,
          joinedNodeName,
          DIRECTION.OUT,
          "Error on connecting to node %s",
          joinedNodeName);
    }

    ODistributedServerLog.info(
        this,
        nodeName,
        clusterManager.getNodeName(member, true),
        DIRECTION.IN,
        "Added node configuration id=%s name=%s, now %d nodes are configured",
        member,
        clusterManager.getNodeName(member, true),
        getActiveServers().size());

    // NOTIFY NODE WAS ADDED SUCCESSFULLY
    for (ODistributedLifecycleListener l : listeners) l.onNodeJoined(joinedNodeName);

    // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN ONLY IF AUTO-DEPLOY IS SET
    for (String db : messageService.getDatabases()) {
      if (getDatabaseConfiguration(db).isAutoDeploy()
          && getDatabaseStatus(joinedNodeName, db) == DB_STATUS.ONLINE) {
        setDatabaseStatus(joinedNodeName, db, DB_STATUS.NOT_AVAILABLE);
      }
    }
    dumpServersStatus();
  }

  // This is used only during startup and gets called by the cluster metadata manager
  public void connectToAllNodes(Set<String> clusterNodes) throws IOException {
    for (String m : clusterNodes) if (!m.equals(nodeName)) getRemoteServer(m);
  }

  @Override
  public void removeServer(final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    if (nodeLeftName == null) return;
    Member member = clusterManager.removeFromLocalActiveServerList(nodeLeftName);
    if (member == null) return;

    ODistributedServerLog.debug(
        this,
        nodeName,
        nodeLeftName,
        DIRECTION.NONE,
        "Distributed server '%s' is unreachable",
        nodeLeftName);

    try {
      // REMOVE INTRA SERVER CONNECTION
      closeRemoteServer(nodeLeftName);

      // NOTIFY ABOUT THE NODE HAS LEFT
      for (ODistributedLifecycleListener l : listeners)
        try {
          l.onNodeLeft(nodeLeftName);
        } catch (Exception e) {
          // IGNORE IT
          ODistributedServerLog.debug(
              this,
              nodeName,
              nodeLeftName,
              DIRECTION.NONE,
              "Error on calling onNodeLeft event on '%s'",
              e,
              l);
        }

      // UNLOCK ANY PENDING LOCKS
      if (messageService != null) {
        for (String dbName : messageService.getDatabases())
          messageService.getDatabase(dbName).handleUnreachableNode(nodeLeftName);
      }

      clusterManager.removeServerFromCluster(member, nodeLeftName, removeOnlyDynamicServers);

      for (String databaseName : getManagedDatabases()) {
        try {
          if (getDatabaseConfiguration(databaseName).getServerRole(nodeName)
              == ODistributedConfiguration.ROLES.MASTER) {
            reassignClustersOwnership(nodeName, databaseName, null, false);
          }
        } catch (Exception e) {
          // IGNORE IT
          ODistributedServerLog.error(
              this,
              nodeName,
              null,
              DIRECTION.NONE,
              "Cannot re-balance the cluster for database '%s' because the Lock Manager is not available (err=%s)",
              databaseName,
              e.getMessage());
        }
      }

      if (nodeLeftName.equalsIgnoreCase(nodeName))
        // CURRENT NODE: EXIT
        System.exit(1);
    } finally {
      // REMOVE NODE IN DB CFG
      if (messageService != null) messageService.handleUnreachableNode(nodeLeftName);
    }
  }

  @Override
  public DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName) {
    return clusterManager.getDatabaseStatus(iNode, iDatabaseName);
  }

  @Override
  public void setDatabaseStatus(String iNode, String iDatabaseName, DB_STATUS iStatus) {
    clusterManager.setDatabaseStatus(iNode, iDatabaseName, iStatus);
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase)) return;

    if (getNodeStatus() != NODE_STATUS.ONLINE) return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {

      final String dbName = iDatabase.getName();

      //      final ODocument dCfg = (ODocument)
      // configurationMap.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName);
      //      if (dCfg != null && getAvailableNodes(dbName) > 0) {
      //        throw new ODistributedException(
      //            "Cannot create the new database '" + dbName + "' because it is already present
      // in distributed configuration");
      //      }

      final ODistributedConfiguration cfg = getDatabaseConfiguration(dbName);

      // TODO: TEMPORARY PATCH TO WAIT FOR DB PROPAGATION IN CFG TO ALL THE OTHER SERVERS
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw OException.wrapException(
            new ODistributedException(
                "Error on creating database '" + dbName + "' on distributed nodes"),
            e);
      }

      // WAIT UNTIL THE DATABASE HAS BEEN PROPAGATED TO ALL THE SERVERS
      final Set<String> servers = cfg.getAllConfiguredServers();
      if (servers.size() > 1) {
        int retry = 0;
        for (; retry < 100; ++retry) {
          boolean allServersAreOnline = true;
          for (String server : servers) {
            if (!isNodeOnline(server, dbName)) {
              allServersAreOnline = false;
              break;
            }
          }

          if (allServersAreOnline) break;

          // WAIT FOR ANOTHER RETRY
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw OException.wrapException(
                new ODistributedException(
                    "Error on creating database '" + dbName + "' on distributed nodes"),
                e);
          }
        }

        if (retry >= 100)
          ODistributedServerLog.warn(
              this,
              getLocalNodeName(),
              null,
              DIRECTION.NONE,
              "Timeout waiting for all nodes to be up for database %s",
              dbName);
      }

      onOpen(iDatabase);

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.instance().set(currDb);
    }
  }

  // Called to notify this server, that a node has been removed from the cluster
  public void onServerRemoved(String nodeName) {
    closeRemoteServer(nodeName);
  }

  // Called when the status of a distributed database changes to online
  public void onDbStatusOnline(String databaseName) {
    final DB_STATUS s = getDatabaseStatus(getLocalNodeName(), databaseName);
    if (s == DB_STATUS.NOT_AVAILABLE) {
      // INSTALL THE DATABASE
      installDatabase(
          false,
          databaseName,
          false,
          OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());
    }
  }

  // Called when the db config has changed
  public void onDbConfigUpdated(String databaseName, ODocument config, boolean updated) {
    // SEND A DISTRIBUTED MSG TO ALL THE SERVERS
    final Set<String> servers = new HashSet<String>(getActiveServers());
    servers.remove(nodeName);

    if (!servers.isEmpty() && messageService.getDatabase(databaseName) != null) {

      final ODistributedResponse dResponse =
          sendRequest(
              databaseName,
              null,
              servers,
              new OUpdateDatabaseConfigurationTask(databaseName, config),
              getNextMessageIdCounter(),
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE,
              null);
    }
  }

  public boolean onNodeJoining(final String joinedNodeName) {
    // NOTIFY NODE IS GOING TO BE ADDED. IS EVERYBODY OK?
    for (ODistributedLifecycleListener l : listeners) {
      if (!l.onNodeJoining(joinedNodeName)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ODocument getClusterConfiguration() {
    if (!enabled) return null;

    return clusterManager.getClusterConfiguration();
  }

  @Override
  public String getNodeNameById(int id) {
    return clusterManager.getNodeNameById(id);
  }

  @Override
  public int getNodeIdByName(String node) {
    return clusterManager.getNodeIdByName(node);
  }

  @Override
  public ODocument getNodeConfigurationByUuid(String iNode, boolean useCache) {
    return clusterManager.getNodeConfigurationByUuid(iNode, useCache);
  }

  public void reloadRegisteredNodes() {
    clusterManager.reloadRegisteredNodes();
  }

  public boolean removeNodeFromConfiguration(
      String nodeName,
      String databaseName,
      boolean removeOnlyDynamicServers,
      boolean statusOffline) {
    return clusterManager.removeNodeFromConfiguration(
        nodeName, databaseName, removeOnlyDynamicServers, statusOffline);
  }

  public HazelcastInstance getHazelcastInstance() {
    return clusterManager.getHazelcastInstance();
  }

  @Override
  public ODocument getOnlineDatabaseConfiguration(String databaseName) {
    return clusterManager.getOnlineDatabaseConfiguration(databaseName);
  }
}
