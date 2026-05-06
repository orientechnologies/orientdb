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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.ONodeListenerConfig;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.NODE_STATUS;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManagerFactory;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManagerImpl;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStartupException;
import com.orientechnologies.orient.server.distributed.ODistributedStrategy;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactoryManager;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.distributed.impl.metadata.OClassDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.ORemoteTaskFactoryManagerImpl;
import com.orientechnologies.orient.server.distributed.impl.task.ORestartServerTask;
import com.orientechnologies.orient.server.distributed.impl.task.OStopServerTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseConfigurationTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastClusterMetadataManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import sun.misc.Signal;

/**
 * Plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedPlugin extends OServerPluginAbstract implements ODistributedServerManager {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODistributedPlugin.class);
  public static final String REPLICATOR_USER = "_CrossServerTempUser";

  protected static final String PAR_DEF_DISTRIB_DB_CONFIG = "configuration.db.default";
  protected static final String NODE_NAME_ENV = "ORIENTDB_NODE_NAME";

  private OServer serverInstance;
  private String nodeName = null;
  protected File defaultDatabaseConfigFile;
  protected List<ODistributedLifecycleListener> listeners = new ArrayList<>();

  protected static final int DEPLOY_DB_MAX_RETRIES = 10;
  protected Set<String> installingDatabases =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  protected volatile ODistributedMessageServiceImpl messageService;
  protected Date startedOn = new Date();
  protected ODistributedStrategy responseManagerFactory = new ODefaultDistributedStrategy();
  protected ORemoteTaskFactoryManager taskFactoryManager = new ORemoteTaskFactoryManagerImpl(this);

  private volatile String lastServerDump = "";

  private OCancellableTimer haStatsTask = null;
  private OCancellableTimer healthCheckerTask = null;
  protected OSignalHandler.OSignalListener signalListener;

  private final OHazelcastClusterMetadataManager clusterManager;

  public ODistributedPlugin() {
    clusterManager = new OHazelcastClusterMetadataManager(this);
  }

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

    if (nodeName == null) assignNodeName();
    // TODO: get the group name from some configuration
    ((OrientDBDistributed) serverInstance.getDatabases()).initDistributed(nodeName, "OrientDB", 1);
    clusterManager.configHazelcastPlugin(oServer, iParams, nodeName);
  }

  public File getDefaultDatabaseConfigFile() {
    return defaultDatabaseConfigFile;
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
    OrientDBInternal databases = serverInstance.getDatabases();
    if (databases instanceof OrientDBDistributed) ((OrientDBDistributed) databases).setPlugin(this);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    serverInstance.addTemporaryUser(REPLICATOR_USER, "" + new SecureRandom().nextLong(), "*");

    try {
      clusterManager.startupHazelcastPlugin();

      OContextConfiguration ctx = serverInstance.getContextConfiguration();
      final long statsDelay = ctx.getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DUMP_STATS_EVERY);
      if (statsDelay > 0) {
        haStatsTask = databases.periodicExecute(this::dumpStats, statsDelay);
      }

      final long healthChecker =
          ctx.getValueAsLong(OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY);
      if (healthChecker > 0) {
        OClusterHealthChecker checkTask = new OClusterHealthChecker(this, healthChecker);
        healthCheckerTask = databases.periodicExecute(checkTask, healthChecker);
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
      logger.errorNode(nodeName, "Error on starting distributed plugin", e);
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

    logger.warnNode(nodeName, "Shutting down node '%s'...", nodeName);
    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    clusterManager.prepareHazelcastPluginShutdown();
    try {
      if (healthCheckerTask != null) healthCheckerTask.cancel();
      if (haStatsTask != null) haStatsTask.cancel();

      setNodeStatus(NODE_STATUS.OFFLINE);

    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }
    clusterManager.hazelcastPluginShutdown();
  }

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
  public ONodeConfig getLocalNodeConfiguration() {
    ONodeConfig nodeCfg = new ONodeConfig();

    nodeCfg.setId(getLocalNodeId());
    nodeCfg.setUuid(clusterManager.getLocalNodeUuid());
    nodeCfg.setName(nodeName);
    nodeCfg.setVersion(OConstants.getRawVersion());
    nodeCfg.setPublicAddress(clusterManager.getPublicAddress());
    nodeCfg.setStartedOn(startedOn);
    nodeCfg.setStatus(getNodeStatus().toString());
    nodeCfg.setConnections(serverInstance.getClientConnectionManager().getTotal());

    List<ONodeListenerConfig> listeners = new ArrayList<>();
    for (OServerNetworkListener listener : serverInstance.getNetworkListeners()) {
      listeners.add(
          new ONodeListenerConfig(
              listener.getProtocolType().getSimpleName(), listener.getListeningAddress(true)));
    }
    nodeCfg.setListeners(listeners);

    // STORE THE TEMP USER/PASSWD USED FOR REPLICATION
    final OSecurityUser user = serverInstance.getSecurity().getUser(REPLICATOR_USER);
    if (user != null)
      nodeCfg.setReplicator(serverInstance.getSecurity().getUser(REPLICATOR_USER).getPassword());

    if (((OrientDBDistributed) serverInstance.getDatabases()).getNodeState() != null) {
      ODatabasesTopology databaseTopology =
          ((OrientDBDistributed) serverInstance.getDatabases())
              .getNodeState()
              .getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      var dbs =
          dbIds.stream()
              .map((id) -> databaseTopology.getDatabaseName(id))
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

    nodeCfg.setLatencies("latencies", getMessageService().getLatencies());
    nodeCfg.setMessages("messages", getMessageService().getMessageStats());

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

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public ODistributedResponse sendRequest(
      final String iDatabaseName, final Collection<String> iTargetNodes, final ORemoteTask iTask) {
    return sendRequest(iDatabaseName, iTargetNodes, iTask, nextRequestId(), null, null);
  }

  @Override
  public ODistributedResponse sendSingleRequest(
      String databaseName, String node, ORemoteTask iTask) {
    return sendRequest(
        databaseName, Collections.singletonList(node), iTask, nextRequestId(), null, null);
  }

  public ODistributedResponse sendRequest(
      final String iDatabaseName,
      final Collection<String> iTargetNodes,
      final ORemoteTask iTask,
      final ODistributedRequestId reqId,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    final ODistributedRequest.EXECUTION_MODE iExecutionMode = EXECUTION_MODE.RESPONSE;
    final ODistributedRequest req = new ODistributedRequest(this, reqId, iDatabaseName, iTask);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      logger.errorOut(
          nodeName, null, "No nodes configured for partition '%s' request: %s", iDatabaseName, req);
      throw new ODistributedException(
          "No nodes configured '" + iDatabaseName + "' request: " + req);
    }

    getMessageService().updateMessageStats(iTask.getName());
    if (responseManagerFactory != null) {
      return send2Nodes(req, iTargetNodes, iExecutionMode, localResult, responseManagerFactory);
    } else {
      return send2Nodes(req, iTargetNodes, iExecutionMode, localResult);
    }
  }

  protected void checkForServerOnline(final ODistributedRequest iRequest)
      throws ODistributedException {

    if (!getServerInstance().getDatabases().isDistributedOnline()) {
      logger.errorOut(
          this.nodeName, null, "Local server is not online. Request %s will be ignored", iRequest);
      throw new OOfflineNodeException(
          "Local server is not online. Request " + iRequest + " will be ignored");
    }
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    try {
      checkForServerOnline(iRequest);

      final String databaseName = iRequest.getDatabaseName();

      if (iNodes.isEmpty()) {
        logger.errorOut(
            this.nodeName,
            null,
            "No nodes configured for database '%s' request: %s",
            databaseName,
            iRequest);
        throw new ODistributedException(
            "No nodes configured for partition '" + databaseName + "' request: " + iRequest);
      }
      final ORemoteTask task = iRequest.getTask();
      final boolean checkNodesAreOnline = task.isNodeOnlineRequired();

      final Set<String> nodesConcurToTheQuorum;
      int availableNodes = iNodes.size();
      int onlineMasters;
      if (databaseName != null) {
        nodesConcurToTheQuorum =
            getDistributedStrategy().getNodesConcurInQuorum(this, databaseName, iRequest, iNodes);

        // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE
        // NODES

        if (checkNodesAreOnline) {
          availableNodes =
              ctx.getNodesWithStatus(
                  iNodes,
                  databaseName,
                  ODistributedServerManager.DB_STATUS.ONLINE,
                  ODistributedServerManager.DB_STATUS.BACKUP,
                  ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
        }

        // all online masters
        onlineMasters = ctx.getOnlineMasters(databaseName);
        //        onlineMasters =
        //            getOnlineNodes(databaseName).stream()
        //                .filter(f -> cfg.getServerRole(f) ==
        // ODistributedConfiguration.ROLES.MASTER)
        //                .collect(Collectors.toSet())
        //                .size();

      } else {
        nodesConcurToTheQuorum = new HashSet<String>(iNodes);
        onlineMasters = availableNodes;
      }

      final int expectedResponses = localResult != null ? availableNodes + 1 : availableNodes;

      final int quorum =
          calculateQuorum(
              task.getQuorumType(),
              null,
              expectedResponses,
              nodesConcurToTheQuorum.size(),
              onlineMasters,
              checkNodesAreOnline,
              this.nodeName);

      final boolean groupByResponse =
          task.getResultStrategy() != OAbstractRemoteTask.RESULT_STRATEGY.UNION;

      final boolean waitLocalNode = waitForLocalNode(null, iNodes);

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

      getMessageService().registerRequest(iRequest.getId().getMessageId(), currentResponseMgr);

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

          logger.warnOut(
              this.nodeName,
              node,
              "Error on sending distributed request %s (err=%s). Active nodes: %s",
              iRequest,
              reason,
              ctx.getAvailableNodeNames(databaseName));
        }
      }

      if (currentResponseMgr.getExpectedNodes().isEmpty())
        // NO SERVER TO SEND A MESSAGE
        throw new ODistributedException(
            "No server active for distributed request ("
                + iRequest
                + ") against database '"
                + databaseName
                + "' to nodes "
                + iNodes);

      if (databaseName != null) {
        ODistributedDatabaseImpl shared = getDatabase(databaseName);
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
      throw OException.wrapException(
          new ODistributedException(
              "Error on executing distributed request ("
                  + iRequest
                  + ") against database '"
                  + this.nodeName
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

        logger.warnIn(
            this.nodeName,
            null,
            "Timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s"
                + " request=(%s)",
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
      final ODistributedConfiguration cfg,
      final int totalServers,
      final int totalMasterServers,
      int onlineMasters,
      final boolean checkNodesAreOnline,
      final String localNodeName) {

    int quorum = 1;

    int totalServerInQuorum = totalServers;
    int clusterQuorum = 0;
    switch (quorumType) {
      case NONE:
        // IGNORE IT
        break;
      case READ:
        if (cfg != null) {
          clusterQuorum = cfg.getReadQuorum(totalServers, localNodeName);
        } else {
          clusterQuorum = 1;
        }
        break;
      case WRITE:
        if (cfg != null) {
          clusterQuorum = cfg.getWriteQuorum(totalMasterServers, localNodeName);
          totalServerInQuorum = totalMasterServers;
        } else {
          clusterQuorum = totalMasterServers / 2 + 1;
          totalServerInQuorum = totalMasterServers;
        }
        break;
      case WRITE_ALL_MASTERS:
        if (cfg != null) {
          int cfgQuorum = cfg.getWriteQuorum(totalMasterServers, localNodeName);
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
        final long l = getMessageService().getCurrentLatency(n);
        delta = Math.max(delta, l);
      }

    return timeout + delta;
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult) {
    return send2Nodes(
        iRequest,
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
              getServerInstance().getDatabases(),
              iRequest,
              iNodes,
              nodesConcurToTheQuorum,
              quorum,
              waitLocalNode,
              adjustTimeoutWithLatency(
                  iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
              groupByResponse);
        });
  }

  protected boolean waitForLocalNode(
      final ODistributedConfiguration cfg, final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(this.nodeName)) {
      if (cfg != null) {
        if (cfg.isReadYourWrites(null)) waitLocalNode = true;
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

    return OScenarioThreadLocal.executeAsDistributed(
        () -> {
          return localInternalExecute(reqId, task, database);
        });
  }

  private Object localInternalExecute(
      final ODistributedRequestId reqId,
      final ORemoteTask task,
      final ODatabaseDocumentInternal database) {
    try {
      final Object result = task.execute(reqId, serverInstance, database);

      if (result instanceof Throwable && !(result instanceof OException))
        // EXCEPTION
        logger.debugNode(
            nodeName,
            "Error on executing request %d (%s) on local node: ",
            (Throwable) result,
            reqId,
            task);

      return result;

    } catch (InterruptedException e) {
      // IGNORE IT
      logger.debugNode(
          nodeName,
          "Interrupted execution on executing distributed request %s on local node: %s",
          e,
          reqId,
          task);
      return e;

    } catch (Exception e) {
      if (!(e instanceof OException))
        logger.errorNode(
            nodeName,
            "Error on executing distributed request %s on local node: %s",
            e,
            reqId,
            task);

      return e;
    }
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public void reassignClustersOwnership(
      final String iNode, final String databaseName, final boolean canCreateNewClusters) {
    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();

    if (!ctx.isNodeMaster(iNode, databaseName))
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return;

    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try (ODatabaseDocumentInternal iDatabase = getServerInstance().openDatabase(databaseName)) {

      logger.infoNode(
          nodeName, "Reassigning ownership of clusters for database %s...", iDatabase.getName());
      final Set<String> availableNodes = ctx.getAvailableNodeNames(iDatabase.getName());

      // FILTER OUT NON MASTER SERVER
      for (Iterator<String> it = availableNodes.iterator(); it.hasNext(); ) {
        final String node = it.next();
        if (ctx.isNodeMaster(node, databaseName)) it.remove();
      }
      iDatabase.activateOnCurrentThread();
      final OSchema schema = iDatabase.getDatabaseOwner().getMetadata().getSchema();

      for (final OClass clazz : schema.getClasses()) {
        ((OClassDistributed) clazz)
            .autoAssignClusterOwnership(iDatabase, availableNodes, canCreateNewClusters);
      }

      logger.infoNode(
          nodeName,
          "Reassignment of clusters for database '%s' completed (classes=%d)",
          iDatabase.getName(),
          schema.getClasses().size());
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }
  }

  @Override
  public int getLocalNodeId() {
    return clusterManager.getLocalNodeId();
  }

  @Override
  public String toString() {
    return nodeName;
  }

  @Override
  public ODistributedMessageService getMessageService() {
    return ((OrientDBDistributed) serverInstance.getDatabases()).getMessageService();
  }

  public boolean isSyncronizing(String databaseName) {
    return this.installingDatabases.contains(databaseName);
  }

  @Override
  public ORemoteTaskFactoryManager getTaskFactoryManager() {
    return taskFactoryManager;
  }

  @Override
  public Set<String> getActiveServers() {
    return clusterManager.getActiveServers();
  }

  @Override
  public Set<String> getActiveServerNotLocal() {
    Set<String> res = clusterManager.getActiveServers();
    res.remove(getLocalNodeName());
    return res;
  }

  public ODistributedStrategy getDistributedStrategy() {
    return responseManagerFactory;
  }

  public void setDistributedStrategy(final ODistributedStrategy streatgy) {
    this.responseManagerFactory = streatgy;
  }

  public void notifyClients(String databaseName) {
    List<String> hosts = new ArrayList<>();
    for (String name : getActiveServers()) {
      ONodeConfig memberConfig = clusterManager.getNodeConfigurationByName(name, true);
      if (memberConfig != null) {
        final String nodeStatus = memberConfig.getStatus();

        if (!"OFFLINE".equals(nodeStatus)) {
          final Collection<ONodeListenerConfig> listeners = memberConfig.getListeners();
          if (listeners != null)
            for (ONodeListenerConfig listener : listeners) {
              if (listener.getProtocol().equals("ONetworkProtocolBinary")) {
                hosts.add(listener.getListen());
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

    logger.warnNode("Assigning distributed node name: %s", this.nodeName);

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

  public ODistributedRequestId nextRequestId() {
    return new ODistributedRequestId(getLocalNodeId(), getNextMessageIdCounter());
  }

  public void stopNode(final String iNode) throws IOException {
    logger.warnNode(nodeName, "Sending request of stopping node '%s'...", iNode);

    final ODistributedRequest request =
        new ODistributedRequest(
            this,
            nextRequestId(),
            null,
            getTaskFactoryManager()
                .getFactoryByServerName(iNode)
                .createTask(OStopServerTask.FACTORYID));

    getRemoteServer(iNode).sendRequest(request);
  }

  public void restartNode(final String iNode) throws IOException {
    logger.warnNode(nodeName, "Sending request of restarting node '%s'...", iNode);

    final ODistributedRequest request =
        new ODistributedRequest(
            this,
            nextRequestId(),
            null,
            getTaskFactoryManager()
                .getFactoryByServerName(iNode)
                .createTask(ORestartServerTask.FACTORYID));

    getRemoteServer(iNode).sendRequest(request);
  }

  public long getNextMessageIdCounter() {
    return ((OrientDBDistributed) serverInstance.getDatabases()).getNextMessageIdCounter();
  }

  @Override
  public void updateLastClusterChange() {
    clusterManager.updateLastClusterChange();
  }

  public void closeRemoteServer(final String node) {
    ((OrientDBDistributed) this.serverInstance.getDatabases()).closeRemoteServer(node);
  }

  /** Avoids to dump the same configuration twice if it's unchanged since the last time. */
  public void dumpServersStatus() {
    final OClusterConfiguration cfg = getClusterConfiguration();

    final String compactStatus = ODistributedOutput.getCompactServerStatus(this, cfg);

    if (!lastServerDump.equals(compactStatus)) {
      lastServerDump = compactStatus;

      //      logger.infoNode(
      //          getLocalNodeName(),
      //          "Distributed servers status (*=current):\n%s",
      //          ODistributedOutput.formatServerStatus(this, cfg));
    }
  }

  public static String getListeningBinaryAddress(final ONodeConfig cfg) {
    if (cfg == null) return null;

    final Collection<ONodeListenerConfig> listeners = cfg.getListeners();
    if (listeners == null)
      throw new ODatabaseException(
          "Cannot connect to a remote node because bad distributed configuration: missing"
              + " 'listeners' array field");
    String listenUrl = null;
    for (ONodeListenerConfig listener : listeners) {
      if ((listener.getProtocol()).equals("ONetworkProtocolBinary")) {
        listenUrl = (String) listener.getListen();
        break;
      }
    }
    return listenUrl;
  }

  protected void dumpStats() {
    try {
      final OClusterConfiguration clusterCfg = getClusterConfiguration();

      ODatabasesTopology databaseTopology =
          ((OrientDBDistributed) serverInstance.getDatabases())
              .getNodeState()
              .getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      final List<String> dbs =
          new ArrayList<>(
              dbIds.stream().map((dbId) -> databaseTopology.getDatabaseName(dbId)).toList());
      Collections.sort(dbs);
      final StringBuilder buffer = new StringBuilder(8192);

      buffer.append(ODistributedOutput.formatLatency(this, clusterCfg));
      buffer.append(ODistributedOutput.formatMessages(this, clusterCfg));

      OLogManager.instance().flush();
      for (String db : dbs) {
        buffer.append(getDatabase(db).dump());
      }

      // DUMP HA STATS
      System.out.println(buffer);

    } catch (Exception e) {
      logger.errorNode(nodeName, "Error on printing HA stats", e);
    }
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) throws IOException {
    if (rNodeName == null) throw new IllegalArgumentException("Server name is NULL");

    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    ORemoteServerController remoteServer = ctx.getRemoteServer(rNodeName);
    if (remoteServer == null) {
      Member member = clusterManager.getClusterMemberByName(rNodeName);

      for (int retry = 0; retry < 20; ++retry) {
        ONodeConfig cfg = getNodeConfigurationByUuid(member.getUuid(), false);
        if (cfg == null || cfg.getListeners() == null) {
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

        final String userPassword = cfg.getReplicator();

        if (userPassword != null) {
          try {
            remoteServer =
                ctx.connectRemoteServer(new ONodeId(rNodeName), url, REPLICATOR_USER, userPassword);
            break;
          } catch (ONetworkProtocolException | IOException e) {
            logger.warn("failing to connect to remote node %s", rNodeName, e);
          }
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

  public void onNodeJoined(String joinedNodeName, String url, String userPassword, Member member) {
    try {
      getRemoteServer(joinedNodeName);
    } catch (IOException e) {
      logger.errorOut(nodeName, joinedNodeName, "Error on connecting to node %s", joinedNodeName);
    }
    ((OrientDBDistributed) serverInstance.getDatabases())
        .connected(new ONodeId(joinedNodeName), url, REPLICATOR_USER, userPassword);

    logger.infoIn(
        nodeName,
        clusterManager.getNodeName(member, true),
        "Added node configuration id=%s name=%s, now %d nodes are configured",
        member,
        clusterManager.getNodeName(member, true),
        getActiveServers().size());

    // NOTIFY NODE WAS ADDED SUCCESSFULLY
    for (ODistributedLifecycleListener l : listeners) l.onNodeJoined(joinedNodeName);

    // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN ONLY IF AUTO-DEPLOY IS SET
    dumpServersStatus();
  }

  // This is used only during startup and gets called by the cluster metadata manager
  public void connectToAllNodes(Set<String> clusterNodes) throws IOException {
    for (String m : clusterNodes) if (!m.equals(nodeName)) getRemoteServer(m);
  }

  @Override
  public DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName) {
    return ((OrientDBDistributed) serverInstance.getDatabases())
        .getDatabaseStatus(iNode, iDatabaseName);
  }

  // Called to notify this server, that a node has been removed from the cluster
  public void onServerRemoved(String nodeName) {
    closeRemoteServer(nodeName);
  }

  // Called when the db config has changed
  public void onDbConfigUpdated(String databaseName, ODocument config) {
    // SEND A DISTRIBUTED MSG TO ALL THE SERVERS
    final Set<String> servers = new HashSet<String>(getActiveServers());
    servers.remove(nodeName);

    if (!servers.isEmpty() && getDatabase(databaseName) != null) {

      final ODistributedResponse dResponse =
          sendRequest(
              databaseName, servers, new OUpdateDatabaseConfigurationTask(databaseName, config));
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
  public OClusterConfiguration getClusterConfiguration() {
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
  public ONodeConfig getNodeConfigurationByUuid(String iNode, boolean useCache) {
    return clusterManager.getNodeConfigurationByUuid(iNode, useCache);
  }

  public HazelcastInstance getHazelcastInstance() {
    return clusterManager.getHazelcastInstance();
  }

  @Override
  public ODistributedDatabaseImpl getDatabase(String name) {
    return ((OrientDBDistributed) getServerInstance().getDatabases()).getDatabase(name);
  }
}
