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
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
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
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
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
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastClusterMetadataManager;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import sun.misc.Signal;

/**
 * Plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedPlugin extends OServerPluginAbstract implements ODistributedServerManager {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODistributedPlugin.class);

  protected static final String PAR_DEF_DISTRIB_DB_CONFIG = "configuration.db.default";
  protected static final String NODE_NAME_ENV = "ORIENTDB_NODE_NAME";

  private OServer serverInstance;
  private String nodeName = null;
  protected File defaultDatabaseConfigFile;
  protected List<ODistributedLifecycleListener> listeners = new ArrayList<>();

  protected static final int DEPLOY_DB_MAX_RETRIES = 10;
  protected Set<String> installingDatabases =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  protected ODistributedStrategy responseManagerFactory = new ODefaultDistributedStrategy();

  private volatile String lastServerDump = "";

  private OCancellableTimer haStatsTask = null;
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

    try {
      clusterManager.configHazelcastPlugin(oServer, iParams, nodeName);
    } catch (FileNotFoundException e) {
      throw OException.wrapException(
          new ODatabaseException("Error loading hazelcast configuration"), e);
    }
    String contextName = clusterManager.getHazelcastConfig().getGroupConfig().getName();
    String contextPassword = clusterManager.getHazelcastConfig().getGroupConfig().getName();
    if (nodeName == null) assignNodeName();
    ((OrientDBDistributed) serverInstance.getDatabases())
        .initDistributed(nodeName, contextName, 1, contextPassword);
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
    try {
      clusterManager.startupHazelcastPlugin();

      OContextConfiguration ctx = serverInstance.getContextConfiguration();
      final long statsDelay = ctx.getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DUMP_STATS_EVERY);
      if (statsDelay > 0) {
        haStatsTask = databases.periodicExecute(this::dumpStats, statsDelay);
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

    clusterManager.prepareHazelcastPluginShutdown();
    try {
      if (haStatsTask != null) haStatsTask.cancel();

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

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public ODistributedResponse sendRequest(
      final String iDatabaseName, final Collection<ONodeId> iTargetNodes, final ORemoteTask iTask) {
    return sendRequest(iDatabaseName, iTargetNodes, iTask, nextRequestId(), null, null);
  }

  @Override
  public ODistributedResponse sendSingleRequest(
      String databaseName, ONodeId node, ORemoteTask iTask) {
    return sendRequest(
        databaseName, Collections.singletonList(node), iTask, nextRequestId(), null, null);
  }

  public ODistributedResponse sendRequest(
      final String iDatabaseName,
      final Collection<ONodeId> iTargetNodes,
      final ORemoteTask iTask,
      final ODistributedRequestId reqId,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    final ODistributedRequest req =
        new ODistributedRequest(ctx.getTaskFactoryManager(), reqId, iDatabaseName, iTask);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      logger.errorOut(
          nodeName, null, "No nodes configured for partition '%s' request: %s", iDatabaseName, req);
      throw new ODistributedException(
          "No nodes configured '" + iDatabaseName + "' request: " + req);
    }

    ctx.getMessageService().updateMessageStats(iTask.getName());
    if (responseManagerFactory != null) {
      return send2Nodes(req, iTargetNodes, localResult, responseManagerFactory);
    } else {
      return send2Nodes(req, iTargetNodes, localResult);
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
      final ODistributedRequest request,
      Collection<ONodeId> nodes,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    try {
      checkForServerOnline(request);

      final String databaseName = request.getDatabaseName();

      if (nodes.isEmpty()) {
        logger.errorOut(
            this.nodeName,
            null,
            "No nodes configured for database '%s' request: %s",
            databaseName,
            request);
        throw new ODistributedException(
            "No nodes configured for partition '" + databaseName + "' request: " + request);
      }
      final ORemoteTask task = request.getTask();
      final boolean checkNodesAreOnline = task.isNodeOnlineRequired();

      final Set<ONodeId> nodesConcurToTheQuorum;
      int availableNodes = nodes.size();
      int onlineMasters;
      if (databaseName != null) {
        nodesConcurToTheQuorum =
            getDistributedStrategy().getNodesConcurInQuorum(this, databaseName, request, nodes);

        // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE
        // NODES

        if (checkNodesAreOnline) {
          availableNodes =
              ctx.getNodesWithStatus(
                  nodes,
                  databaseName,
                  ODistributedServerManager.DB_STATUS.ONLINE,
                  ODistributedServerManager.DB_STATUS.BACKUP,
                  ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
        }

        // all online masters
        onlineMasters = ctx.getOnlineMasters(databaseName);
      } else {
        nodesConcurToTheQuorum = new HashSet<>(nodes);
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

      final boolean waitLocalNode = waitForLocalNode(null, nodes);

      // CREATE THE RESPONSE MANAGER
      final ODistributedResponseManager currentResponseMgr =
          responseManagerFactory.newResponseManager(
              request,
              nodes,
              task,
              nodesConcurToTheQuorum,
              availableNodes,
              expectedResponses,
              quorum,
              groupByResponse,
              waitLocalNode);

      if (localResult != null && currentResponseMgr.setLocalResult(ctx.getNodeId(), localResult)) {
        // COLLECT LOCAL RESULT ONLY
        return currentResponseMgr.getFinalResponse();
      }

      // SORT THE NODE TO GUARANTEE THE SAME ORDER OF DELIVERY
      if (!(nodes instanceof List)) nodes = new ArrayList<ONodeId>(nodes);
      if (nodes.size() > 1) Collections.sort((List<ONodeId>) nodes);

      ctx.getMessageService().registerRequest(request.getId().getMessageId(), currentResponseMgr);

      for (ONodeId node : nodes) {
        // CATCH ANY EXCEPTION LOG IT AND IGNORE TO CONTINUE SENDING REQUESTS TO OTHER NODES
        try {
          if (ctx.getNodeId().equals(node)) {
            ctx.executeDistributedRequest(request);
          } else {
            final ORemoteServerController remoteServer = ctx.getRemoteServer(node);
            remoteServer.sendRequest(request);
          }

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
              final ORemoteServerController remoteServer = ctx.getRemoteServer(node);
              remoteServer.sendRequest(request);
              continue;

            } catch (Exception ex) {
              // IGNORE IT BECAUSE MANAGED BELOW
            }
          }

          logger.warnOut(
              this.nodeName,
              node.getNode(),
              "Error on sending distributed request %s (err=%s). Active nodes: %s",
              request,
              reason,
              ctx.getAvailableNodeNames(databaseName));
        }
      }

      if (currentResponseMgr.getExpectedNodes().isEmpty())
        // NO SERVER TO SEND A MESSAGE
        throw new ODistributedException(
            "No server active for distributed request ("
                + request
                + ") against database '"
                + databaseName
                + "' to nodes "
                + nodes);

      if (databaseName != null) {
        ODistributedDatabaseImpl shared = getDatabase(databaseName);
        if (shared != null) {
          shared.incSentRequest();
        }
      }

      return waitForResponse(request, currentResponseMgr);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODistributedException(
              "Error on executing distributed request ("
                  + request
                  + ") against database '"
                  + this.nodeName
                  + "' to nodes "
                  + nodes),
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
      final Collection<ONodeId> iNodes, final long timeout, final ODistributedRequestId requestId) {
    long delta = 0;
    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    if (iNodes != null)
      for (ONodeId n : iNodes) {
        // UPDATE THE TIMEOUT WITH THE CURRENT SERVER LATENCY
        final long l = ctx.getMessageService().getCurrentLatency(n);
        delta = Math.max(delta, l);
      }

    return timeout + delta;
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest, Collection<ONodeId> iNodes, final Object localResult) {
    return send2Nodes(
        iRequest,
        iNodes,
        localResult,
        (iRequest1,
            iNodes1,
            task,
            nodesConcurToTheQuorum,
            availableNodes,
            expectedResponses,
            quorum,
            groupByResponse,
            waitLocalNode) ->
            new ODistributedResponseManagerImpl(
                this,
                getServerInstance().getDatabases(),
                iRequest,
                iNodes,
                nodesConcurToTheQuorum,
                quorum,
                waitLocalNode,
                adjustTimeoutWithLatency(
                    iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
                groupByResponse));
  }

  protected boolean waitForLocalNode(
      final ODistributedConfiguration cfg, final Collection<ONodeId> iNodes) {
    boolean waitLocalNode = false;
    var localId = getServerInstance().getDatabases().getNodeId();
    if (iNodes.contains(localId)) {
      if (cfg != null) {
        if (cfg.isReadYourWrites(null)) waitLocalNode = true;
      } else {
        waitLocalNode = true;
      }
    }
    return waitLocalNode;
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public String toString() {
    return nodeName;
  }

  @Override
  public ODistributedMessageService getMessageService() {
    return ((OrientDBDistributed) serverInstance.getDatabases()).getMessageService();
  }

  public ODistributedStrategy getDistributedStrategy() {
    return responseManagerFactory;
  }

  public void setDistributedStrategy(final ODistributedStrategy streatgy) {
    this.responseManagerFactory = streatgy;
  }

  public void notifyClients(String databaseName) {
    List<String> hosts = new ArrayList<>();
    var ctx = (OrientDBDistributed) serverInstance.getDatabases();
    for (ONodeId name : ctx.getOps().getNetworkTopology().getMembers()) {
      ONodeConfig memberConfig = ctx.getNodeConfiguration(name);
      if (memberConfig != null) {
        final Collection<ONodeListenerConfig> listeners = memberConfig.getListeners();
        if (listeners != null)
          for (ONodeListenerConfig listener : listeners) {
            if (listener.getProtocol().equals("ONetworkProtocolBinary")) {
              hosts.add(listener.getListen());
            }
          }
      }
    }
    serverInstance.getPushManager().pushDistributedConfig(databaseName, hosts);
  }

  public void onDatabaseEvent(
      final String nodeName, final String databaseName, final DB_STATUS status) {
    notifyClients(databaseName);
    invokeOnDatabaseStatusChange(nodeName, databaseName, status);
  }

  public void invokeOnDatabaseStatusChange(
      final String node, final String databaseName, final DB_STATUS status) {
    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      try {
        l.onDatabaseChangeStatus(node, databaseName, status);
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
    return new ODistributedRequestId(getServerInstance().getNodeId(), getNextMessageIdCounter());
  }

  public long getNextMessageIdCounter() {
    return ((OrientDBDistributed) serverInstance.getDatabases()).getNextMessageIdCounter();
  }

  @Override
  public void updateLastClusterChange() {
    clusterManager.updateLastClusterChange();
  }

  public void closeRemoteServer(final ONodeId node) {
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

  public ORemoteServerController getRemoteServer(final ONodeId rNodeId) throws IOException {
    if (rNodeId == null) throw new IllegalArgumentException("Server name is NULL");

    OrientDBDistributed ctx = (OrientDBDistributed) serverInstance.getDatabases();
    ORemoteServerController remoteServer = ctx.getRemoteServer(rNodeId);
    if (remoteServer == null) {
      Member member = clusterManager.getClusterMemberByNodeId(rNodeId);

      for (int retry = 0; retry < 20; ++retry) {
        ONodeConfig cfg = getNodeConfigurationByUuid(member.getUuid(), false);
        if (cfg == null || cfg.getListeners() == null) {
          try {
            Thread.sleep(100);
            member = clusterManager.getClusterMemberByNodeId(rNodeId);
            continue;

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw OException.wrapException(
                new ODistributedException("Cannot find node '" + rNodeId + "'"), e);
          }
        }

        final String url = ODistributedPlugin.getListeningBinaryAddress(cfg);

        if (url == null) {
          closeRemoteServer(rNodeId);
          throw new ODatabaseException(
              "Cannot connect to a remote node because the url was not found");
        }

        try {
          remoteServer = ctx.connectRemoteServer(rNodeId, url);
          break;
        } catch (ONetworkProtocolException | IOException e) {
          logger.warn("failing to connect to remote node %s", rNodeId, e);
        }

        // RETRY TO GET USR+PASSWORD IN A WHILE
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException.wrapException(
              new OInterruptedException("Cannot connect to remote server " + rNodeId), e);
        }
      }
    }

    if (remoteServer == null) throw new ODistributedException("Cannot find node '" + rNodeId + "'");

    return remoteServer;
  }

  @Override
  public long getLastClusterChangeOn() {
    return clusterManager.getLastClusterChangeOn();
  }

  public void onNodeJoined(ONodeId joinedNodeId, String url) {
    try {
      getRemoteServer(joinedNodeId);
    } catch (IOException e) {
      logger.errorOut(
          nodeName, joinedNodeId.getNode(), "Error on connecting to node %s", joinedNodeId);
    }
    ((OrientDBDistributed) serverInstance.getDatabases()).connected(joinedNodeId, url);

    // NOTIFY NODE WAS ADDED SUCCESSFULLY
    notifyNodeJoined(joinedNodeId);

    // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN ONLY IF AUTO-DEPLOY IS SET
    dumpServersStatus();
  }

  public void notifyNodeJoined(ONodeId joinedNodeName) {
    for (ODistributedLifecycleListener l : listeners) l.onNodeJoined(joinedNodeName.getNode());
  }

  public void notifyNodeLeft(ONodeId joinedNodeName) {
    for (ODistributedLifecycleListener l : listeners) l.onNodeLeft(joinedNodeName.getNode());
  }

  // This is used only during startup and gets called by the cluster metadata manager
  public void connectToAllNodes(Set<ONodeId> clusterNodes) throws IOException {
    for (ONodeId m : clusterNodes) {
      if (!m.equals(serverInstance.getDatabases().getNodeId())) {
        getRemoteServer(m);
      }
    }
  }

  @Override
  public DB_STATUS getDatabaseStatus(ONodeId iNode, String iDatabaseName) {
    return ((OrientDBDistributed) serverInstance.getDatabases())
        .getDatabaseStatus(iNode, iDatabaseName);
  }

  // Called to notify this server, that a node has been removed from the cluster
  public void onServerRemoved(ONodeId nodeName) {
    closeRemoteServer(nodeName);
  }

  @Override
  public OClusterConfiguration getClusterConfiguration() {
    if (!enabled) return null;

    return clusterManager.getClusterConfiguration();
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
