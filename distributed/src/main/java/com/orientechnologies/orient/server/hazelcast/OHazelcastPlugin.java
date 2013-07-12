/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.hazelcast;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedThreadLocal;
import com.orientechnologies.orient.server.distributed.OReplicationConfig;
import com.orientechnologies.orient.server.distributed.OServerOfflineException;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAlignRequestTask;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {
  protected static final String        DISTRIBUTED_EXECUTOR_NAME = "OHazelcastPlugin::Executor";
  protected static final int           SEND_RETRY_MAX            = 100;

  protected int                        nodeNumber;
  protected String                     localNodeId;
  protected String                     configFile                = "hazelcast.xml";
  protected Map<String, Member>        remoteClusterNodes        = new ConcurrentHashMap<String, Member>();
  protected long                       timeOffset;
  protected long                       runId                     = -1;
  protected volatile String            status                    = "starting";
  protected Map<String, Boolean>       pendingAlignments         = new HashMap<String, Boolean>();
  protected TimerTask                  alignmentTask;
  protected String                     membershipListenerRegistration;
  protected Map<Long, Long>            executionQueue            = new HashMap<Long, Long>();
  protected Object                     lockQueue                 = new Object();

  protected volatile HazelcastInstance hazelcastInstance;

  public OHazelcastPlugin() {
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast"))
        configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    remoteClusterNodes.clear();
    synchronizers.clear();

    try {
      hazelcastInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(configFile));
      localNodeId = getNodeId(hazelcastInstance.getCluster().getLocalMember());

      OServer.registerServerInstance(localNodeId, serverInstance);

      initDistributedDatabases();
    } catch (FileNotFoundException e) {
      throw new OConfigurationException("Error on creating Hazelcast instance", e);
    }

    final IMap<String, Object> configurationMap = getConfigurationMap();
    configurationMap.addEntryListener(this, true);

    setStatus("aligning");

    // GET AND REGISTER THE CLUSTER RUN ID IF NOT PRESENT
    configurationMap.putIfAbsent("runId", hazelcastInstance.getCluster().getClusterTime());
    runId = (Long) getConfigurationMap().get("runId");

    // COMPUTE THE DELTA BETWEEN LOCAL AND CENTRAL CLUSTER TIMES
    timeOffset = System.currentTimeMillis() - getHazelcastInstance().getCluster().getClusterTime();

    // REGISTER CURRENT MEMBERS
    registerAndAlignNodes();

    super.startup();
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    if (alignmentTask != null)
      alignmentTask.cancel();

    super.shutdown();

    remoteClusterNodes.clear();
    if (membershipListenerRegistration != null) {
      hazelcastInstance.getCluster().removeMembershipListener(membershipListenerRegistration);
    }
  }

  @Override
  public long incrementDistributedSerial(final String iDatabaseName) {
    return hazelcastInstance.getAtomicLong("db." + iDatabaseName).incrementAndGet();
  }

  @Override
  public long getRunId() {
    return runId;
  }

  public Map<String, Object> propagate(final Set<String> iNodeIds, final OAbstractRemoteTask<? extends Object> iTask)
      throws ODistributedException {
    final Map<String, Object> result = new HashMap<String, Object>();

    ODistributedServerLog.debug(this, iTask.getNodeSource(), iNodeIds.toString(), DIRECTION.OUT, "propagate %s oper=%d.%d", iTask
        .getName().toUpperCase(), iTask.getRunId(), iTask.getOperationSerial());

    for (String nodeId : iNodeIds) {
      final Member m = remoteClusterNodes.get(nodeId);
      if (m == null)
        ODistributedServerLog.warn(this, getLocalNodeId(), nodeId, DIRECTION.OUT,
            "cannot propagate operation on remote member because is disconnected");
      else
        result.put(nodeId, sendOperation2Node(nodeId, iTask));
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public Object sendOperation2Node(final String iNodeId, final OAbstractRemoteTask<? extends Object> iTask) {
    iTask.setNodeDestination(iNodeId);
    Member member = remoteClusterNodes.get(iNodeId);
    if (member == null) {
      // CHECK IF IS ENTERING IN THE CLUSTER AND HASN'T BEEN REGISTERED YET
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (getNodeId(m).equals(iNodeId)) {
          member = m;
          break;
        }
      }

      if (member == null)
        throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");
    }

    final Member clusterMember = member;

    ExecutionCallback<Object> callback = null;
    if (iTask.getMode() == EXECUTION_MODE.ASYNCHRONOUS)
      callback = new ExecutionCallback<Object>() {
        @Override
        public void onResponse(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
          ODistributedServerLog.error(this, getLocalNodeId(), iNodeId, DIRECTION.OUT,
              "error on execution of operation in ASYNCH mode", t);
        }
      };

    for (int retry = 0; retry < SEND_RETRY_MAX; ++retry) {
      try {

        Object result = executeOperation((Callable<Object>) iTask, clusterMember, iTask.getMode(), callback);

        // OK
        return result;

      } catch (ExecutionException e) {
        if (e.getCause() instanceof OServerOfflineException) {
          final OServerOfflineException exc = (OServerOfflineException) e.getCause();

          // RETRY
          ODistributedServerLog.warn(this, getLocalNodeId(), exc.getNodeId(), DIRECTION.OUT,
              "remote node %s is not online (status=%s), retrying %d...", exc.getNodeStatus(), retry + 1);
          // WAIT A BIT
          try {
            Thread.sleep(200 + (retry * 50));
          } catch (InterruptedException ex) {
            Thread.interrupted();
          }

        } else {
          ODistributedServerLog.error(this, getLocalNodeId(), iNodeId, DIRECTION.OUT, "error on execution of operation in %s mode",
              e, EXECUTION_MODE.SYNCHRONOUS);
          throw new ODistributedException("Error on executing remote operation in " + iTask.getMode() + " mode against node: "
              + member, e);
        }

      } catch (Exception e) {
        // WRAP IT
        ODistributedServerLog.error(this, getLocalNodeId(), iNodeId, DIRECTION.OUT, "error on execution of operation in %s mode",
            e, iTask.getMode());
        throw new ODistributedException("Error on executing remote operation in " + iTask.getMode() + " mode against node: "
            + member, e);
      }
    }

    throw new ODistributedException("Cannot complete the operation because the cluster is offline");
  }

  public Object execute(final String iClusterName, final Object iKey, final OAbstractRemoteTask<? extends Object> iTask,
      OReplicationConfig replicationData) throws ExecutionException {

    final String dbName = iTask.getDatabaseName();

    String masterNodeId = null;

    try {
      if (replicationData == null) {
        // NO REPLICATION: LOCAL ONLY
        ODistributedThreadLocal.INSTANCE.set(iTask.getNodeSource());
        try {
          // EXECUTE IT LOCALLY
          return ((OAbstractRemoteTask<? extends Object>) iTask).executeOnLocalNode();
        } finally {
          // SET LAST EXECUTION SERIAL
          ODistributedThreadLocal.INSTANCE.set(null);
        }

      } else {
        if (replicationData != null) {
          // SET THE DESTINATION NODE
          iTask.setNodeDestination(replicationData.masterNode);
          replicationData.masterNode = waitUntilMasterNodeIsOnline(iClusterName, iKey, dbName, replicationData.masterNode);
          masterNodeId = replicationData.masterNode;
        }

        if (getLocalNodeId().equals(replicationData.masterNode))
          // LOCAL + PROPAGATE
          return executeLocallyAndPropagate((OAbstractReplicatedTask<? extends Object>) iTask);
        else
          // REMOTE + LOCAL
          return executeRemotelyAndApplyLocally(iClusterName, iKey, (OAbstractReplicatedTask<? extends Object>) iTask, dbName,
              replicationData);
      }
    } catch (InterruptedException e) {
      Thread.interrupted();

    } catch (Exception e) {
      ODistributedServerLog.error(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
          "error on execution of operation in %s mode", e, EXECUTION_MODE.SYNCHRONOUS);
      throw new ExecutionException("error on execution of operation in " + EXECUTION_MODE.SYNCHRONOUS + " mode against node "
          + masterNodeId, e);
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  protected Object executeRemotelyAndApplyLocally(final String iClusterName, final Object iKey,
      final OAbstractReplicatedTask<? extends Object> iTask, final String dbName, final OReplicationConfig iReplicationData)
      throws InterruptedException, Exception, ExecutionException {

    // RETRY UNTIL SUCCEED
    for (int retry = 0; retry < SEND_RETRY_MAX; ++retry) {
      ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
          "routing %s against db=%s in %s mode...", iTask.getName().toUpperCase(), dbName, EXECUTION_MODE.SYNCHRONOUS);

      try {
        // EXECUTES ON THE TARGET NODE
        ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
            "remote execution %s db=%s mode=%s oper=%d.%d...", iTask.getName().toUpperCase(), dbName, iTask.getMode(),
            iTask.getRunId(), iTask.getOperationSerial());

        final Object remoteResult = executeOperation((Callable<Object>) iTask, iKey, EXECUTION_MODE.SYNCHRONOUS, null);

        final Object localResult;
        if (iTask instanceof OAbstractReplicatedTask<?>) {
          // APPLY LOCALLY TOO
          ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.IN,
              "local execution %s against db=%s mode=%s oper=%d.%d...", iTask.getName().toUpperCase(), dbName, iTask.getMode(),
              iTask.getRunId(), iTask.getOperationSerial());

          localResult = enqueueLocalExecution(iTask);

          // CHECK CONFLICT
          if (remoteResult != null && localResult != null)
            if (!remoteResult.equals(localResult)) {
              ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.IN,
                  "detected conflict on %s mode=%s db=%s oper=%d.%d: remote={%s} != local={%s}", iTask.getName().toUpperCase(),
                  EXECUTION_MODE.SYNCHRONOUS, dbName, iTask.getRunId(), iTask.getOperationSerial(), remoteResult, localResult);

              iTask.handleConflict(iTask.getNodeDestination(), localResult, remoteResult);
            }

        } else
          localResult = remoteResult;

        // OK
        return localResult;

      } catch (MemberLeftException e) {
        // RETRY
        ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
            "error on execution of operation in %s mode, because node left. Re-route it in transparent way", e,
            EXECUTION_MODE.SYNCHRONOUS);

        return execute(iClusterName, iKey, iTask, iReplicationData);

      } catch (ExecutionException e) {
        if (e.getCause() instanceof OServerOfflineException) {
          // RETRY
          ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
              "remote node is not online, retrying %d...", retry + 1);
          // WAIT A BIT
          try {
            Thread.sleep(200 + (retry * 50));
          } catch (InterruptedException ex) {
            Thread.interrupted();
          }
        } else {
          ODistributedServerLog.error(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
              "error on execution of operation in %s mode", e, EXECUTION_MODE.SYNCHRONOUS);
          throw e;
        }
      }
    }

    ODistributedServerLog.error(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
        "error on execution %s in %s mode", iTask.getName(), EXECUTION_MODE.SYNCHRONOUS);

    // NEVER HAPPENS BECAUSE .error() THROWS AN EXCEPTION
    throw new ODistributedException("Error on execution " + iTask.getName() + " in " + EXECUTION_MODE.SYNCHRONOUS + " mode");
  }

  private Object executeLocallyAndPropagate(final OAbstractReplicatedTask<? extends Object> iTask) throws Exception {
    // LOCAL EXECUTION AVOID TO USE EXECUTORS
    final Object localResult = enqueueLocalExecution(iTask);

    final Set<String> targetNodes = getRemoteNodeIdsBut(iTask.getNodeSource(), iTask.getNodeDestination());
    if (!targetNodes.isEmpty()) {
      // RESET THE SOURCE TO AVOID LOOPS
      iTask.setNodeSource(getLocalNodeId());

      final Map<String, Object> remoteResults = propagate(targetNodes, iTask);

      for (Entry<String, Object> entry : remoteResults.entrySet()) {
        final String remoteNode = entry.getKey();
        final Object remoteResult = entry.getValue();

        if ((localResult == null && remoteResult != null) || (localResult != null && remoteResult == null)
            || (localResult != null && !localResult.equals(remoteResult))) {
          // CONFLICT
          iTask.handleConflict(remoteNode, localResult, remoteResult);
        }
      }
    }

    return localResult;
  }

  public boolean isLocalNodeMaster(final Object iKey) {
    final Member partitionOwner = hazelcastInstance.getPartitionService().getPartition(iKey).getOwner();
    final boolean local = partitionOwner.equals(hazelcastInstance.getCluster().getLocalMember());

    ODistributedServerLog.debug(this, getLocalNodeId(), null, DIRECTION.NONE,
        "network partition: check for local master: key '%s' is assigned to %s (local=%s)", iKey, getNodeId(partitionOwner), local);

    return local;
  }

  /**
   * Returns the replication data, or null if replication is not active.
   */
  public OReplicationConfig getReplicationData(final String iDatabaseName, final String iClusterName, final Object iKey,
      final String iLocalNodeId, final String iRemoteNodeId) {

    final ODocument cfg = getDatabaseClusterConfiguration(iDatabaseName, iClusterName);
    final Boolean active = cfg.field("synchronization");
    if (active == null || !active)
      // NOT ACTIVE, RETURN
      return null;

    final OReplicationConfig data = new OReplicationConfig();
    data.masterNode = cfg.field("master");
    if (data.masterNode == null) {
      ODistributedServerLog
          .warn(
              this,
              getLocalNodeId(),
              null,
              DIRECTION.NONE,
              "network partition: found wrong configuration for database '%s': cannot find the 'master' field for the cluster '%s'. '$auto' will be used",
              iDatabaseName, iClusterName);
      data.masterNode = MASTER_AUTO;
    }

    if (data.masterNode.startsWith("$"))
      // GET THE MASTER NODE BY USING THE STRATEGY FACTORY
      data.masterNode = getReplicationStrategy(data.masterNode).getNode(this, iClusterName, iKey);

    if (data.masterNode == null)
      throw new ODistributedException("Cannot find a master node for the key '" + iKey + "'");

    final boolean local = data.masterNode.equals(getLocalNodeId());
    ODistributedServerLog.debug(this, getLocalNodeId(), "?", DIRECTION.OUT, "master node for %s%s%s -> %s (local=%s)",
        iClusterName != null ? "cluster=" + iClusterName + " " : "", iKey != null ? "key=" + iKey : "", iClusterName == null
            && iKey == null ? "default operation" : "", data.masterNode, local);

    final Set<String> targetNodes = getRemoteNodeIdsBut(iLocalNodeId, iRemoteNodeId);
    if (!targetNodes.isEmpty())
      data.synchReplicas = targetNodes.toArray(new String[targetNodes.size()]);

    return data;
  }

  @Override
  public ODocument getDatabaseConfiguration(final String iDatabaseName) {
    // SEARCH IN THE CLUSTER'S DISTRIBUTED CONFIGURATION
    final IMap<String, Object> distributedConfiguration = getConfigurationMap();
    ODocument cfg = (ODocument) distributedConfiguration.get("db." + iDatabaseName);

    if (cfg == null) {
      cfg = super.getDatabaseConfiguration(iDatabaseName);
      // STORE IT IN THE CLUSTER CONFIGURATION
      distributedConfiguration.put("db." + iDatabaseName, cfg);
    } else {
      // SAVE THE MOST RECENT CONFIG LOCALLY
      saveDatabaseConfiguration(iDatabaseName, cfg);
    }
    return cfg;
  }

  @Override
  public ODocument getDatabaseStatus(final String iDatabaseName) {
    final ODocument status = new ODocument();
    status.field("configuration", getDatabaseConfiguration(iDatabaseName), OType.EMBEDDED);
    status.field("cluster", getClusterConfiguration(), OType.EMBEDDED);
    return status;
  }

  @Override
  public ODocument getClusterConfiguration() {
    if (!enabled)
      return null;

    final ODocument cluster = new ODocument();

    final HazelcastInstance instance = getHazelcastInstance();

    cluster.field("name", instance.getName());
    cluster.field("local", getNodeId(instance.getCluster().getLocalMember()));

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    members.add(getLocalNodeConfiguration());
    for (Member member : remoteClusterNodes.values()) {
      members.add(getNodeConfiguration(getNodeId(member)));
    }

    return cluster;
  }

  public ODocument getNodeConfiguration(final String iNode) {
    return (ODocument) getConfigurationMap().get("node." + iNode);
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();

    nodeCfg.field("alias", getLocalNodeAlias());
    nodeCfg.field("id", getLocalNodeId());
    nodeCfg.field("status", getStatus());

    List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : serverInstance.getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress());
    }
    return nodeCfg;
  }

  public String getStatus() {
    return status;
  }

  public boolean checkStatus(final String iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  public void setStatus(final String iStatus) {
    if (status.equals(iStatus))
      // NO CHANGE
      return;

    status = iStatus;

    final IMap<String, Object> map = getConfigurationMap();
    final String nodeName = "node." + getLocalNodeId();
    final ODocument nodeConfiguration = getLocalNodeConfiguration();
    map.put(nodeName, nodeConfiguration);

    ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.NONE, "updated node status to '%s'", status);
  }

  public void registerAndAlignNodes() {
    membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

    // COLLECTS THE MEMBER LIST
    for (Member clusterMember : hazelcastInstance.getCluster().getMembers()) {
      final String nodeId = getNodeId(clusterMember);
      if (!getLocalNodeId().equals(nodeId))
        remoteClusterNodes.put(nodeId, clusterMember);
    }

    if (remoteClusterNodes.isEmpty())
      ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.NONE, "no node running has been detected");
    else
      ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.NONE, "detected %d running nodes %s",
          remoteClusterNodes.size(), remoteClusterNodes.keySet());

    if (!alignmentStartup)
      // NO ALIGNMENT: THE NODE IS ONLINE
      setStatus("online");
    else if (remoteClusterNodes.isEmpty())
      // NO NODES; AVOID ALIGNMENT
      setStatus("online");
    else
      alignNodes();

    if (alignmentTimer > 0) {
      // SCHEDULE THE AUTO ALIGNMENT
      alignmentTask = new TimerTask() {
        @Override
        public void run() {
          alignNodes();
        }
      };

      Orient.instance().getTimer().schedule(alignmentTask, alignmentTimer, alignmentTimer);
    }
  }

  protected void alignNodes() {
    if (remoteClusterNodes.isEmpty())
      // NO NODES; AVOID ALIGNMENT
      return;

    setStatus("aligning");

    // EXECUTE THE ALIGNMENT: THE STATUS ONLINE WILL BE SET ASYNCHRONOUSLY ONCE FINISHED
    synchronized (synchronizers) {

      for (Entry<String, OStorageSynchronizer> entry : synchronizers.entrySet()) {
        final String databaseName = entry.getKey();
        try {
          final long[] lastOperationId = entry.getValue().getLog().getLastOperationId(ODatabaseJournal.OPERATION_STATUS.COMMITTED);

          if (lastOperationId[0] == -1 && lastOperationId[1] == -1)
            // AVOID TO SEND THE REQUEST IF THE LOG IS EMPTY
            continue;

          ODistributedServerLog
              .warn(this, getLocalNodeId(), remoteClusterNodes.keySet().toString(), DIRECTION.OUT,
                  "sending align request in broadcast for database %s from %d:%d", databaseName, lastOperationId[0],
                  lastOperationId[1]);

          synchronized (pendingAlignments) {
            for (String node : remoteClusterNodes.keySet()) {
              pendingAlignments.put(node + "/" + databaseName, Boolean.FALSE);

              ODistributedServerLog.info(this, getLocalNodeId(), node, DIRECTION.NONE, "setting node in alignment state for db=%s",
                  databaseName);
            }
          }

          propagate(remoteClusterNodes.keySet(), new OAlignRequestTask(serverInstance, this, databaseName,
              EXECUTION_MODE.ASYNCHRONOUS, lastOperationId[0], lastOperationId[1]));

        } catch (IOException e) {
          ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.OUT,
              "error on retrieve last operation id from the log for db=%s", databaseName);
        }
      }

      if (pendingAlignments.isEmpty())
        setStatus("online");
    }
  }

  @Override
  public void endAlignment(final String iNode, final String iDatabaseName) {
    synchronized (pendingAlignments) {
      if (pendingAlignments.remove(iNode + "/" + iDatabaseName) == null) {
        ODistributedServerLog.error(this, getLocalNodeId(), iNode, DIRECTION.OUT,
            "received response for an alignment against an unknown node %s database %s", iDatabaseName);
      }

      if (pendingAlignments.isEmpty())
        setStatus("online");
      else {
        // WAKE UP ALL THE POSTPONED ALIGNMENTS
        for (Entry<String, Boolean> entry : pendingAlignments.entrySet()) {
          final String[] parts = entry.getKey().split("/");
          final String node = parts[0];
          final String databaseName = parts[1];

          if (entry.getValue()) {
            final OStorageSynchronizer synch = synchronizers.get(databaseName);

            long[] lastOperationId;

            try {
              lastOperationId = synch.getLog().getLastOperationId(ODatabaseJournal.OPERATION_STATUS.COMMITTED);

              ODistributedServerLog.info(this, getLocalNodeId(), node, DIRECTION.OUT, "resend alignment request db=%s from %d:%d",
                  databaseName, lastOperationId[0], lastOperationId[1]);

              sendOperation2Node(node, new OAlignRequestTask(serverInstance, this, databaseName, EXECUTION_MODE.ASYNCHRONOUS,
                  lastOperationId[0], lastOperationId[1]));

            } catch (IOException e) {
              ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.OUT,
                  "error on retrieve last operation id from the log for db=%s", databaseName);
            }
          } else
            ODistributedServerLog.info(this, getLocalNodeId(), node, DIRECTION.NONE,
                "db=%s is in alignment status yet, the node is not online yet", databaseName);
        }
      }
    }
  }

  @Override
  public void postponeAlignment(final String iNode, final String iDatabaseName) {
    synchronized (pendingAlignments) {
      final String key = iNode + "/" + iDatabaseName;
      if (!pendingAlignments.containsKey(key)) {
        ODistributedServerLog.error(this, getLocalNodeId(), iNode, DIRECTION.IN,
            "received response to postpone an alignment against an unknown node", iDatabaseName);
      }

      pendingAlignments.put(key, Boolean.TRUE);
    }
  }

  public long getTimeOffset() {
    return timeOffset;
  }

  public String getLocalNodeId() {
    return localNodeId;
  }

  public String getLocalNodeAlias() {
    if (alias != null)
      return alias;

    return getLocalNodeId();
  }

  public String getNodeId(final Member iMember) {
    return iMember.getInetSocketAddress().toString().substring(1);
  }

  public Set<String> getRemoteNodeIds() {
    return remoteClusterNodes.keySet();
  }

  public Set<String> getRemoteNodeIdsBut(final String... iExcludeNodes) {
    final Set<String> otherNodes = remoteClusterNodes.keySet();

    final Set<String> set = new HashSet<String>(otherNodes.size());
    for (String item : remoteClusterNodes.keySet()) {
      boolean include = true;
      for (String excludeNode : iExcludeNodes)
        if (item.equals(excludeNode)) {
          include = false;
          break;
        }

      if (include)
        set.add(item);
    }
    return set;
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    // final String nodeId = getStorageId(iEvent.getMember());
    // remoteClusterNodes.put(nodeId, iEvent.getMember());
  }

  /**
   * Removes the node map entry.
   */
  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    final String nodeId = getNodeId(iEvent.getMember());
    getConfigurationMap().remove("node." + nodeId);
    remoteClusterNodes.remove(nodeId);
  }

  @Override
  public void entryAdded(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      if (!getLocalNodeId().equals(nodeId))
        remoteClusterNodes.put(nodeId, iEvent.getMember());
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryRemoved(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      ODistributedServerLog.warn(this, getLocalNodeId(), nodeId, DIRECTION.NONE,
          "tracked remote node has been disconnected from the cluster");
      remoteClusterNodes.remove(nodeId);

      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryUpdated(EntryEvent<String, Object> event) {
    if (event.getKey().startsWith("node."))
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
  }

  @Override
  public void entryEvicted(EntryEvent<String, Object> event) {
  }

  public String getRemoteNodeStatus(final String iNodeId) {
    final ODocument cfg = getNodeConfiguration(iNodeId);
    return (String) (cfg != null ? cfg.field("status") : null);
  }

  public boolean isOfflineNode(final String iNodeId) {
    final ODocument cfg = getNodeConfiguration(iNodeId);
    return cfg == null || !cfg.field("status").equals("online");
  }

  public int getNodeNumber() {
    return nodeNumber;
  }

  public HazelcastInstance getHazelcastInstance() {
    while (hazelcastInstance == null) {
      // WAIT UNTIL THE INSTANCE IS READY
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return hazelcastInstance;
  }

  protected IMap<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  public Lock getLock(final String iName) {
    return getHazelcastInstance().getLock(iName);
  }

  public Class<? extends OReplicationConflictResolver> getConfictResolverClass() {
    return confictResolverClass;
  }

  protected Object executeOperation(final Callable<Object> task, final Object iKey, final EXECUTION_MODE iMode,
      final ExecutionCallback<Object> callback) throws ExecutionException, InterruptedException {
    Member member = hazelcastInstance.getPartitionService().getPartition(iKey).getOwner();
    return executeOperation(task, member, iMode, callback);
  }

  protected Object executeOperation(final Callable<Object> task, Member member, final EXECUTION_MODE iMode,
      final ExecutionCallback<Object> callback) throws ExecutionException, InterruptedException {

    if (iMode == EXECUTION_MODE.ASYNCHRONOUS && callback != null) {
      hazelcastInstance.getExecutorService(DISTRIBUTED_EXECUTOR_NAME).submitToMember(task, member, callback);
      return null;
    }

    Future<Object> future = hazelcastInstance.getExecutorService(DISTRIBUTED_EXECUTOR_NAME).submitToMember(task, member);

    if (iMode == EXECUTION_MODE.SYNCHRONOUS)
      return future.get();

    return null;
  }

  /**
   * Initializes distributed databases.
   */
  protected void initDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.NONE, "opening database '%s'...", storageEntry.getKey());
      getDatabaseSynchronizer(storageEntry.getKey());
    }
  }

  protected String waitUntilMasterNodeIsOnline(final String iClusterName, final Object iKey, final String dbName,
      String masterNodeId) {
    if (!masterNodeId.equals(localNodeId) && isOfflineNode(masterNodeId)) {
      ODistributedServerLog.warn(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
          "node is offline (status=%s). Waiting for completition...", getRemoteNodeStatus(masterNodeId));

      while (isOfflineNode(masterNodeId)) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        // RE-READ THE KEY OWNER (IT COULD BE CHANGED DURING THE PAUSE)
        final OReplicationConfig newReplicationConfig = getReplicationData(dbName, iClusterName, iKey, null, null);

        if (!newReplicationConfig.masterNode.equals(masterNodeId)) {
          ODistributedServerLog.warn(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
              "node %s is the new owner of the requested key set", getRemoteNodeStatus(masterNodeId));
          masterNodeId = newReplicationConfig.masterNode;
        }

      }

      ODistributedServerLog.warn(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
          "node aligned, flushing pending operations...");
    }
    return masterNodeId;
  }

  @Override
  public Object enqueueLocalExecution(final OAbstractReplicatedTask<? extends Object> iTask) throws Exception {

    final OStorageSynchronizer dbSynchronizer = iTask.getDatabaseSynchronizer();

    waitForMyTurnInQueue(iTask);
    try {
      ODistributedServerLog.debug(this, iTask.getNodeSource(), iTask.getNodeDestination(), DIRECTION.IN, "pop operation=%d:%d",
          iTask.getRunId(), iTask.getOperationSerial());

      final long operationLogOffset = logOperation2Journal(dbSynchronizer, iTask);

      // EXECUTE IT LOCALLY
      try {
        final Object result = iTask.executeOnLocalNode();

        // OK, SET AS COMMITTED
        updateJournal(iTask, dbSynchronizer, operationLogOffset, true);

        return result;

      } catch (Exception e) {
        // ERROR: SET AS CANCELED
        updateJournal(iTask, dbSynchronizer, operationLogOffset, false);
        throw e;
      }

    } finally {
      updateQueue(iTask);
    }
  }

  @Override
  public String toString() {
    return getLocalNodeAlias();
  }

  private void updateQueue(final OAbstractReplicatedTask<? extends Object> iTask) {
    // SET LAST EXECUTION SERIAL
    ODistributedThreadLocal.INSTANCE.set(null);

    synchronized (lockQueue) {
      ODistributedServerLog.debug(this, iTask.getNodeSource(), iTask.getNodeDestination(), DIRECTION.IN,
          "completed operation=%d:%d", iTask.getRunId(), iTask.getOperationSerial());

      executionQueue.put(iTask.getRunId(), iTask.getOperationSerial());
      lockQueue.notifyAll();
    }
  }

  private void updateJournal(final OAbstractReplicatedTask<? extends Object> iTask, final OStorageSynchronizer dbSynchronizer,
      final long operationLogOffset, final boolean iSuccess) {
    try {
      if (iSuccess)
        iTask.setAsCommitted(dbSynchronizer, operationLogOffset);
      else
        iTask.setAsCanceled(dbSynchronizer, operationLogOffset);
    } catch (IOException e) {
      ODistributedServerLog.error(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.IN,
          "error on changing the log status for %s db=%s %s", e, getName(), iTask.getDatabaseName(), iTask.getPayload());
      throw new ODistributedException("Error on changing the log status", e);
    }
  }

  private long logOperation2Journal(final OStorageSynchronizer dbSynchronizer, final OAbstractReplicatedTask<? extends Object> iTask) {
    final long operationLogOffset;
    try {
      operationLogOffset = dbSynchronizer.getLog().append(iTask);

    } catch (IOException e) {
      ODistributedServerLog.error(this, iTask.getDistributedServerManager().getLocalNodeId(), iTask.getNodeSource(), DIRECTION.IN,
          "error on logging operation %s db=%s %s", e, iTask.getName(), iTask.getDatabaseName(), iTask.getPayload());
      throw new ODistributedException("Error on logging operation", e);
    }
    return operationLogOffset;
  }

  public void resetOperationQueue(long iCurrentRunId, long iOperationSerial) {
    synchronized (lockQueue) {
      final Long last = executionQueue.get(iCurrentRunId);
      if (last == null || last != iOperationSerial) {
        executionQueue.put(iCurrentRunId, iOperationSerial);
        lockQueue.notifyAll();
      }
    }
  }

  private void waitForMyTurnInQueue(final OAbstractReplicatedTask<? extends Object> iTask) {
    // MANAGE ORDER
    while (true)
      synchronized (lockQueue) {
        final Long last = executionQueue.get(iTask.getRunId());
        if (last == null) {
          // FIRST OPERATION
          executionQueue.put(iTask.getRunId(), 0l);
          break;
        } else if (last != iTask.getOperationSerial() - 1) {
          // SLEEP UNTIL NEXT OPERATION
          try {
            ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.NONE,
                "waiting for %d tasks in queue %s. current=%d my=%d", (iTask.getOperationSerial() - last - 1), iTask.getRunId(),
                last, iTask.getOperationSerial());

            lockQueue.wait(OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsLong());
          } catch (InterruptedException e) {
          }
        } else
          break;
      }

    ODistributedThreadLocal.INSTANCE.set(iTask.getNodeSource());
  }
}
