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
import java.util.Arrays;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.types.ORef;
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
import com.orientechnologies.orient.server.distributed.OSkippedOperationException;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAlignRequestTask;
import com.orientechnologies.orient.server.distributed.task.ONoOperationTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {
  protected static final String        SYNCH_EXECUTOR_NAME     = "OHazelcastPlugin::SynchExecutor";
  protected static final String        ASYNCH_EXECUTOR_NAME    = "OHazelcastPlugin::AsynchExecutor";
  protected static final long          RESET_OPERATION_TIMEOUT = 20000;

  protected int                        nodeNumber;
  protected String                     localNodeId;
  protected String                     configFile              = "hazelcast.xml";
  protected Map<String, Member>        remoteClusterNodes      = new ConcurrentHashMap<String, Member>();
  protected final long                 REALIGN_DELAY_TIME      = 1000;
  protected long                       timeOffset;
  protected long                       runId                   = -1;
  protected volatile String            status                  = "starting";
  protected Map<String, Boolean>       pendingAlignments       = new HashMap<String, Boolean>();
  protected TimerTask                  alignmentTask;
  protected String                     membershipListenerRegistration;
  protected Object                     lockQueue               = new Object();

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

  /**
   * Propagates the tasks against the configured synchronous and asynchronous nodes. The sending operation is executed in parallel
   * and then waits only for synchronous tasks to complete.
   * 
   * @param iTask
   * @param iReplicationData
   * @return
   * @throws ODistributedException
   */
  public Map<String, Object> replicate(final OAbstractRemoteTask<? extends Object> iTask, final OReplicationConfig iReplicationData)
      throws ODistributedException {
    final Map<String, Object> results = new HashMap<String, Object>();

    if ((iReplicationData.synchReplicas == null || iReplicationData.synchReplicas.length == 0)
        && (iReplicationData.asynchReplicas == null || iReplicationData.asynchReplicas.length == 0)) {
      // NO PROPAGATION
      ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.IN,
          "empty node list, skip replication of %s against db=%s oper=%d.%d...", iTask.getName().toUpperCase(),
          iTask.getDatabaseName(), iTask.getRunId(), iTask.getOperationSerial());

      return results;
    }

    iTask.setNodeSource(getLocalNodeId());

    final Map<OAbstractRemoteTask<? extends Object>, Future<Object>> synchTasks = new HashMap<OAbstractRemoteTask<? extends Object>, Future<Object>>();

    if (iReplicationData.synchReplicas != null && iReplicationData.synchReplicas.length > 0) {
      // PROPAGATE TO SYNCHRONOUS NODES
      ODistributedServerLog.debug(this, iTask.getNodeSource(), Arrays.toString(iReplicationData.synchReplicas), DIRECTION.OUT,
          "propagate to SYNCHRONOUS nodes %s oper=%d.%d", iTask.getName().toUpperCase(), iTask.getRunId(),
          iTask.getOperationSerial());

      for (String nodeId : iReplicationData.synchReplicas) {
        final OAbstractRemoteTask<? extends Object> task = iTask instanceof OAbstractReplicatedTask ? ((OAbstractReplicatedTask<? extends Object>) iTask)
            .copy() : iTask;
        synchTasks.put(task, sendTask2Node(nodeId, task, EXECUTION_MODE.SYNCHRONOUS, results));
      }
    }

    if (iReplicationData.asynchReplicas != null && iReplicationData.asynchReplicas.length > 0) {
      // PROPAGATE TO ASYNCHRONOUS NODES
      ODistributedServerLog.debug(this, iTask.getNodeSource(), Arrays.toString(iReplicationData.asynchReplicas), DIRECTION.OUT,
          "propagate to ASYNCHRONOUS nodes %s oper=%d.%d", iTask.getName().toUpperCase(), iTask.getRunId(),
          iTask.getOperationSerial());

      for (String nodeId : iReplicationData.asynchReplicas) {
        final OAbstractRemoteTask<? extends Object> task = iTask instanceof OAbstractReplicatedTask ? ((OAbstractReplicatedTask<? extends Object>) iTask)
            .copy() : iTask;
        sendTask2Node(nodeId, task, EXECUTION_MODE.ASYNCHRONOUS, results);
      }
    }

    if (iReplicationData.synchReplicas != null && iReplicationData.synchReplicas.length > 0) {
      // WAIT FOR SYNCHRONOUS TASKS TO COMPLETE
      for (Map.Entry<OAbstractRemoteTask<? extends Object>, Future<Object>> f : synchTasks.entrySet()) {

        final OAbstractRemoteTask<? extends Object> task = f.getKey();

        try {
          results.put(task.getNodeDestination(), f.getValue().get(task.getTimeout(), TimeUnit.MILLISECONDS));

        } catch (TimeoutException e) {
          // TIMEOUT
          ODistributedServerLog.error(this, getLocalNodeId(), task.getNodeDestination(), DIRECTION.OUT,
              "timeout on replication of operation %d.%d task=%s, db=%s...", task.getRunId(), task.getOperationSerial(), task,
              task.getDatabaseName());

          f.getValue().cancel(true);

          ODistributedExecutionCallback.handleTaskException(this, task, EXECUTION_MODE.SYNCHRONOUS, task.getNodeDestination(),
              results, e);
        } catch (Throwable e) {
          // ERROR
          ODistributedExecutionCallback.handleTaskException(this, task, EXECUTION_MODE.SYNCHRONOUS, task.getNodeDestination(),
              results, e);
        }
      }
    }

    return results;
  }

  @SuppressWarnings("unchecked")
  public Future<Object> sendTask2Node(final String iNodeId, final OAbstractRemoteTask<? extends Object> iTask,
      final EXECUTION_MODE iMode, final Map<String, Object> iResults) {

    iTask.setNodeDestination(iNodeId);
    final Member clusterMember = getHazelcastMember(iNodeId);

    try {
      if (iMode == EXECUTION_MODE.SYNCHRONOUS) {
        // SYNCHRONOUS
        final IExecutorService exec = hazelcastInstance.getExecutorService(SYNCH_EXECUTOR_NAME);
        return exec.submitToMember((Callable<Object>) iTask, clusterMember);
      } else {
        // ASYNCHRONOUS
        final IExecutorService exec = hazelcastInstance.getExecutorService(ASYNCH_EXECUTOR_NAME);
        final ODistributedExecutionCallback callback = new ODistributedExecutionCallback(iTask, iMode, iNodeId, iResults);
        exec.submitToMember((Callable<Object>) iTask, clusterMember, callback);
        return null;
      }

    } catch (Exception e) {
      ODistributedServerLog.error(this, getLocalNodeId(), iNodeId, DIRECTION.OUT,
          "error on execution of operation %d.%d in %s mode", e, iTask.getRunId(), iTask.getOperationSerial(), iMode);
      throw new ODistributedException("Error on executing remote operation " + iTask.getRunId() + "." + iTask.getOperationSerial()
          + " in " + iMode + " mode against node: " + clusterMember, e);
    }
  }

  private Member getHazelcastMember(final String iNodeId) {
    Member member = remoteClusterNodes.get(iNodeId);
    if (member == null) {
      // CHECK IF IS ENTERING IN THE CLUSTER AND HASN'T BEEN REGISTERED YET
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (getNodeId(m).equals(iNodeId)) {
          member = m;
          break;
        }
      }

      if (member == null) {
        ODistributedServerLog.warn(this, getLocalNodeId(), iNodeId, DIRECTION.OUT, "cannot find remote member %s", iNodeId);
        throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");
      }
    }
    return member;
  }

  public Object execute(final String iClusterName, final Object iKey, final OAbstractRemoteTask<? extends Object> iTask,
      OReplicationConfig iReplicationData) throws ExecutionException {

    String masterNodeId = null;

    try {
      if (iReplicationData == null || !(iTask instanceof OAbstractReplicatedTask)) {
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
        if (!checkOperationSequence(iTask))
          return null;

        final String dbName = iTask.getDatabaseName();

        if (iReplicationData != null) {
          // SET THE DESTINATION NODE
          iTask.setNodeDestination(iReplicationData.masterNode);
          iReplicationData.masterNode = waitUntilMasterNodeIsOnline(iClusterName, iKey, dbName, iReplicationData.masterNode);
          masterNodeId = iReplicationData.masterNode;
        }

        if (getLocalNodeId().equals(iReplicationData.masterNode))
          // LOCAL + PROPAGATE
          return executeLocallyAndPropagate((OAbstractReplicatedTask<? extends Object>) iTask, iReplicationData);
        else
          // REMOTE + LOCAL
          return executeRemotelyAndApplyLocally(iClusterName, iKey, (OAbstractReplicatedTask<? extends Object>) iTask, dbName,
              iReplicationData);
      }
    } catch (InterruptedException e) {
      Thread.interrupted();

      // PROPAGATE IT
      ODistributedServerLog.debug(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
          "caught interruption during execution %d.%d of operation", iTask.getRunId(), iTask.getOperationSerial());
      throw new ExecutionException("Caught interruption during execution of operation " + iTask.getRunId() + "."
          + iTask.getOperationSerial() + " in " + EXECUTION_MODE.SYNCHRONOUS + " mode against node " + masterNodeId, e);

    } catch (ONeedRetryException e) {
      // PROPAGATE IT
      ODistributedServerLog.debug(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT,
          "error on execution %d.%d of operation raising a ONeedRetryException", iTask.getRunId(), iTask.getOperationSerial());
      throw e;

    } catch (Exception e) {
      // ALL OTHER EXCEPTION: WRAP ON EXECUTION EXCEPTION
      ODistributedServerLog.error(this, getLocalNodeId(), masterNodeId, DIRECTION.OUT, "error on execution %d.%d of operation", e,
          iTask.getRunId(), iTask.getOperationSerial());
      throw new ExecutionException("error on execution of operation " + iTask.getRunId() + "." + iTask.getOperationSerial()
          + " against node " + masterNodeId, e);
    }
  }

  protected boolean checkOperationSequence(final OAbstractRemoteTask<? extends Object> iTask) {
    final long opSerial = iTask.getOperationSerial();
    if (opSerial == -1)
      // SERVICE MSG: ALWAYS OK
      return true;

    final OStorageSynchronizer dbSynchronizer = getDatabaseSynchronizer(iTask.getDatabaseName());
    final long[] lastExecutedOperation = dbSynchronizer.getLog().getLastExecutedOperationId();

    if (iTask.getRunId() == lastExecutedOperation[0] && opSerial <= lastExecutedOperation[1]) {
      // ALREADY EXECUTED, SKIP IT
      ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.IN,
          "received operation %d.%d but it has already been executed (now at %d.%d): probably it's from an alignment? Ignore it.",
          iTask.getRunId(), opSerial, lastExecutedOperation[0], lastExecutedOperation[1]);
      return false;
    }

    ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.IN,
        "checking operation if %d.%d > last journaled %d.%d = ok, thread=%s", iTask.getRunId(), opSerial, lastExecutedOperation[0],
        lastExecutedOperation[1], Thread.currentThread().getName());

    return true;
  }

  protected Object executeRemotelyAndApplyLocally(final String iClusterName, final Object iKey,
      final OAbstractReplicatedTask<? extends Object> iTask, final String dbName, final OReplicationConfig iReplicationData)
      throws InterruptedException, Exception, ExecutionException {

    try {
      // EXECUTES ON THE TARGET NODE
      ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
          "routing execution %s db=%s oper=%d.%d...", iTask.getName().toUpperCase(), dbName, iTask.getRunId(),
          iTask.getOperationSerial());

      final Map<String, Object> remoteResults = replicate(iTask, iReplicationData);

      final Object localResult;
      if (iTask instanceof OAbstractReplicatedTask<?>) {
        // APPLY LOCALLY TOO
        ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.IN,
            "apply changes locally about %s against db=%s oper=%d.%d...", iTask.getName().toUpperCase(), dbName, iTask.getRunId(),
            iTask.getOperationSerial());

        final ORef<Long> journalOffset = new ORef<Long>();

        localResult = enqueueLocalExecution(iTask, journalOffset);

        if (journalOffset.value != null)
          // OK, SET AS "COMMITTED" IN JOURNAL AFTER THE REMOTE OPERATION ARE COMPLETED
          updateJournal(iTask, iTask.getDatabaseSynchronizer(), journalOffset.value, true);

        checkForConflicts(iTask, localResult, remoteResults, iReplicationData.minSuccessfulOperations);

      } else
        // GET THE FIRST REMOTE RESULT AS LOCAL
        localResult = remoteResults.values().iterator().next();

      // OK
      return localResult;

    } catch (MemberLeftException e) {
      // RETRY
      ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
          "error on execution of operation %d.%d in %s mode, because node left. Re-route it in transparent way", e,
          iTask.getRunId(), iTask.getOperationSerial(), EXECUTION_MODE.SYNCHRONOUS);

      return execute(iClusterName, iKey, iTask, iReplicationData);

    } catch (Exception e) {
      ODistributedServerLog.error(this, getLocalNodeId(), iTask.getNodeDestination(), DIRECTION.OUT,
          "error on execution of operation %d.%d in %s mode", e, iTask.getRunId(), iTask.getOperationSerial(),
          EXECUTION_MODE.SYNCHRONOUS);
      throw e;
    } finally {
      notifyQueueWaiters(iTask.getDatabaseName(), iTask.getRunId(), iTask.getOperationSerial(), false);
    }
  }

  private Object executeLocallyAndPropagate(final OAbstractReplicatedTask<? extends Object> iTask,
      final OReplicationConfig iReplicationData) throws Exception {
    OAbstractReplicatedTask<? extends Object> taskToPropagate = iTask;
    Object localResult = null;
    final ORef<Long> journalOffset = new ORef<Long>();
    try {
      // LOCAL EXECUTION AVOID TO USE EXECUTORS
      localResult = enqueueLocalExecution(iTask, journalOffset);

    } catch (Exception e) {
      // ERROR: PROPAGATE A NO-OP TASK TO MAINTAIN THE TASK SEQUENCE AND SET AS "CANCELED" IN JOURNAL
      updateJournal(iTask, iTask.getDatabaseSynchronizer(), journalOffset.value, false);
      taskToPropagate = new ONoOperationTask(iTask);
      throw e;

    } finally {

      final Map<String, Object> remoteResults = replicate(taskToPropagate, iReplicationData);

      // OK, SET AS "COMMITTED" IN JOURNAL AFTER THE REMOTE OPERATION ARE COMPLETED
      updateJournal(iTask, iTask.getDatabaseSynchronizer(), journalOffset.value, true);

      checkForConflicts(taskToPropagate, localResult, remoteResults, iReplicationData.minSuccessfulOperations);
    }

    return localResult;
  }

  private void checkForConflicts(final OAbstractReplicatedTask<? extends Object> taskToPropagate, final Object localResult,
      final Map<String, Object> remoteResults, final int minSuccessfulOperations) {

    int successfulReplicatedNodes = 0;

    for (Entry<String, Object> entry : remoteResults.entrySet()) {
      final String remoteNode = entry.getKey();
      final Object remoteResult = entry.getValue();

      if (!(remoteResult instanceof Exception)) {
        successfulReplicatedNodes++;

        if ((localResult == null && remoteResult != null) || (localResult != null && remoteResult == null)
            || (localResult != null && !localResult.equals(remoteResult))) {
          // CONFLICT
          taskToPropagate.handleConflict(remoteNode, localResult, remoteResult);
        }
      }
    }

    if (successfulReplicatedNodes < minSuccessfulOperations)
      // ERROR: MINIMUM SUCCESSFUL OPERATION NOT REACHED: RESTORE OLD RECORD
      // TODO: MANAGE ROLLBACK OF TASK
      // taskToPropagate.rollbackLocalChanges();
      ;
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

    final Set<String> targetNodes = getOnlineRemoteNodeIdsBut(iLocalNodeId, iRemoteNodeId);
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

  public boolean isEnabled() {
    return enabled;
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

  private void registerAndAlignNodes() {
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
      broadcastAlignmentRequest();

    if (alignmentTimer > 0) {
      // SCHEDULE THE AUTO ALIGNMENT
      alignmentTask = new TimerTask() {
        @Override
        public void run() {
          broadcastAlignmentRequest();
        }
      };

      Orient.instance().getTimer().schedule(alignmentTask, alignmentTimer, alignmentTimer);
    }
  }

  @Override
  public void broadcastAlignmentRequest() {
    if (remoteClusterNodes.isEmpty())
      // NO NODES; AVOID ALIGNMENT
      return;

    setStatus("aligning");

    final Map<OAlignRequestTask, OReplicationConfig> tasks = new HashMap<OAlignRequestTask, OReplicationConfig>();

    // EXECUTE THE ALIGNMENT: THE STATUS ONLINE WILL BE SET ASYNCHRONOUSLY ONCE FINISHED
    synchronized (synchronizers) {

      for (Entry<String, OStorageSynchronizer> entry : synchronizers.entrySet()) {
        final String databaseName = entry.getKey();

        final OReplicationConfig replicationData = getReplicationData(databaseName, null, null, getLocalNodeId(), null);

        try {
          // GET LAST OPERATION, DOESN'T MATTER THE STATUS
          final long[] lastOperationId = entry.getValue().getLog().getLastJournaledOperationId(null);

          if (lastOperationId[0] == -1 && lastOperationId[1] == -1)
            // AVOID TO SEND THE REQUEST IF THE LOG IS EMPTY
            continue;

          ODistributedServerLog.warn(this, getLocalNodeId(), remoteClusterNodes.keySet().toString(), DIRECTION.OUT,
              "sending align request in broadcast for database '%s' from operation %d:%d", databaseName, lastOperationId[0],
              lastOperationId[1]);

          synchronized (pendingAlignments) {
            for (String node : remoteClusterNodes.keySet()) {
              pendingAlignments.put(node + "/" + databaseName, Boolean.FALSE);

              ODistributedServerLog.info(this, getLocalNodeId(), node, DIRECTION.NONE, "setting node in alignment state for db=%s",
                  databaseName);
            }
          }

          tasks.put(new OAlignRequestTask(serverInstance, this, databaseName, lastOperationId[0], lastOperationId[1]),
              replicationData);

        } catch (IOException e) {
          ODistributedServerLog.warn(this, getLocalNodeId(), null, DIRECTION.OUT,
              "error on retrieve last operation id from the log for db=%s", databaseName);
        }
      }

      if (pendingAlignments.isEmpty())
        setStatus("online");
    }

    // SEND ALL THE ALIGNMENT TASKS SYNCHRONOUSLY (OUT OF SYNCHRONIZERS LOCK)
    for (Entry<OAlignRequestTask, OReplicationConfig> t : tasks.entrySet()) {
      replicate(t.getKey(), t.getValue());
    }
  }

  @Override
  public void endAlignment(final String iNode, final String iDatabaseName, final int alignedOperations) {
    synchronized (pendingAlignments) {
      if (pendingAlignments.remove(iNode + "/" + iDatabaseName) == null) {
        ODistributedServerLog.error(this, getLocalNodeId(), iNode, DIRECTION.OUT,
            "received response for an alignment against an unknown node %s database %s", iDatabaseName);
      }

      if (pendingAlignments.isEmpty()) {
        setStatus("online");

        if (alignedOperations > 0) {
          ODistributedServerLog.error(this, getLocalNodeId(), iNode, DIRECTION.OUT,
              "aligned %d operations, schedule another realignment in %dms to get the new operations if any, database %s",
              alignedOperations, REALIGN_DELAY_TIME, iDatabaseName);

          Orient.instance().getTimer().schedule(new TimerTask() {
            @Override
            public void run() {
              // CHECK FOR ANY PENDING OPERATIONS UNTIL IS 0
              broadcastAlignmentRequest();
            }
          }, REALIGN_DELAY_TIME);
        }
      } else {
        // WAKE UP ALL THE POSTPONED ALIGNMENTS
        for (Entry<String, Boolean> entry : pendingAlignments.entrySet()) {
          final String[] parts = entry.getKey().split("/");
          final String node = parts[0];
          final String databaseName = parts[1];

          if (entry.getValue()) {
            final OStorageSynchronizer synch = synchronizers.get(databaseName);

            long[] lastOperationId;

            try {
              lastOperationId = synch.getLog().getLastJournaledOperationId(null);

              ODistributedServerLog.info(this, getLocalNodeId(), node, DIRECTION.OUT, "resend alignment request db=%s from %d:%d",
                  databaseName, lastOperationId[0], lastOperationId[1]);

              sendTask2Node(node,
                  new OAlignRequestTask(serverInstance, this, databaseName, lastOperationId[0], lastOperationId[1]),
                  EXECUTION_MODE.ASYNCHRONOUS, new HashMap<String, Object>());

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
    final Object ipAddress = iMember.getInetSocketAddress();
    if (ipAddress != null)
      // USE THE IP-ADDRESS
      return ipAddress.toString().substring(1);

    // IP-ADDRESS NOT AVAILABLE, RETURN THE HAZELCAST'S UUID
    return iMember.getUuid();
  }

  public Set<String> getRemoteNodeIds() {
    return remoteClusterNodes.keySet();
  }

  public Set<String> getOnlineRemoteNodeIdsBut(final String... iExcludeNodes) {
    final Set<String> otherNodes = remoteClusterNodes.keySet();

    final Set<String> set = new HashSet<String>(otherNodes.size() + 1);
    for (String item : otherNodes) {
      // if (isOfflineNode(item))
      // // SKIP IT BECAUSE IS NOT ONLINE YET
      // // TODO: SPEED UP THIS CHECKING THE NODE IN ALIGNMENT STATES?
      // continue;

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
  public void entryUpdated(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      ODistributedServerLog.debug(this, getLocalNodeId(), nodeId, DIRECTION.NONE,
          "received notification about update in the cluster: %s", iEvent);

      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryEvicted(EntryEvent<String, Object> event) {
  }

  public String getRemoteNodeStatus(final String iNodeId) {
    final ODocument cfg = getNodeConfiguration(iNodeId);
    return (String) (cfg != null ? cfg.field("status") : null);
  }

  public boolean isOfflineNode(final String iNodeId) {
    synchronized (pendingAlignments) {
      if (pendingAlignments.containsKey(iNodeId))
        // ALIGNMENT STATUS
        return true;
    }

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
  public Object enqueueLocalExecution(final OAbstractReplicatedTask<? extends Object> iTask, final ORef<Long> iOperationOffset)
      throws Exception {

    final long opSerial = iTask.getOperationSerial();
    if (opSerial == -1)
      try {
        // SERVICE MSG: JUST EXECUTE IT
        return iTask.executeOnLocalNode();
      } finally {
        notifyQueueWaiters(iTask.getDatabaseName(), iTask.getRunId(), opSerial, false);
      }

    if (!waitForMyTurnInQueue(iTask))
      throw new OSkippedOperationException(iTask);

    try {
      ODistributedServerLog.debug(this, iTask.getNodeSource(), iTask.getNodeDestination(), DIRECTION.IN,
          "local exec: pop operation=%d.%d, thread=%s", iTask.getRunId(), opSerial, Thread.currentThread().getName());

      iOperationOffset.value = logOperation2Journal(iTask.getDatabaseSynchronizer(), iTask);

      // EXECUTE IT LOCALLY
      return iTask.executeOnLocalNode();

    } finally {
      notifyQueueWaiters(iTask.getDatabaseName(), iTask.getRunId(), opSerial, false);
    }
  }

  @Override
  public String toString() {
    return getLocalNodeAlias();
  }

  public void updateJournal(final OAbstractReplicatedTask<? extends Object> iTask, final OStorageSynchronizer dbSynchronizer,
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

  private boolean waitForMyTurnInQueue(final OAbstractReplicatedTask<? extends Object> iTask) {
    try {
      // MANAGE ORDER
      final OStorageSynchronizer dbSynchronizer = getDatabaseSynchronizer(iTask.getDatabaseName());

      while (true) {
        if (!checkOperationSequence(iTask))
          break;

        final long opSerial = iTask.getOperationSerial();

        synchronized (lockQueue) {

          final long[] lastExecutedOperation = dbSynchronizer.getLog().getLastExecutedOperationId();

          if ((lastExecutedOperation[0] != iTask.getRunId() && opSerial > 1) // FIRST OF THE NEW RUN?
              || (lastExecutedOperation[0] == iTask.getRunId() && lastExecutedOperation[1] != opSerial - 1)) {

            final long timeSinceLastJournalUpdate = System.currentTimeMillis() - lastExecutedOperation[2];

            if (timeSinceLastJournalUpdate > RESET_OPERATION_TIMEOUT) {
              lastExecutedOperation[1]++;

              ODistributedServerLog.warn(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.NONE,
                  "timeout expired waiting for operation, skip op=%d.%d task=%s thread=%s", lastExecutedOperation[0],
                  lastExecutedOperation[1], iTask, Thread.currentThread().getName());

              dbSynchronizer.getLog().updateLastOperation(lastExecutedOperation[0], lastExecutedOperation[1], false);

            } else
              // SLEEP UNTIL NEXT OPERATION
              try {
                final String tasksToWait = lastExecutedOperation[0] != iTask.getRunId() ? ">=1 (prev run)" : ""
                    + (opSerial - lastExecutedOperation[1] - 1);

                ODistributedServerLog.debug(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.NONE,
                    "waiting for %s task(s) queue=%s current=%d my=%d thread=%s", tasksToWait, iTask.getRunId(),
                    lastExecutedOperation[1], opSerial, Thread.currentThread().getName());

                lockQueue.wait(OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong());
              } catch (InterruptedException e) {
              }
          } else {
            // OK!
            ODistributedThreadLocal.INSTANCE.set(iTask.getNodeSource());
            return true;
          }
        }
      }
    } catch (Throwable r) {
      ODistributedServerLog.error(this, getLocalNodeId(), iTask.getNodeSource(), DIRECTION.NONE,
          "error while checking for queue sequence, queue=%s, thread=%s", iTask.getRunId(), Thread.currentThread().getName());
    }

    return false;
  }

  public boolean notifyQueueWaiters(final String iDatabaseName, final long iRunId, final long iOperationSerial, final boolean iForce) {
    ODistributedThreadLocal.INSTANCE.set(null);

    final boolean updated;
    if (iOperationSerial > -1)
      // UPDATE THE TASK ID
      updated = getDatabaseSynchronizer(iDatabaseName).getLog().updateLastOperation(iRunId, iOperationSerial, iForce);
    else
      updated = false;

    synchronized (lockQueue) {
      lockQueue.notifyAll();
    }

    return updated;
  }
}
