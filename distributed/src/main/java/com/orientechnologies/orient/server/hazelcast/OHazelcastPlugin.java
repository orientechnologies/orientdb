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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
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
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedPartition;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedThreadLocal;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAlignRequestTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {
  protected String                                 localNodeId;
  protected String                                 hazelcastConfigFile = "hazelcast.xml";
  protected Map<String, Member>                    remoteClusterNodes  = new ConcurrentHashMap<String, Member>();
  protected long                                   runId               = -1;
  protected Map<String, ODistributedConfiguration> configurations      = new ConcurrentHashMap<String, ODistributedConfiguration>();
  protected OHazelcastDistributedMessageService    messageService      = new OHazelcastDistributedMessageService(this);

  protected volatile STATUS                        status              = STATUS.STARTING;

  protected String                                 membershipListenerRegistration;
  protected Object                                 lockQueue           = new Object();

  // ALIGNMENT
  protected Map<String, Boolean>                   pendingAlignments   = new HashMap<String, Boolean>();
  protected TimerTask                              alignmentTask;

  protected volatile HazelcastInstance             hazelcastInstance;

  public OHazelcastPlugin() {
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);

    if (nodeName == null) {
      // GENERATE NODE NAME
      nodeName = "node" + System.currentTimeMillis();
      final OServerConfiguration cfg = iServer.getConfiguration();
      for (OServerHandlerConfiguration h : cfg.handlers) {
        if (h.clazz.equals(getClass().toString())) {
          for (OServerParameterConfiguration p : h.parameters) {
            if (p.name.equals("nodeName")) {
              p.value = nodeName;
              try {
                iServer.saveConfiguration();
              } catch (IOException e) {
                throw new OConfigurationException("Cannot save server configuration", e);
              }
              break;
            }
          }
        }
      }
    }

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast"))
        hazelcastConfigFile = OSystemVariableResolver.resolveSystemVariables(param.value);
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    remoteClusterNodes.clear();
    synchronizers.clear();

    try {
      hazelcastInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(hazelcastConfigFile));

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

    setStatus(STATUS.OFFLINE);

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

    ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
        "network partition: check for local master: key '%s' is assigned to %s (local=%s)", iKey, getNodeName(partitionOwner),
        local);

    return local;
  }

  @Override
  public ODocument getClusterConfiguration() {
    if (!enabled)
      return null;

    final ODocument cluster = new ODocument();

    final HazelcastInstance instance = getHazelcastInstance();

    cluster.field("name", instance.getName());
    cluster.field("local", instance.getCluster().getLocalMember().getUuid());

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    members.add(getLocalNodeConfiguration());
    for (Member member : remoteClusterNodes.values()) {
      members.add(getNodeConfiguration(getNodeName(member)));
    }

    return cluster;
  }

  public ODocument getNodeConfiguration(final String iNode) {
    return (ODocument) getConfigurationMap().get("node." + iNode);
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();

    nodeCfg.field("name", getLocalNodeName());
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

  public STATUS getStatus() {
    return status;
  }

  public boolean checkStatus(final String iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  public void setStatus(final STATUS iStatus) {
    if (status.equals(iStatus))
      // NO CHANGE
      return;

    status = iStatus;

    final IMap<String, Object> map = getConfigurationMap();
    final String nodeName = "node." + getLocalNodeName();
    final ODocument nodeConfiguration = getLocalNodeConfiguration();
    map.put(nodeName, nodeConfiguration);

    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "updated node status to '%s'", status);
  }

  private void registerAndAlignNodes() {
    membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

    // COLLECTS THE MEMBER LIST
    for (Member clusterMember : hazelcastInstance.getCluster().getMembers()) {
      final String nodeId = getNodeName(clusterMember);
      if (!getLocalNodeName().equals(nodeId))
        remoteClusterNodes.put(nodeId, clusterMember);
    }

    if (remoteClusterNodes.isEmpty())
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "no node running has been detected");
    else
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "detected %d running nodes %s",
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

    final List<OAlignRequestTask> tasks = new ArrayList<OAlignRequestTask>();

    // EXECUTE THE ALIGNMENT: THE STATUS ONLINE WILL BE SET ASYNCHRONOUSLY ONCE FINISHED
    synchronized (synchronizers) {

      for (Entry<String, OStorageSynchronizer> entry : synchronizers.entrySet()) {
        final String databaseName = entry.getKey();

        try {
          // GET LAST OPERATION, DOESN'T MATTER THE STATUS
          final long[] lastOperationId = entry.getValue().getLog().getLastJournaledOperationId(null);

          if (lastOperationId[0] == -1 && lastOperationId[1] == -1)
            // AVOID TO SEND THE REQUEST IF THE LOG IS EMPTY
            continue;

          ODistributedServerLog.warn(this, getLocalNodeName(), remoteClusterNodes.keySet().toString(), DIRECTION.OUT,
              "sending align request in broadcast for database '%s' from operation %d:%d", databaseName, lastOperationId[0],
              lastOperationId[1]);

          synchronized (pendingAlignments) {
            for (String node : remoteClusterNodes.keySet()) {
              pendingAlignments.put(node + "/" + databaseName, Boolean.FALSE);

              ODistributedServerLog.info(this, getLocalNodeName(), node, DIRECTION.NONE,
                  "setting node in alignment state for db=%s", databaseName);
            }
          }

          tasks.add(new OAlignRequestTask(serverInstance, this, databaseName, lastOperationId[0], lastOperationId[1]));

        } catch (IOException e) {
          ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.OUT,
              "error on retrieve last operation id from the log for db=%s", databaseName);
        }
      }

      if (pendingAlignments.isEmpty())
        setStatus("online");
    }

    // SEND ALL THE ALIGNMENT TASKS SYNCHRONOUSLY (OUT OF SYNCHRONIZERS LOCK)
    for (OAbstractRemoteTask<?> t : tasks)
      sendRequest(t);

  }

  public Object sendRequest(final OAbstractRemoteTask<?> iTask) {
    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), runId,
        incrementDistributedSerial(iTask.getDatabaseName()), iTask.getDatabaseName(), iTask.getClusterName(), iTask);

    final ODistributedResponse response = messageService.send(req);

    return response.getPayload();
  }

  @Override
  public void endAlignment(final String iNode, final String iDatabaseName, final int alignedOperations) {
    synchronized (pendingAlignments) {
      if (pendingAlignments.remove(iNode + "/" + iDatabaseName) == null) {
        ODistributedServerLog.error(this, getLocalNodeName(), iNode, DIRECTION.OUT,
            "received response for an alignment against an unknown node %s database %s", iDatabaseName);
      }

      if (pendingAlignments.isEmpty()) {
        setStatus("online");

        if (alignedOperations > 0) {
          ODistributedServerLog.error(this, getLocalNodeName(), iNode, DIRECTION.OUT,
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

              ODistributedServerLog.info(this, getLocalNodeName(), node, DIRECTION.OUT,
                  "resend alignment request db=%s from %d:%d", databaseName, lastOperationId[0], lastOperationId[1]);

              sendTask2Node(node,
                  new OAlignRequestTask(serverInstance, this, databaseName, lastOperationId[0], lastOperationId[1]),
                  EXECUTION_MODE.ASYNCHRONOUS, new HashMap<String, Object>());

            } catch (IOException e) {
              ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.OUT,
                  "error on retrieve last operation id from the log for db=%s", databaseName);
            }
          } else
            ODistributedServerLog.info(this, getLocalNodeName(), node, DIRECTION.NONE,
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
        ODistributedServerLog.error(this, getLocalNodeName(), iNode, DIRECTION.IN,
            "received response to postpone an alignment against an unknown node", iDatabaseName);
      }

      pendingAlignments.put(key, Boolean.TRUE);
    }
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  public String getNodeName(final Member iMember) {
    final ODocument cfg = getNodeConfiguration(iMember.getUuid());
    return (String) (cfg != null ? cfg.field("name") : null);
  }

  public Set<String> getRemoteNodeIds() {
    return remoteClusterNodes.keySet();
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
    final String nodeId = getNodeName(iEvent.getMember());
    getConfigurationMap().remove("node." + nodeId);
    remoteClusterNodes.remove(nodeId);
  }

  @Override
  public void entryAdded(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      if (!getLocalNodeName().equals(nodeId))
        remoteClusterNodes.put(nodeId, iEvent.getMember());
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryRemoved(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      ODistributedServerLog.warn(this, getLocalNodeName(), nodeId, DIRECTION.NONE,
          "tracked remote node has been disconnected from the cluster");
      remoteClusterNodes.remove(nodeId);

      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryUpdated(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      ODistributedServerLog.debug(this, getLocalNodeName(), nodeId, DIRECTION.NONE,
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
      final String databaseName = storageEntry.getKey();

      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "opening database '%s'...", databaseName);

      messageService.configureDatabase(databaseName);

      getDatabaseSynchronizer(databaseName);
    }
  }

  @Override
  public String toString() {
    return getLocalNodeName();
  }

  public void updateJournal(final OAbstractReplicatedTask<? extends Object> iTask, final OStorageSynchronizer dbSynchronizer,
      final long operationLogOffset, final boolean iSuccess) {
    try {
      if (iSuccess)
        iTask.setAsCommitted(dbSynchronizer, operationLogOffset);
      else
        iTask.setAsCanceled(dbSynchronizer, operationLogOffset);
    } catch (IOException e) {
      ODistributedServerLog.error(this, getLocalNodeName(), iTask.getNodeSource(), DIRECTION.IN,
          "error on changing the log status for %s db=%s %s", e, getName(), iTask.getDatabaseName(), iTask.getPayload());
      throw new ODistributedException("Error on changing the log status", e);
    }
  }

  private long logOperation2Journal(final OStorageSynchronizer dbSynchronizer, final OAbstractReplicatedTask<? extends Object> iTask) {
    final long operationLogOffset;
    try {
      operationLogOffset = dbSynchronizer.getLog().append(iTask);

    } catch (IOException e) {
      ODistributedServerLog.error(this, iTask.getDistributedServerManager().getLocalNodeName(), iTask.getNodeSource(),
          DIRECTION.IN, "error on logging operation %s db=%s %s", e, iTask.getName(), iTask.getDatabaseName(), iTask.getPayload());
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

              ODistributedServerLog.warn(this, getLocalNodeName(), iTask.getNodeSource(), DIRECTION.NONE,
                  "timeout expired waiting for operation, skip op=%d.%d task=%s thread=%s", lastExecutedOperation[0],
                  lastExecutedOperation[1], iTask, Thread.currentThread().getName());

              dbSynchronizer.getLog().updateLastOperation(lastExecutedOperation[0], lastExecutedOperation[1], false);

            } else
              // SLEEP UNTIL NEXT OPERATION
              try {
                final String tasksToWait = lastExecutedOperation[0] != iTask.getRunId() ? ">=1 (prev run)" : ""
                    + (opSerial - lastExecutedOperation[1] - 1);

                ODistributedServerLog.debug(this, getLocalNodeName(), iTask.getNodeSource(), DIRECTION.NONE,
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
      ODistributedServerLog.error(this, getLocalNodeName(), iTask.getNodeSource(), DIRECTION.NONE,
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

  @Override
  public ODistributedConfiguration getConfiguration(final String iDatabaseName) {
    return configurations.get(iDatabaseName);
  }

  @Override
  public ODistributedPartition newPartition(final List<String> partition) {
    return new OHazelcastDistributionPartition(partition);
  }

  public OHazelcastDistributedMessageService getMessageService() {
    return messageService;
  }
}
