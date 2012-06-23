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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiTask;
import com.hazelcast.util.ConcurrentHashSet;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask;
import com.orientechnologies.orient.server.task.OAlignRequestDistributedTask;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {
  private int                               nodeNumber;
  private String                            localNodeId;
  private String                            configFile         = "hazelcast.xml";
  private Map<String, Member>               remoteClusterNodes = new ConcurrentHashMap<String, Member>();
  private long                              timeOffset;
  private Set<String>                       aligningNodes      = new ConcurrentHashSet<String>();
  private long                              runId              = -1;
  private volatile static HazelcastInstance hazelcastInstance;

  public OHazelcastPlugin() {
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    super.config(oServer, iParams);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration"))
        configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    remoteClusterNodes.clear();
    synchronizers.clear();
    aligningNodes.clear();

    initDistributedDatabases();

    try {
      hazelcastInstance = Hazelcast.init(new FileSystemXmlConfig(configFile));
    } catch (FileNotFoundException e) {
      throw new OConfigurationException("Error on creating Hazelcast instance", e);
    }

    // REGISTER THE CLUSTER RUN ID IF NOT PRESENT
    getConfigurationMap().putIfAbsent("runId", hazelcastInstance.getCluster().getClusterTime());
    runId = (Long) getConfigurationMap().get("runId");

    localNodeId = getNodeId(hazelcastInstance.getCluster().getLocalMember());
    // PUT ITSELF AS ALIGNING NODE
    aligningNodes.add(getLocalNodeId());

    // COMPUTE THE DELTA BETWEEN LOCAL AND CENTRAL CLUSTER TIMES
    timeOffset = System.currentTimeMillis() - getHazelcastInstance().getCluster().getClusterTime();

    // REGISTER CURRENT MEMBERS
    registerAndAlignNodes();

    // END OF ALIGN
    aligningNodes.remove(getLocalNodeId());

    publishNodeConfiguration();

    super.startup();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    super.shutdown();

    remoteClusterNodes.clear();
    aligningNodes.clear();
    hazelcastInstance.getCluster().removeMembershipListener(this);
  }

  @Override
  public long incrementDistributedSerial(final String iDatabaseName) {
    return hazelcastInstance.getAtomicNumber("db." + iDatabaseName).incrementAndGet();
  }

  @Override
  public long getRunId() {
    return runId;
  }

  @SuppressWarnings("unchecked")
  public Object sendOperation2Node(final String iNodeId, final OAbstractDistributedTask<? extends Object> iTask) {
    final Member clusterMember = remoteClusterNodes.get(iNodeId);

    if (clusterMember == null)
      throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");

    final DistributedTask<Object> task = new DistributedTask<Object>((Callable<Object>) iTask, clusterMember);
    hazelcastInstance.getExecutorService().execute(task);

    try {
      return task.get();
    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation", e);
      throw new ODistributedException("Error on executing remote operation against node '" + iNodeId + "'", e);
    }
  }

  @SuppressWarnings("unchecked")
  public Object routeOperation2Node(final Object iKey, final OAbstractDistributedTask<? extends Object> iTask)
      throws ExecutionException {

    final String nodeId = getOwnerNode(iKey);

    if (isAligningNode(nodeId)) {
      OLogManager.instance().warn(this, "DISTRIBUTED -> node %s is aligning. Waiting for completition...", nodeId);
      while (isAligningNode(nodeId)) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
      OLogManager.instance().warn(this, "DISTRIBUTED -> node %s is aligned. Flushing pending operations...", nodeId);
    }

    try {
      if (isLocalNodeOwner(iKey)) {
        // AVOID TO USE EXECUTORS
        return iTask.call();
      } else {
        OLogManager.instance().warn(this, "DISTRIBUTED -> routing operation %s in %s mode against %s/%s...",
            iTask.getName().toUpperCase(), EXECUTION_MODE.SYNCHRONOUS, nodeId, iTask.getDatabaseName());

        try {
          // EXECUTES ON THE TARGET NODE
          final DistributedTask<Object> task = new DistributedTask<Object>((Callable<Object>) iTask, iKey);
          final Object remoteResult = executeOperation(task, EXECUTION_MODE.SYNCHRONOUS, null);

          // APPLY LOCALLY TOO
          final Object localResult = iTask.setRedistribute(false).call();

          if (remoteResult != null && localResult != null)
            if (!remoteResult.equals(localResult))
              OLogManager.instance().warn(this,
                  "DISTRIBUTED -> detected conflict on %s in %s mode against %s/%s: remote {%s} != local {%s}",
                  iTask.getName().toUpperCase(), EXECUTION_MODE.SYNCHRONOUS, nodeId, iTask.getDatabaseName(), remoteResult,
                  localResult);

          return localResult;

        } catch (MemberLeftException e) {
          // RETRY
          OLogManager
              .instance()
              .warn(
                  this,
                  "DISTRIBUTED -> error on execution of operation in %s mode against node %s because node left. Re-route it in transparent way",
                  e, EXECUTION_MODE.SYNCHRONOUS, nodeId);
          return routeOperation2Node(iKey, iTask);
        }
      }
    } catch (ExecutionException e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation in %s mode against node %s", e,
          EXECUTION_MODE.SYNCHRONOUS, nodeId);
      throw e;
    } catch (InterruptedException e) {
      Thread.interrupted();

    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation in %s mode against node %s", e,
          EXECUTION_MODE.SYNCHRONOUS, nodeId);
      throw new ExecutionException("error on execution of operation in " + EXECUTION_MODE.SYNCHRONOUS + " mode against node %s", e);
    }

    return null;
  }

  public boolean isLocalNodeOwner(final Object iKey) {
    return hazelcastInstance.getPartitionService().getPartition(iKey).getOwner()
        .equals(hazelcastInstance.getCluster().getLocalMember());
  }

  public String getOwnerNode(final Object iKey) {
    return getNodeId(hazelcastInstance.getPartitionService().getPartition(iKey).getOwner());
  }

  @SuppressWarnings("unchecked")
  public Collection<Object> sendOperation2Nodes(final Set<String> iNodeIds, final OAbstractDistributedTask<? extends Object> iTask)
      throws ODistributedException {
    final Set<Member> members = new HashSet<Member>();
    for (String nodeId : iNodeIds) {
      final Member m = remoteClusterNodes.get(nodeId);
      if (m == null)
        OLogManager.instance().warn(this, "DISTRIBUTED -> cannot execute operation on remote member %s because is disconnected",
            nodeId);
      else
        members.add(m);
    }

    final MultiTask<Object> task = new MultiTask<Object>((Callable<Object>) iTask, members);

    ExecutionCallback<Object> callback = null;
    if (iTask.getMode() == EXECUTION_MODE.ASYNCHRONOUS)
      callback = new ExecutionCallback<Object>() {
        @SuppressWarnings("unused")
        @Override
        public void done(Future<Object> future) {
          try {
            if (!future.isCancelled()) {
              // CHECK FOR CONFLICTS
              Object result = future.get();
            }
          } catch (Exception e) {
            OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation in ASYNCH mode against nodes: %s",
                members, e);
          }
        }
      };

    Collection<Object> result = null;
    try {
      result = (Collection<Object>) executeOperation(task, iTask.getMode(), callback);
    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation in %s mode against nodes: %s", e,
          iTask.getMode(), members);
      throw new ODistributedException("Error on executing remote operation in " + iTask.getMode() + " mode against nodes: "
          + members, e);
    }

    return result;
  }

  @Override
  public ODocument getDatabaseConfiguration(String iDatabaseName) {
    // SEARCH IN THE CLUSTER'S DISTRIBUTED CONFIGURATION
    final IMap<String, Object> distributedConfiguration = getConfigurationMap();
    ODocument cfg = (ODocument) distributedConfiguration.get("db." + iDatabaseName);

    if (cfg == null) {
      cfg = super.getDatabaseConfiguration(iDatabaseName);
      // STORE IT IN THE CLUSTER CONFIGURATION
      distributedConfiguration.put("db." + iDatabaseName, cfg);
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

    List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : OServerMain.server().getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress());
    }
    return nodeCfg;
  }

  public void registerAndAlignNodes() {
    hazelcastInstance.getCluster().addMembershipListener(this);

    for (Member clusterMember : hazelcastInstance.getCluster().getMembers()) {
      final String nodeId = getNodeId(clusterMember);

      // ALIGN THE ALL THE CONFIGURED DATABASES
      if (getLocalNodeId().equals(nodeId)) {
        // IT'S ME
        continue;
      }

      remoteClusterNodes.put(nodeId, clusterMember);
    }

    OLogManager.instance().warn(this, "DISTRIBUTED -> detected running nodes %s", remoteClusterNodes.keySet());

    synchronized (synchronizers) {
      for (Entry<String, OStorageSynchronizer> entry : synchronizers.entrySet()) {
        final String dbName = entry.getKey();

        try {
          final long[] lastOperationId = entry.getValue().getLog().getLastOperationId();

          OLogManager.instance().warn(this, "DISTRIBUTED --> send align request for database %s", dbName);

          sendOperation2Nodes(remoteClusterNodes.keySet(), new OAlignRequestDistributedTask(getLocalNodeId(), dbName,
              EXECUTION_MODE.ASYNCHRONOUS, lastOperationId[0], lastOperationId[1]));

        } catch (IOException e) {
          OLogManager.instance().warn(this, "DISTRIBUTED -> error on retrieve last operation id from the log for db %s", dbName);
        }
      }
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

  protected String getNodeId(final Member iMember) {
    return iMember.getInetSocketAddress().toString().substring(1);
  }

  public Set<String> getRemoteNodeIds() {
    return remoteClusterNodes.keySet();
  }

  public Set<String> getRemoteNodeIdsBut(final String iNodeid) {
    final Set<String> otherNodes = remoteClusterNodes.keySet();
    final Set<String> set = new HashSet<String>(otherNodes.size());
    for (String item : remoteClusterNodes.keySet()) {
      if (!item.equals(iNodeid))
        set.add(item);
    }
    return set;
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    aligningNodes.add(getNodeId(iEvent.getMember()));
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
      remoteClusterNodes.put(nodeId, iEvent.getMember());
      aligningNodes.remove(nodeId);
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryRemoved(EntryEvent<String, Object> iEvent) {
    if (iEvent.getKey().startsWith("node.")) {
      final String nodeId = ((ODocument) iEvent.getValue()).field("id");
      OLogManager.instance().warn(this, "DISTRIBUTED -> disconnected cluster node %s", nodeId);
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

  public boolean isAligningNode(final String iNodeId) {
    return aligningNodes.contains(iNodeId);
  }

  public int getNodeNumber() {
    return nodeNumber;
  }

  public static HazelcastInstance getHazelcastInstance() {
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

  /**
   * Publishes the local node configuration to the cluster.
   */
  protected void publishNodeConfiguration() {
    final IMap<String, Object> map = getConfigurationMap();
    final String nodeName = "node." + getLocalNodeId();
    if (map.containsKey(nodeName))
      throw new ODistributedException("Node '" + nodeName
          + "' is already configured in current cluster. Assure the node ids are different");

    final ODocument nodeConfiguration = getLocalNodeConfiguration();
    map.put(nodeName, nodeConfiguration);
    map.addEntryListener(this, true);

    OLogManager.instance().warn(this, "DISTRIBUTED <> published node configuration for %s", getLocalNodeId());
  }

  protected IMap<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  protected ILock getLock() {
    return getHazelcastInstance().getLock("orientdb");
  }

  protected Object executeOperation(final DistributedTask<Object> task, final EXECUTION_MODE iMode,
      final ExecutionCallback<Object> callback) throws ExecutionException, InterruptedException {
    if (iMode == EXECUTION_MODE.ASYNCHRONOUS)
      task.setExecutionCallback(callback);

    hazelcastInstance.getExecutorService().execute(task);

    if (iMode == EXECUTION_MODE.SYNCHRONOUS)
      return task.get();

    return null;
  }

  /**
   * Initializes distributed databases.
   */
  protected void initDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      OLogManager.instance().warn(this, "DISTRIBUTED <> opening database %s...", storageEntry.getKey());
      getDatabaseSynchronizer(storageEntry.getKey());
    }
  }
}
