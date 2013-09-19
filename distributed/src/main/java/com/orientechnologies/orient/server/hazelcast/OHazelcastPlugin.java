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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
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
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {

  protected static final String                 CONFIG_NODE_PREFIX     = "node.";
  protected static final String                 CONFIG_DATABASE_PREFIX = "database.";

  protected String                              nodeId;
  protected String                              hazelcastConfigFile    = "hazelcast.xml";
  protected Map<String, Member>                 cachedClusterNodes     = new ConcurrentHashMap<String, Member>();
  protected long                                runId                  = -1;
  protected OHazelcastDistributedMessageService messageService;
  protected long                                timeOffset             = 0;

  protected volatile STATUS                     status                 = STATUS.OFFLINE;

  protected String                              membershipListenerRegistration;

  protected volatile HazelcastInstance          hazelcastInstance;

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
        if (h.clazz.equals(getClass().getName())) {
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

    OLogManager.instance().info(this, "Starting distributed server '%s'...", getLocalNodeName());

    cachedClusterNodes.clear();
    managedDatabases.clear();

    try {
      hazelcastInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(hazelcastConfigFile));

      nodeId = hazelcastInstance.getCluster().getLocalMember().getUuid();
      timeOffset = System.currentTimeMillis() - hazelcastInstance.getCluster().getClusterTime();
      cachedClusterNodes.put(getLocalNodeName(), hazelcastInstance.getCluster().getLocalMember());

      OServer.registerServerInstance(getLocalNodeName(), serverInstance);

      final IMap<String, Object> configurationMap = getConfigurationMap();
      configurationMap.addEntryListener(this, true);

      messageService = new OHazelcastDistributedMessageService(this);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        final String memberName = getNodeName(m);
        if (memberName != null)
          cachedClusterNodes.put(memberName, m);
      }

      initDistributedDatabases();

      // GET AND REGISTER THE CLUSTER RUN ID IF NOT PRESENT
      configurationMap.putIfAbsent("runId", hazelcastInstance.getCluster().getClusterTime());
      runId = (Long) getConfigurationMap().get("runId");

      // REGISTER CURRENT MEMBERS
      setStatus(STATUS.ONLINE);

      super.startup();

    } catch (FileNotFoundException e) {
      throw new OConfigurationException("Error on creating Hazelcast instance", e);
    }
  }

  @Override
  public long getDistributedTime(final long iTime) {
    return iTime - timeOffset;
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

    messageService.shutdown();

    super.shutdown();

    cachedClusterNodes.clear();
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

  private void checkForConflicts(final String iDatabaseName, final OAbstractReplicatedTask taskToPropagate,
      final Object localResult, final Map<String, Object> remoteResults, final int minSuccessfulOperations) {

    int successfulReplicatedNodes = 0;

    for (Entry<String, Object> entry : remoteResults.entrySet()) {
      final String remoteNode = entry.getKey();
      final Object remoteResult = entry.getValue();

      if (!(remoteResult instanceof Exception)) {
        successfulReplicatedNodes++;

        if ((localResult == null && remoteResult != null) || (localResult != null && remoteResult == null)
            || (localResult != null && !localResult.equals(remoteResult))) {
          // CONFLICT
          taskToPropagate.handleConflict(iDatabaseName, remoteNode, localResult, remoteResult, null);
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

    cluster.field("localName", instance.getName());
    cluster.field("localId", instance.getCluster().getLocalMember().getUuid());

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    members.add(getLocalNodeConfiguration());
    for (Member member : cachedClusterNodes.values()) {
      members.add(getNodeConfigurationById(member.getUuid()));
    }

    return cluster;
  }

  public ODocument getNodeConfigurationById(final String iNodeId) {
    return (ODocument) getConfigurationMap().get(CONFIG_NODE_PREFIX + iNodeId);
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();

    nodeCfg.field("id", getLocalNodeId());
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
    nodeCfg.field("databases", getManagedDatabases());

    return nodeCfg;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public STATUS getStatus() {
    return status;
  }

  public boolean checkStatus(final STATUS iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  @Override
  public void setStatus(final STATUS iStatus) {
    if (status.equals(iStatus))
      // NO CHANGE
      return;

    status = iStatus;

    getConfigurationMap().put(CONFIG_NODE_PREFIX + getLocalNodeId(), getLocalNodeConfiguration());

    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "updated node status to '%s'", status);
  }

  @Override
  public Object sendRequest(final String iDatabaseName, final OAbstractRemoteTask iTask, final EXECUTION_MODE iExecutionMode) {
    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), iDatabaseName,
        iTask.getClusterName(), iTask, iExecutionMode);

    final ODistributedResponse response = messageService.send(req);
    if (response != null)
      return response.getPayload();

    return null;
  }

  public String[] getManagedDatabases() {
    synchronized (managedDatabases) {
      return managedDatabases.toArray(new String[managedDatabases.size()]);
    }
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public String getLocalNodeId() {
    return nodeId;
  }

  public String getNodeName(final Member iMember) {
    final ODocument cfg = getNodeConfigurationById(iMember.getUuid());
    return (String) (cfg != null ? cfg.field("name") : null);
  }

  public Set<String> getRemoteNodeIds() {
    return cachedClusterNodes.keySet();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "added new node id=%s name=%s", iEvent.getMember(),
        getNodeName(iEvent.getMember()));
  }

  /**
   * Removes the node map entry.
   */
  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "node removed id=%s name=%s", iEvent.getMember(),
        getNodeName(iEvent.getMember()));

    final Member member = iEvent.getMember();
    cachedClusterNodes.remove(getNodeName(member));
    OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
  }

  @Override
  public void entryAdded(EntryEvent<String, Object> iEvent) {
    final String key = iEvent.getKey();
    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "added node configuration id=%s name=%s",
          iEvent.getMember(), getNodeName(iEvent.getMember()));

      final ODocument cfg = (ODocument) iEvent.getValue();
      cachedClusterNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      saveDatabaseConfiguration(key.substring(CONFIG_DATABASE_PREFIX.length()), (ODocument) iEvent.getValue());
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryUpdated(EntryEvent<String, Object> iEvent) {
    final String key = iEvent.getKey();
    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "updated node configuration id=%s name=%s",
          iEvent.getMember(), getNodeName(iEvent.getMember()));

      final ODocument cfg = (ODocument) iEvent.getValue();
      cachedClusterNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      saveDatabaseConfiguration(key.substring(CONFIG_DATABASE_PREFIX.length()), (ODocument) iEvent.getValue());
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
    }
  }

  @Override
  public void entryRemoved(EntryEvent<String, Object> iEvent) {
    final String key = iEvent.getKey();
    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "removed node configuration id=%s name=%s",
          iEvent.getMember(), getNodeName(iEvent.getMember()));

      cachedClusterNodes.remove(getNodeName(iEvent.getMember()));

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      synchronized (cachedDatabaseConfiguration) {
        cachedDatabaseConfiguration.remove(key.substring(CONFIG_DATABASE_PREFIX.length()));
      }
    }
  }

  @Override
  public void entryEvicted(EntryEvent<String, Object> iEvent) {
  }

  @Override
  public boolean isNodeAvailable(final String iNodeName) {
    return cachedClusterNodes.containsKey(iNodeName);
  }

  public boolean isOfflineNodeById(final String iNodeId) {
    final ODocument cfg = getNodeConfigurationById(iNodeId);
    return cfg == null || !cfg.field("status").equals(STATUS.ONLINE.toString());
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

  public Lock getLock(final String iName) {
    return getHazelcastInstance().getLock(iName);
  }

  public Class<? extends OReplicationConflictResolver> getConfictResolverClass() {
    return confictResolverClass;
  }

  @Override
  public String toString() {
    return getLocalNodeName();
  }

  public Serializable executeOnLocalNode(final ODistributedRequest req) {
    final Object requestPayload = req.getPayload();

    if (requestPayload instanceof OAbstractReplicatedTask) {
      // ROLLBACKABLE TASK
      return executeRollbackableTask(req.getDatabaseName(), (OAbstractReplicatedTask) requestPayload);
    } else if (requestPayload instanceof OAbstractRemoteTask) {
      // REMOTE TASK
      return executeTask(req.getDatabaseName(), (OAbstractRemoteTask) requestPayload);
    }

    throw new ODistributedException("Invalid payload in request " + req);
  }

  @Override
  public ODistributedPartition newPartition(final List<String> partition) {
    return new OHazelcastDistributionPartition(partition);
  }

  protected Serializable executeTask(final String databaseName, final OAbstractRemoteTask task) {
    try {
      return (Serializable) task.execute(serverInstance, this, databaseName);
    } catch (Exception e1) {
      // EXCEPTION
      throw new ODistributedException("Error on executing task: " + task, e1);
    }
  }

  protected Serializable executeRollbackableTask(final String databaseName, final OAbstractReplicatedTask task) {
    try {
      final Serializable response = (Serializable) task.execute(serverInstance, this, databaseName);

      return response;

    } catch (Exception e1) {
      if (e1 instanceof ONeedRetryException)
        throw (ONeedRetryException) e1;

      throw new ODistributedException("Error on executing task: " + task, e1);
    }
  }

  protected IMap<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  /**
   * Initializes distributed databases.
   */
  protected void initDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "opening database '%s'...", databaseName);

      messageService.configureDatabase(databaseName);

      checkLocalNodeInConfiguration(databaseName);
    }
  }

  protected void checkLocalNodeInConfiguration(final String iDatabaseName) {
    final String localNode = getLocalNodeName();

    // LOAD DATABASE FILE IF ANY
    loadDatabaseConfiguration(iDatabaseName, getDistributedConfigFile(iDatabaseName));

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabaseName);
    for (String clusterName : cfg.getClusterNames()) {
      final List<List<String>> partitions = cfg.getPartitions(clusterName);
      if (partitions != null)
        for (List<String> partition : partitions) {
          for (String node : partition)
            if (node.equals(localNode))
              // FOUND: DO NOTHING
              return;
        }
    }

    // NOT FOUND: ADD THE NODE IN CONFIGURATION. LOOK FOR $newNode TAG
    boolean dirty = false;
    for (String clusterName : cfg.getClusterNames()) {
      final List<List<String>> partitions = cfg.getPartitions(clusterName);
      if (partitions != null)
        for (int p = 0; p < partitions.size(); ++p) {
          List<String> partition = partitions.get(p);
          for (String node : partition)
            if (node.equalsIgnoreCase(ODistributedConfiguration.NEW_NODE_TAG)) {
              ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "adding node '%s' in partition: %s.%s.%d",
                  localNode, iDatabaseName, clusterName, p);
              partition.add(localNode);
              dirty = true;
              break;
            }
        }
    }

    if (dirty) {
      final ODocument doc = cfg.serialize();
      getConfigurationMap().put(CONFIG_DATABASE_PREFIX + iDatabaseName, doc);
      updateDatabaseConfiguration(iDatabaseName, doc);
    }
  }

  @Override
  protected ODocument loadDatabaseConfiguration(final String iDatabaseName, final File file) {
    // FIRST LOOK IN THE CLUSTER
    if (hazelcastInstance != null) {
      final ODocument cfg = (ODocument) getConfigurationMap().get(CONFIG_DATABASE_PREFIX + iDatabaseName);
      if (cfg != null) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "loaded database configuration from active cluster");

        updateDatabaseConfiguration(iDatabaseName, cfg);
        return cfg;
      }
    }

    // NO NODE IN CLUSTER, LOAD FROM FILE
    return super.loadDatabaseConfiguration(iDatabaseName, file);
  }
}
