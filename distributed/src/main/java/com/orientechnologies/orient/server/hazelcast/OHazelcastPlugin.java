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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfilerData.OProfilerEntry;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.OBuffer;
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
import com.orientechnologies.orient.server.distributed.task.ODeployDatabaseTask;
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
  protected OHazelcastDistributedMessageService messageService;
  protected long                                timeOffset             = 0;
  protected Date                                startedOn              = new Date();

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
      OLogManager.instance().warn(this, "Generating new node name for current node: %s", nodeName);

      // SALVE THE NODE NAME IN CONFIGURATION
      boolean found = false;
      final OServerConfiguration cfg = iServer.getConfiguration();
      for (OServerHandlerConfiguration h : cfg.handlers) {
        if (h.clazz.equals(getClass().getName())) {
          for (OServerParameterConfiguration p : h.parameters) {
            if (p.name.equals("nodeName")) {
              found = true;
              p.value = nodeName;
              break;
            }
          }

          if (!found) {
            h.parameters = OArrays.copyOf(h.parameters, h.parameters.length + 1);
            h.parameters[h.parameters.length - 1] = new OServerParameterConfiguration("nodeName", nodeName);
          }

          try {
            iServer.saveConfiguration();
          } catch (IOException e) {
            throw new OConfigurationException("Cannot save server configuration", e);
          }
          break;
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

    try {
      hazelcastInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(hazelcastConfigFile));

      nodeId = hazelcastInstance.getCluster().getLocalMember().getUuid();
      timeOffset = System.currentTimeMillis() - hazelcastInstance.getCluster().getClusterTime();
      cachedClusterNodes.put(getLocalNodeName(), hazelcastInstance.getCluster().getLocalMember());

      OServer.registerServerInstance(getLocalNodeName(), serverInstance);

      final IMap<String, Object> configurationMap = getConfigurationMap();
      configurationMap.addEntryListener(this, true);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        final String memberName = getNodeName(m);
        if (memberName != null)
          cachedClusterNodes.put(memberName, m);
      }

      messageService = new OHazelcastDistributedMessageService(this);

      loadDistributedDatabases();

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
    // members.add(getLocalNodeConfiguration());
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
    nodeCfg.field("startedOn", startedOn);

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
  public Object sendRequest(final String iDatabaseName, final String iClusterName, final OAbstractRemoteTask iTask,
      final EXECUTION_MODE iExecutionMode) {
    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), iDatabaseName, iClusterName,
        iTask, iExecutionMode);

    final OHazelcastDistributedDatabase db = messageService.getDatabase(iDatabaseName);

    final ODistributedResponse response = db.send(req);
    if (response != null)
      return response.getPayload();

    return null;
  }

  @Override
  public void sendRequest2Node(final String iDatabaseName, final String iTargetNodeName, final OAbstractRemoteTask iTask) {
    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), iDatabaseName, null, iTask,
        EXECUTION_MODE.NO_RESPONSE);

    final OHazelcastDistributedDatabase db = messageService.getDatabase(iDatabaseName);

    db.send2Node(req, iTargetNodeName);
  }

  public Set<String> getManagedDatabases() {
    return messageService.getDatabases();
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public String getLocalNodeId() {
    return nodeId;
  }

  @Override
  public void onCreate(final ODatabase iDatabase) {
    final OHazelcastDistributedDatabase distribDatabase = messageService.registerDatabase(iDatabase.getName()).setOnline(true);
    distribDatabase.configureDatabase((ODatabaseDocumentTx) ((ODatabaseComplex<?>) iDatabase).getDatabaseOwner());
    // distribDatabase.
    onOpen(iDatabase);
  }

  @SuppressWarnings("unchecked")
  public ODocument getStats() {
    final ODocument doc = new ODocument();

    final Map<String, HashMap<String, Object>> nodes = new HashMap<String, HashMap<String, Object>>();
    doc.field("nodes", nodes);

    Map<String, Object> localNode = new HashMap<String, Object>();
    doc.field("localNode", localNode);

    localNode.put("name", getLocalNodeName());

    Map<String, Object> databases = new HashMap<String, Object>();
    localNode.put("databases", databases);
    for (String dbName : messageService.getDatabases()) {
      Map<String, Object> db = new HashMap<String, Object>();
      databases.put(dbName, db);
      final OProfilerEntry chrono = Orient.instance().getProfiler().getChrono("distributed.replication." + dbName + ".resynch");
      if (chrono != null)
        db.put("resync", new ODocument().fromJSON(chrono.toJSON()));
    }

    for (Entry<String, QueueConfig> entry : hazelcastInstance.getConfig().getQueueConfigs().entrySet()) {
      final String queueName = entry.getKey();

      if (!queueName.startsWith(OHazelcastDistributedMessageService.NODE_QUEUE_PREFIX))
        continue;

      final IQueue<Object> queue = hazelcastInstance.getQueue(queueName);

      final String[] names = queueName.split("\\.");

      HashMap<String, Object> node = nodes.get(names[2]);
      if (node == null) {
        node = new HashMap<String, Object>();
        nodes.put(names[2], node);
      }

      if (names[3].equals("response")) {
        node.put("responses", queue.size());
      } else {
        final String dbName = names[3];

        Map<String, Object> db = (HashMap<String, Object>) node.get(dbName);
        if (db == null) {
          db = new HashMap<String, Object>(2);
          node.put(dbName, db);
        }

        db.put("requests", queue.size());
        final Object lastMessage = queue.peek();
        if (lastMessage != null)
          db.put("lastMessage", lastMessage.toString());
      }
    }

    return doc;
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
      final ODocument cfg = (ODocument) iEvent.getValue();
      cachedClusterNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());

      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
          "added node configuration id=%s name=%s, now %d nodes are configured", iEvent.getMember(),
          getNodeName(iEvent.getMember()), cachedClusterNodes.size());

      installNewDatabases();

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

  /**
   * Executes the request on local node. In case of error returns the Exception itself
   * 
   * @param database
   */
  public Serializable executeOnLocalNode(final ODistributedRequest req, ODatabaseDocumentTx database) {
    final OAbstractRemoteTask task = req.getPayload();

    try {
      return (Serializable) task.execute(serverInstance, this, database);
    } catch (Throwable e) {
      return e;
    }
  }

  @Override
  public ODistributedPartition newPartition(final List<String> partition) {
    return new OHazelcastDistributionPartition(partition);
  }

  protected IMap<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  /**
   * Initializes all the available server's databases as distributed.
   */
  protected void loadDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "opening database '%s'...", databaseName);

        if (!getConfigurationMap().containsKey(CONFIG_DATABASE_PREFIX + databaseName)) {
          // PUBLISH CFG FIRST TIME
          final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);
          getConfigurationMap().put(CONFIG_DATABASE_PREFIX + databaseName, cfg.serialize());
        }

        messageService.registerDatabase(databaseName).configureDatabase(null).setOnline(true);
      }
    }

    installNewDatabases();
  }

  @SuppressWarnings("unchecked")
  protected void installNewDatabases() {
    final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();

    for (Entry<String, Object> entry : getConfigurationMap().entrySet()) {
      if (entry.getKey().startsWith(CONFIG_DATABASE_PREFIX)) {
        final String databaseName = entry.getKey().substring(CONFIG_DATABASE_PREFIX.length());

        if (!configuredDatabases.contains(databaseName)) {
          final ODocument config = (ODocument) entry.getValue();
          final Boolean autoDeploy = config.field("autoDeploy");
          if (autoDeploy != null && autoDeploy) {

            final OHazelcastDistributedDatabase distrDatabase = messageService.registerDatabase(databaseName).configureDatabase(
                null);

            final Map<String, OBuffer> results = (Map<String, OBuffer>) sendRequest(databaseName, null, new ODeployDatabaseTask(),
                EXECUTION_MODE.RESPONSE);

            final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

            // EXTRACT THE REAL RESULT
            OBuffer result = null;
            for (Entry<String, OBuffer> r : results.entrySet())
              if (r.getValue().getBuffer() != null && r.getValue().getBuffer().length > 0) {
                result = r.getValue();

                ODistributedServerLog.warn(this, getLocalNodeName(), r.getKey(), DIRECTION.IN, "installing database %s in %s...",
                    databaseName, dbPath);

                break;
              }

            if (result == null)
              throw new ODistributedException("No response received from remote nodes for auto-deploy of database");

            new File(dbPath).mkdirs();
            final ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:" + dbPath);

            final ByteArrayInputStream in = new ByteArrayInputStream(result.getBuffer());
            try {
              db.restore(in, null);
              in.close();

              db.close();
              Orient.instance().unregisterStorageByName(db.getName());

              distrDatabase.setOnline(true);

            } catch (IOException e) {
              ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.IN,
                  "error on copying database '%s' on local server", e, databaseName);
            }
          }
        }
      }
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
