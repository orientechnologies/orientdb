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

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OCopyDatabaseChunkTask;
import com.orientechnologies.orient.server.distributed.task.ODeployDatabaseTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object>,
    OCommandOutputListener {

  protected static final String                 CONFIG_NODE_PREFIX     = "node.";
  protected static final String                 CONFIG_DBSTATUS_PREFIX = "dbstatus.";
  protected static final String                 CONFIG_DATABASE_PREFIX = "database.";

  protected String                              nodeId;
  protected String                              hazelcastConfigFile    = "hazelcast.xml";
  protected Map<String, Member>                 activeNodes            = new ConcurrentHashMap<String, Member>();
  protected OHazelcastDistributedMessageService messageService;
  protected long                                timeOffset             = 0;
  protected Date                                startedOn              = new Date();

  protected volatile NODE_STATUS                status                 = NODE_STATUS.OFFLINE;

  protected String                              membershipListenerRegistration;

  protected volatile HazelcastInstance          hazelcastInstance;
  protected Object                              installDatabaseLock    = new Object();
  protected long                                lastClusterChangeOn;

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

    super.startup();

    status = NODE_STATUS.STARTING;

    final String localNodeName = getLocalNodeName();

    OLogManager.instance().info(this, "Starting distributed server '%s'...", localNodeName);

    activeNodes.clear();

    try {
      hazelcastInstance = configureHazelcast();

      nodeId = hazelcastInstance.getCluster().getLocalMember().getUuid();
      timeOffset = System.currentTimeMillis() - hazelcastInstance.getCluster().getClusterTime();
      activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());

      membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

      OServer.registerServerInstance(localNodeName, serverInstance);

      final IMap<String, Object> configurationMap = (IMap<String, Object>) getConfigurationMap();
      configurationMap.addEntryListener(this, true);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (!m.getUuid().equals(getLocalNodeId())) {
          final String memberName = getNodeName(m);
          if (memberName != null)
            activeNodes.put(memberName, m);
          else if (!m.equals(hazelcastInstance.getCluster().getLocalMember()))
            ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.NONE, "Cannot find configuration for member: %s", m);
        }
      }

      messageService = new OHazelcastDistributedMessageService(this);

      // PUBLISH LOCAL NODE CFG
      configurationMap.put(CONFIG_NODE_PREFIX + getLocalNodeId(), getLocalNodeConfiguration());

      installNewDatabases(true);

      loadDistributedDatabases();

      // REGISTER CURRENT MEMBERS
      setNodeStatus(NODE_STATUS.ONLINE);

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

    OLogManager.instance().warn(this, "Shutting down node %s...", getLocalNodeName());
    setNodeStatus(NODE_STATUS.SHUTDOWNING);

    if (messageService != null)
      messageService.shutdown();

    super.shutdown();

    activeNodes.clear();
    if (membershipListenerRegistration != null) {
      hazelcastInstance.getCluster().removeMembershipListener(membershipListenerRegistration);
    }

    // AVOID TO REMOVE THE CFG TO PREVENT OTHER NODES TO UN-REGISTER IT
    // getConfigurationMap().remove(CONFIG_NODE_PREFIX + getLocalNodeId());

    if (hazelcastInstance != null)
      try {
        hazelcastInstance.shutdown();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on shutting down Hazelcast instance", e);
      } finally {
        hazelcastInstance = null;
      }

    setNodeStatus(NODE_STATUS.OFFLINE);
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
    for (Member member : activeNodes.values()) {
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
    nodeCfg.field("startedOn", startedOn);

    List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : serverInstance.getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress(true));
    }
    nodeCfg.field("databases", getManagedDatabases());

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

    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "updated node status to '%s'", status);
  }

  public boolean checkNodeStatus(final NODE_STATUS iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  @Override
  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName) {
    return (DB_STATUS) getConfigurationMap().get(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
  }

  @Override
  public boolean checkDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    return getDatabaseStatus(iNode, iDatabaseName) == iStatus;
  }

  @Override
  public void setDatabaseStatus(final String iDatabaseName, final DB_STATUS iStatus) {
    getConfigurationMap().put(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + getLocalNodeName() + "." + iDatabaseName, iStatus);
  }

  @Override
  public Object sendRequest(final String iDatabaseName, final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes, final OAbstractRemoteTask iTask, final EXECUTION_MODE iExecutionMode) {

    if (iTask.isRequireNodeOnline())
      // WAIT THE DATABASE ON THE NODE IS ONLINE
      while (getDatabaseStatus(getLocalNodeName(), iDatabaseName) != DB_STATUS.ONLINE) {
        try {
          ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
              "waiting for the database '%s' is online on local node...", iDatabaseName);

          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }

    checkForClusterRebalance(iDatabaseName);

    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), iDatabaseName, iTask,
        iExecutionMode);

    final ODatabaseRecord currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (currentDatabase != null && currentDatabase.getUser() != null)
      // SET CURRENT DATABASE NAME
      req.setUserName(currentDatabase.getUser().getName());

    final OHazelcastDistributedDatabase db = messageService.getDatabase(iDatabaseName);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "No nodes configured for partition '%s.%s' request: %s", iDatabaseName, iClusterNames, req);
      throw new ODistributedException("No nodes configured for partition '" + iDatabaseName + "." + iClusterNames + "' request: "
          + req);
    }

    final ODistributedResponse response = db.send2Nodes(req, iClusterNames, iTargetNodes);
    if (response != null)
      return response.getPayload();

    return null;
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
    final OHazelcastDistributedDatabase distribDatabase = messageService.registerDatabase(iDatabase.getName());
    distribDatabase.configureDatabase(false, false).setOnline();
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
    localNode.put("averageResponseTime", messageService.getAverageResponseTime());

    Map<String, Object> databases = new HashMap<String, Object>();
    localNode.put("databases", databases);
    for (String dbName : messageService.getDatabases()) {
      Map<String, Object> db = new HashMap<String, Object>();
      databases.put(dbName, db);
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
    if (iMember == null)
      return "?";

    final ODocument cfg = getNodeConfigurationById(iMember.getUuid());
    if (cfg != null)
      return cfg.field("name");

    return "ext:" + iMember.getUuid();
  }

  public Set<String> getRemoteNodeIds() {
    return activeNodes.keySet();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    updateLastClusterChange();
    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "added new node id=%s name=%s", iEvent.getMember(),
        getNodeName(iEvent.getMember()));
  }

  @Override
  public void updateLastClusterChange() {
    lastClusterChangeOn = System.currentTimeMillis();
  }

  /**
   * Removes the node map entry.
   */
  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    updateLastClusterChange();
    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "node removed id=%s name=%s", iEvent.getMember(),
        getNodeName(iEvent.getMember()));

    final Member member = iEvent.getMember();

    final String nodeName = getNodeName(member);
    if (nodeName != null) {
      activeNodes.remove(nodeName);

      // REMOVE NODE IN DB CFG
      if (messageService != null) {
        final Set<String> dbs = messageService.getDatabases();
        if (dbs != null)
          for (String dbName : dbs)
            messageService.getDatabase(dbName).removeNodeInConfiguration(nodeName, false);

        // REMOVE THE SERVER'S RESPONSE QUEUE
        messageService.removeQueue(OHazelcastDistributedMessageService.getResponseQueueName(nodeName));
      }
    }

    OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
  }

  @Override
  public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
  }

  @Override
  public void entryAdded(final EntryEvent<String, Object> iEvent) {
    if (iEvent.getMember() == null)
      // IGNORE IT
      return;

    final String key = iEvent.getKey();
    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
        final ODocument cfg = (ODocument) iEvent.getValue();
        final String nodeName = (String) cfg.field("name");

        if (nodeName.equals(getLocalNodeName())) {
          ODistributedServerLog.error(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
              "Found a new node with the same name as current: '" + nodeName
                  + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file");

          throw new ODistributedException("Found a new node with the same name as current: '" + nodeName
              + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file");
        }

        activeNodes.put(nodeName, (Member) iEvent.getMember());

        ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
            "added node configuration id=%s name=%s, now %d nodes are configured", iEvent.getMember(),
            getNodeName(iEvent.getMember()), activeNodes.size());

        installNewDatabases(false);
      }

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      updateCachedDatabaseConfiguration(key.substring(CONFIG_DATABASE_PREFIX.length()), (ODocument) iEvent.getValue(), true, false);
      OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());

      if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember()))
        installNewDatabases(false);

    } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
          "received added status %s=%s", key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());
    }
  }

  @Override
  public void entryUpdated(final EntryEvent<String, Object> iEvent) {
    final String key = iEvent.getKey();
    final String eventNodeName = getNodeName(iEvent.getMember());

    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), eventNodeName, DIRECTION.NONE,
          "updated node configuration id=%s name=%s", iEvent.getMember(), eventNodeName);

      final ODocument cfg = (ODocument) iEvent.getValue();
      activeNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());
      updateLastClusterChange();

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
        final String dbName = key.substring(CONFIG_DATABASE_PREFIX.length());

        ODistributedServerLog.info(this, getLocalNodeName(), eventNodeName, DIRECTION.NONE, "update configuration db=%s", dbName);

        updateLastClusterChange();

        installNewDatabases(false);
        updateCachedDatabaseConfiguration(dbName, (ODocument) iEvent.getValue(), true, false);
        OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());

        updateLastClusterChange();
      }
    } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), eventNodeName, DIRECTION.IN, "received updated status %s=%s",
          key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

      updateLastClusterChange();
    }
  }

  @Override
  public void entryRemoved(final EntryEvent<String, Object> iEvent) {
    final String key = iEvent.getKey();
    if (key.startsWith(CONFIG_NODE_PREFIX)) {
      final String nName = getNodeName(iEvent.getMember());
      if (nName != null) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "removed node configuration id=%s name=%s",
            iEvent.getMember(), nName);
        activeNodes.remove(nName);
      }

      updateLastClusterChange();

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      synchronized (cachedDatabaseConfiguration) {
        cachedDatabaseConfiguration.remove(key.substring(CONFIG_DATABASE_PREFIX.length()));
      }
      updateLastClusterChange();

    } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
          "received removed status %s=%s", key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());
      updateLastClusterChange();
    }
  }

  @Override
  public void entryEvicted(EntryEvent<String, Object> iEvent) {
  }

  @Override
  public boolean isNodeAvailable(final String iNodeName, final String iDatabaseName) {
    if (activeNodes.containsKey(iNodeName)) {
      final Boolean nodeStatus = checkDatabaseStatus(iNodeName, iDatabaseName, DB_STATUS.ONLINE);
      if (nodeStatus != null && nodeStatus)
        return nodeStatus;
    }
    return false;
  }

  public boolean isOffline() {
    return status != NODE_STATUS.ONLINE;
  }

  public void waitUntilOnline() throws InterruptedException {
    while (!status.equals(NODE_STATUS.ONLINE))
      Thread.sleep(100);
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
   */
  public Serializable executeOnLocalNode(final ODistributedRequest req, final ODatabaseDocumentTx database) {
    if (database != null && !(database.getStorage() instanceof ODistributedStorage))
      throw new ODistributedException("Distributed storage was not installed for database '" + database.getName()
          + "'. Implementation found: " + database.getStorage().getClass().getName());

    final OAbstractRemoteTask task = req.getTask();

    try {
      if (database != null)
        ((ODistributedStorage) database.getStorage()).setLastOperationId(req.getId());

      return (Serializable) task.execute(serverInstance, this, database);
    } catch (Throwable e) {
      return e;
    }
  }

  @Override
  public OHazelcastDistributedMessageService getMessageService() {
    return messageService;
  }

  public void updateCachedDatabaseConfiguration(String iDatabaseName, ODocument cfg, boolean iSaveToDisk, boolean iDeployToCluster) {
    final boolean updated = super.updateCachedDatabaseConfiguration(iDatabaseName, cfg, iSaveToDisk);

    if (updated && iDeployToCluster)
      // DEPLOY THE CONFIGURATION TO THE CLUSTER
      getConfigurationMap().put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabaseName, cfg);
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
  }

  @Override
  public void onMessage(String iText) {
    OLogManager.instance().info(this, iText);
  }

  public int getAvailableNodes(final String iDatabaseName) {
    int availableNodes = 0;
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        availableNodes++;
    }
    return availableNodes;
  }

  @Override
  public Map<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  public boolean installDatabase(boolean iStartup, String databaseName, ODocument config) {

    final Boolean hotAlignment = config.field("hotAlignment");
    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();
    if (configuredDatabases.contains(databaseName)) {
      if (iStartup && hotAlignment != null && !hotAlignment) {
        // DROP THE DATABASE ON CURRENT NODE
        ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
            "dropping local database '%s' in '%s' and get a fresh copy from a remote node...", databaseName, dbPath);

        Orient.instance().unregisterStorageByName(databaseName);

        OFileUtils.deleteRecursively(new File(dbPath));
      } else
        // HOT ALIGNMENT RUNNING, DON'T INSTALL THE DB FROM SCRATCH BUT RATHER LET TO THE NODE TO ALIGN BY READING THE QUEUE
        return false;
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final OHazelcastDistributedDatabase distrDatabase = messageService.registerDatabase(databaseName);

    distrDatabase.configureDatabase(false, false);

    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    // GET ALL THE
    final Collection<String> nodes = cfg.getServers();
    nodes.remove(getLocalNodeName());

    ODistributedServerLog.warn(this, getLocalNodeName(), nodes.toString(), DIRECTION.OUT,
        "requesting deploy of database '%s' on local server...", databaseName);

    final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, nodes, new ODeployDatabaseTask(),
        EXECUTION_MODE.RESPONSE);

    ODistributedServerLog.warn(this, getLocalNodeName(), nodes.toString(), DIRECTION.OUT, "deploy returned: %s", results);

    // EXTRACT THE REAL RESULT
    for (Entry<String, Object> r : results.entrySet()) {
      final Object value = r.getValue();

      if (value instanceof Boolean) {
        continue;
      } else if (value instanceof Throwable) {
        ODistributedServerLog.error(this, getLocalNodeName(), r.getKey(), DIRECTION.IN, "error on installing database %s in %s",
            (Exception) value, databaseName, dbPath);
      } else if (value instanceof ODistributedDatabaseChunk) {
        ODistributedDatabaseChunk chunk = (ODistributedDatabaseChunk) value;

        // DISCARD ALL THE MESSAGES BEFORE THE BACKUP
        distrDatabase.setWaitForMessage(chunk.getLastOperationId() + 1);

        final String fileName = Orient.getTempPath() + "install_" + databaseName + ".zip";

        ODistributedServerLog.info(this, getLocalNodeName(), r.getKey(), DIRECTION.IN, "copying remote database '%s' to: %s",
            databaseName, fileName);

        final File file = new File(fileName);
        if (file.exists())
          file.delete();

        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          throw new ODistributedException("Error on creating temp database file to install locally", e);
        }

        FileOutputStream out = null;
        try {
          out = new FileOutputStream(fileName, false);

          long fileSize = writeDatabaseChunk(1, chunk, out);
          for (int chunkNum = 2; !chunk.last; chunkNum++) {
            final Object result = sendRequest(databaseName, null, Collections.singleton(r.getKey()), new OCopyDatabaseChunkTask(
                chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length), EXECUTION_MODE.RESPONSE);

            if (result instanceof Boolean)
              continue;
            else if (result instanceof Exception) {
              ODistributedServerLog.error(this, getLocalNodeName(), r.getKey(), DIRECTION.IN,
                  "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
            } else if (result instanceof ODistributedDatabaseChunk) {
              chunk = (ODistributedDatabaseChunk) result;
              fileSize += writeDatabaseChunk(chunkNum, chunk, out);
            }
          }

          ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "database copied correctly, size=%s",
              OFileUtils.getSizeAsString(fileSize));

        } catch (Exception e) {
          ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
              "error on transferring database '%s' to '%s'", e, databaseName, fileName);
          throw new ODistributedException("Error on transferring database", e);
        } finally {
          try {
            if (out != null) {
              out.flush();
              out.close();
            }
          } catch (IOException e) {
          }
        }

        installDatabase(distrDatabase, databaseName, dbPath, r.getKey(), fileName);
        return true;

      } else
        throw new IllegalArgumentException("Type " + value + " not supported");
    }

    throw new ODistributedException("No response received from remote nodes for auto-deploy of database");

  }

  protected HazelcastInstance configureHazelcast() throws FileNotFoundException {
    return Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(hazelcastConfigFile));
  }

  /**
   * Initializes all the available server's databases as distributed.
   */
  protected void loadDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "opening database '%s'...", databaseName);

        final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

        if (!getConfigurationMap().containsKey(CONFIG_DATABASE_PREFIX + databaseName)) {
          // PUBLISH CFG FIRST TIME
          getConfigurationMap().put(CONFIG_DATABASE_PREFIX + databaseName, cfg.serialize());
        }

        final boolean hotAlignment = cfg.isHotAlignment();

        final OHazelcastDistributedDatabase db = messageService.registerDatabase(databaseName).configureDatabase(hotAlignment,
            hotAlignment);

        if (!db.isRestoringMessages())
          // NO PENDING MESSAGE, SET IT ONLINE
          db.setOnline();
        else
          db.initDatabaseInstance();
      }
    }
  }

  protected void installNewDatabases(final boolean iStartup) {
    if (activeNodes.size() <= 1)
      // NO OTHER NODES WHERE ALIGN
      return;

    // LOCKING THIS RESOURCE PREVENT CONCURRENT INSTALL OF THE SAME DB
    synchronized (installDatabaseLock) {
      for (Entry<String, Object> entry : getConfigurationMap().entrySet()) {
        if (entry.getKey().startsWith(CONFIG_DATABASE_PREFIX)) {
          final String databaseName = entry.getKey().substring(CONFIG_DATABASE_PREFIX.length());

          final ODocument config = (ODocument) entry.getValue();
          final Boolean autoDeploy = config.field("autoDeploy");

          if (autoDeploy != null && autoDeploy) {
            installDatabase(iStartup, databaseName, config);
          }
        }
      }
    }
  }

  protected long writeDatabaseChunk(final int iChunkId, final ODistributedDatabaseChunk chunk, final FileOutputStream out)
      throws IOException {

    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId,
        chunk.offset, OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }

  protected void installDatabase(final OHazelcastDistributedDatabase distrDatabase, final String databaseName, final String dbPath,
      final String iNode, final String iDatabaseCompressedFile) {
    ODistributedServerLog.warn(this, getLocalNodeName(), iNode, DIRECTION.IN, "installing database '%s' to: %s...", databaseName,
        dbPath);

    try {
      File f = new File(iDatabaseCompressedFile);

      new File(dbPath).mkdirs();
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:" + dbPath);

      final FileInputStream in = new FileInputStream(f);
      try {
        db.restore(in, null, null, this);
      } finally {
        in.close();
      }

      db.close();
      Orient.instance().unregisterStorageByName(db.getURL().substring(db.getStorage().getType().length() + 1));

      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "installed database '%s'", databaseName);

      distrDatabase.setOnline();

      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "database '%s' is online", databaseName);

    } catch (IOException e) {
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.IN, "error on copying database '%s' on local server", e,
          databaseName);
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

        updateCachedDatabaseConfiguration(iDatabaseName, cfg, false, false);
        return cfg;
      }
    }

    // NO NODE IN CLUSTER, LOAD FROM FILE
    return super.loadDatabaseConfiguration(iDatabaseName, file);
  }

  /**
   * Pauses the request if the distributed cluster need to be rebalanced because change of shape (add/remove nodes) or a node that
   * is much slower than the average.
   * 
   * @param iDatabaseName
   */
  protected void checkForClusterRebalance(final String iDatabaseName) {
    if (activeNodes.size() <= 1)
      return;

    if (getAvailableNodes(iDatabaseName) <= 1)
      return;

    // TODO: SEPARATE METRICS PER DATABASE
    final long averageResponseTime = messageService.getAverageResponseTime();

    // TODO: SELECT THE RIGHT TIMEOUT
    final long timeout = OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();

    if (averageResponseTime > timeout * 75 / 100) {
      long sleep = Math.abs(timeout - averageResponseTime);
      if( sleep > 3000)
        sleep = 3000;

      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
          "slowing down request to avoid to fill queues. Wait for %dms (timeout=%d, averageResponseTime=%d)...", sleep, timeout,
          averageResponseTime);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
      }
    }
  }
}
