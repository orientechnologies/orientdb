/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.orientechnologies.common.console.DefaultConsoleReader;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.sql.OCommandExecutorSQLSyncCluster;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OCopyDatabaseChunkTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordTask;
import com.orientechnologies.orient.server.distributed.task.ORestartNodeTask;
import com.orientechnologies.orient.server.distributed.task.OSyncDatabaseTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin
    implements MembershipListener, EntryListener<String, Object>, OCommandOutputListener {

  public static final String CONFIG_DATABASE_PREFIX = "database.";

  protected static final String                 NODE_NAME_ENV          = "ORIENTDB_NODE_NAME";
  protected static final String                 CONFIG_NODE_PREFIX     = "node.";
  protected static final String                 CONFIG_DBSTATUS_PREFIX = "dbstatus.";
  protected static final int                    DEPLOY_DB_MAX_RETRIES  = 10;
  protected String                              nodeId;
  protected String                              hazelcastConfigFile    = "hazelcast.xml";
  protected Map<String, Member>                 activeNodes            = new ConcurrentHashMap<String, Member>();
  protected OHazelcastDistributedMessageService messageService;
  protected long                                timeOffset             = 0;
  protected Date                                startedOn              = new Date();

  protected volatile NODE_STATUS status = NODE_STATUS.OFFLINE;

  protected String membershipListenerRegistration;

  protected volatile HazelcastInstance          hazelcastInstance;
  protected long                                lastClusterChangeOn;
  protected List<ODistributedLifecycleListener> listeners = new ArrayList<ODistributedLifecycleListener>();

  public OHazelcastPlugin() {
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);

    if (nodeName == null)
      assignNodeName();

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast"))
        hazelcastConfigFile = OSystemVariableResolver.resolveSystemVariables(param.value);
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    super.startup();

    status = NODE_STATUS.STARTING;

    final String localNodeName = getLocalNodeName();

    activeNodes.clear();

    try {
      hazelcastInstance = configureHazelcast();

      nodeId = hazelcastInstance.getCluster().getLocalMember().getUuid();

      OLogManager.instance().info(this, "Starting distributed server '%s' (hzID=%s) dbDir='%s'...", localNodeName, nodeId,
          serverInstance.getDatabaseDirectory());

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

      // PUBLISH LOCAL NODE CFG
      ODocument cfg = getLocalNodeConfiguration();
      ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
      configurationMap.put(CONFIG_NODE_PREFIX + nodeId, cfg);

      if (!configurationMap.containsKey(CONFIG_NODE_PREFIX + nodeId)) {
        // NODE NOT REGISTERED, FORCING SHUTTING DOWN
        ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on registering local node on cluster");
        System.exit(1);
      }

      messageService = new OHazelcastDistributedMessageService(this);

      installNewDatabases(true);

      loadDistributedDatabases();

      // REGISTER CURRENT MEMBERS
      setNodeStatus(NODE_STATUS.ONLINE);

    } catch (Exception e) {
      ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on starting distributed plugin", e);
      System.exit(1);
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

    super.shutdown();

    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    if (messageService != null)
      messageService.shutdown();

    activeNodes.clear();
    if (membershipListenerRegistration != null) {
      hazelcastInstance.getCluster().removeMembershipListener(membershipListenerRegistration);
    }

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

    final HazelcastInstance instance = getHazelcastInstance();
    if (instance == null)
      return null;

    final ODocument cluster = new ODocument();

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
    final ODocument doc = (ODocument) getConfigurationMap().get(CONFIG_NODE_PREFIX + iNodeId);
    if (doc == null)
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT, "Cannot find node with id '%s'", iNodeId);

    return doc;
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
    // if (!activeNodes.containsKey(iNode))
    // return DB_STATUS.OFFLINE;
    //
    final DB_STATUS status = (DB_STATUS) getConfigurationMap()
        .get(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
    return status != null ? status : DB_STATUS.OFFLINE;
  }

  @Override
  public void setDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    getConfigurationMap().put(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName, iStatus);
  }

  @Override
  public Object sendRequest(final String iDatabaseName, final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes, final OAbstractRemoteTask iTask, final EXECUTION_MODE iExecutionMode) {

    checkForClusterRebalance(iDatabaseName);

    final OHazelcastDistributedRequest req = new OHazelcastDistributedRequest(getLocalNodeName(), iDatabaseName, iTask,
        iExecutionMode);

    final ODatabaseDocument currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (currentDatabase != null && currentDatabase.getUser() != null)
      // SET CURRENT DATABASE NAME
      req.setUserRID(currentDatabase.getUser().getIdentity().getIdentity());

    final OHazelcastDistributedDatabase db = messageService.getDatabase(iDatabaseName);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "No nodes configured for partition '%s.%s' request: %s", iDatabaseName, iClusterNames, req);
      throw new ODistributedException(
          "No nodes configured for partition '" + iDatabaseName + "." + iClusterNames + "' request: " + req);
    }

    if (db == null) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT, "Distributed database '%s' not found",
          iDatabaseName);
      throw new ODistributedException(
          "Distributed database '" + iDatabaseName + "' not found on server '" + getLocalNodeName() + "'");
    }

    final ODistributedResponse response = db.send2Nodes(req, iClusterNames, iTargetNodes, iExecutionMode);
    if (response != null)
      return response.getPayload();

    return null;
  }

  public Set<String> getManagedDatabases() {
    return messageService != null ? messageService.getDatabases() : Collections.EMPTY_SET;
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public String getLocalNodeId() {
    return nodeId;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());
    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return;
    } else if (dbUrl.startsWith("remote:"))
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {

      if (getConfigurationMap().containsKey(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabase.getName()))
        throw new ODistributedException("Cannot create a new database with the same name of one available distributed");

      final OHazelcastDistributedDatabase distribDatabase = messageService.registerDatabase(iDatabase.getName());
      distribDatabase.configureDatabase(false, false, null).setOnline();
      onOpen(iDatabase);

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return;
    } else if (dbUrl.startsWith("remote:"))
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      synchronized (cachedDatabaseConfiguration) {
        final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
        if (cfg == null)
          return;

        if (iDatabase instanceof ODatabase<?> && (!(iDatabase.getStorage() instanceof ODistributedStorage)
            || ((ODistributedStorage) iDatabase.getStorage()).getDistributedManager().isOffline())) {
          ODistributedStorage storage = storages.get(iDatabase.getURL());
          if (storage == null) {
            storage = new ODistributedStorage(serverInstance, (OAbstractPaginatedStorage) iDatabase.getStorage());
            final ODistributedStorage oldStorage = storages.putIfAbsent(iDatabase.getURL(), storage);
            if (oldStorage != null)
              storage = oldStorage;
          }

          iDatabase.replaceStorage(storage);

          installDbClustersLocalStrategy(iDatabase);
        }

      }
    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
    if (OScenarioThreadLocal.INSTANCE.get() == OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
      return;

    // RUN ONLY IN NON-DISTRIBUTED MODE
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return;
    } else if (dbUrl.startsWith("remote:"))
      return;

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    installClustersOfClass(iDatabase, iClass);
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    super.onDrop(iDatabase);
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

    final Member member = iEvent.getMember();
    final String nodeName = getNodeName(member);
    if (nodeName != null) {
      // NOTIFY NODE LEFT
      for (ODistributedLifecycleListener l : listeners)
        l.onNodeLeft(nodeName);

      // UNLOCK ANY PENDING LOCKS
      if (messageService != null)
        for (String dbName : messageService.getDatabases())
          messageService.getDatabase(dbName).unlockRecords(nodeName);

      // UNREGISTER DB STATUSES
      for (Iterator<String> it = getConfigurationMap().keySet().iterator(); it.hasNext();) {
        final String n = it.next();

        if (n.startsWith(CONFIG_DBSTATUS_PREFIX))
          if (n.substring(CONFIG_DBSTATUS_PREFIX.length()).equals(nodeName)) {
            ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
                "removing dbstatus for the node %s that just left: %s", nodeName, n);
            it.remove();
          }
      }

      activeNodes.remove(nodeName);

      // REMOVE NODE IN DB CFG
      if (messageService != null)
        messageService.handleUnreachableNode(nodeName);

      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "node removed id=%s name=%s", member, nodeName);

      if (nodeName.startsWith("ext:")) {
        final List<String> registeredNodes = getRegisteredNodes();

        ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
            "removed node id=%s name=%s has not being recognized. Remove the node manually (registeredNodes=%s)", member, nodeName,
            registeredNodes);
      }
    }

    serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());
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

        // NOTIFY NODE IS GOING TO BE ADDED. EVERYBODY IS OK?
        for (ODistributedLifecycleListener l : listeners) {
          if (!l.onNodeJoining(nodeName)) {
            // DENY JOIN
            ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
                "denied node to join the cluster id=%s name=%s", iEvent.getMember(), getNodeName(iEvent.getMember()));
            return;
          }
        }

        activeNodes.put(nodeName, (Member) iEvent.getMember());

        ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
            "added node configuration id=%s name=%s, now %d nodes are configured", iEvent.getMember(),
            getNodeName(iEvent.getMember()), activeNodes.size());

        installNewDatabases(false);
      }

      // NOTIFY NODE WAS ADDED SUCCESSFULLY
      for (ODistributedLifecycleListener l : listeners)
        l.onNodeJoined(nodeName);

    } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
      // SYNCHRONIZE ADDING OF CLUSTERS TO AVOID DEADLOCKS
      final String databaseName = key.substring(CONFIG_DATABASE_PREFIX.length());

      checkDatabaseEvent(iEvent, databaseName);

      if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember()))
        installNewDatabases(false);
    } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
      ODistributedServerLog.info(this, getLocalNodeName(), getNodeName(iEvent.getMember()), DIRECTION.IN,
          "received new status %s=%s", key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());
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
        final String databaseName = key.substring(CONFIG_DATABASE_PREFIX.length());

        ODistributedServerLog.info(this, getLocalNodeName(), eventNodeName, DIRECTION.NONE, "update configuration db=%s",
            databaseName);

        checkDatabaseEvent(iEvent, databaseName);
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
  public void mapEvicted(MapEvent event) {

  }

  @Override
  public void mapCleared(MapEvent event) {

  }

  @Override
  public boolean isNodeAvailable(final String iNodeName, final String iDatabaseName) {
    return getDatabaseStatus(iNodeName, iDatabaseName) != DB_STATUS.OFFLINE;
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

      final Serializable result = (Serializable) task.execute(serverInstance, this, database);

      if (result instanceof Throwable && !(result instanceof OException))
        ODistributedServerLog.error(this, getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
            "error on executing request %d (%s) on local node: ", (Throwable) result, req.getId(), req.getTask());

      return result;

    } catch (Throwable e) {
      if (!(e instanceof OException))
        ODistributedServerLog.error(this, getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
            "error on executing distributed request %d on local node: %s", e, req.getId(), req.getTask());

      return e;
    }
  }

  @Override
  public OHazelcastDistributedMessageService getMessageService() {
    return messageService;
  }

  public void updateCachedDatabaseConfiguration(String iDatabaseName, ODocument cfg, boolean iSaveToDisk,
      boolean iDeployToCluster) {
    final boolean updated = super.updateCachedDatabaseConfiguration(iDatabaseName, cfg, iSaveToDisk);

    if (updated && iDeployToCluster) {
      // DEPLOY THE CONFIGURATION TO THE CLUSTER
      ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
      getConfigurationMap().put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabaseName, cfg);
    }
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n"))
      iText = iText.substring(2);
    else if (iText.startsWith("\n"))
      iText = iText.substring(1);

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

  public boolean installDatabase(boolean iStartup, final String databaseName, final ODocument config) {

    final Boolean hotAlignment = config.field("hotAlignment");
    final boolean backupDatabase = iStartup && hotAlignment != null && !hotAlignment;

    final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();
    if (configuredDatabases.contains(databaseName)) {
      if (!backupDatabase)
        // HOT ALIGNMENT RUNNING, DON'T INSTALL THE DB FROM SCRATCH BUT RATHER LET TO THE NODE TO ALIGN BY READING THE QUEUE
        return false;
    }
    final OHazelcastDistributedDatabase distrDatabase = messageService.registerDatabase(databaseName);

    // try {
    // Thread.sleep(2000 * activeNodes.size());
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }

    // CREATE THE DISTRIBUTED QUEUE
    String queueName = OHazelcastDistributedMessageService.getRequestQueueName(messageService.manager.getLocalNodeName(),
        databaseName);
    messageService.getQueue(queueName);

    queueName = OHazelcastDistributedMessageService.getRequestQueueName(messageService.manager.getLocalNodeName(),
        databaseName + OCreateRecordTask.SUFFIX_QUEUE_NAME);
    messageService.getQueue(queueName);

    for (int retry = 0; retry < DEPLOY_DB_MAX_RETRIES; ++retry) {
      // ASK DATABASE TO THE FIRST NODE, THE FIRST ATTEMPT, OTHERWISE ASK TO EVERYONE
      if (requestDatabase(distrDatabase, backupDatabase, databaseName, retry > 0))
        // DEPLOYED
        return true;
    }

    // RETRY COUNTER EXCEED
    return false;
  }

  protected boolean requestDatabase(final OHazelcastDistributedDatabase distrDatabase, final boolean backupDatabase,
      final String databaseName, final boolean iAskToAllNodes) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, getLocalNodeName());

    final List<String> selectedNodes = new ArrayList<String>();

    if (!iAskToAllNodes) {
      // GET THE FIRST ONE TO ASK FOR DATABASE. THIS FORCES TO HAVE ONE NODE TO DO BACKUP SAVING RESOURCES IN CASE BACKUP IS STILL
      // VALID FOR FURTHER NODES
      final Iterator<String> it = nodes.iterator();
      while (it.hasNext()) {
        final String f = it.next();
        if (isNodeAvailable(f, databaseName)) {
          selectedNodes.add(f);
          break;
        }
      }
    }

    if (selectedNodes.isEmpty())
      // NO NODE ONLINE, SEND THE MESSAGE TO EVERYONE
      selectedNodes.addAll(nodes);

    ODistributedServerLog.warn(this, getLocalNodeName(), selectedNodes.toString(), DIRECTION.OUT,
        "requesting deploy of database '%s' on local server...", databaseName);

    final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, selectedNodes,
        new OSyncDatabaseTask(OSyncDatabaseTask.MODE.FULL_REPLACE), EXECUTION_MODE.RESPONSE);

    ODistributedServerLog.debug(this, getLocalNodeName(), selectedNodes.toString(), DIRECTION.OUT, "deploy returned: %s", results);

    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    // EXTRACT THE REAL RESULT
    for (Entry<String, Object> r : results.entrySet()) {
      final Object value = r.getValue();

      if (value instanceof Boolean)
        continue;
      else if (value instanceof ODiscardedResponse) {
        // RETRY WITH NEXT NODE, IF ANY
        ODistributedServerLog.warn(this, getLocalNodeName(), selectedNodes.toString(), DIRECTION.OUT,
            "requesting deploy of database '%s' on local server failed, retrying...", databaseName);
        return false;
      } else if (value instanceof Throwable) {
        ODistributedServerLog.error(this, getLocalNodeName(), r.getKey(), DIRECTION.IN, "error on installing database %s in %s",
            (Exception) value, databaseName, dbPath);
      } else if (value instanceof ODistributedDatabaseChunk) {
        if (backupDatabase)
          backupCurrentDatabase(databaseName);

        final Set<String> toSyncClusters = installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, r.getKey(),
            (ODistributedDatabaseChunk) value);

        // SYNC ALL THE CLUSTERS
        for (String cl : toSyncClusters) {
          // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
          OCommandExecutorSQLSyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
        }

        return true;

      } else
        throw new IllegalArgumentException("Type " + value + " not supported");
    }

    throw new ODistributedException("No response received from remote nodes for auto-deploy of database");
  }

  protected void backupCurrentDatabase(final String iDatabaseName) {
    Orient.instance().unregisterStorageByName(iDatabaseName);

    final String backupDirectory = OGlobalConfiguration.DISTRIBUTED_BACKUP_DIRECTORY.getValueAsString();
    if (backupDirectory == null)
      // SKIP BACKUP
      return;

    // MOVE DIRECTORY TO ../backup/databases/<db-name>
    final File backupFullPath = new File(serverInstance.getDatabaseDirectory() + backupDirectory + "/" + iDatabaseName);
    final File backupParentPath = new File(serverInstance.getDatabaseDirectory() + backupDirectory);

    if (!backupParentPath.exists())
      // CREATE THE DIRECTORY STRUCTURE
      backupParentPath.mkdirs();
    else if (backupFullPath.exists()) {
      // DELETE PREVIOUS BACKUP
      OFileUtils.deleteRecursively(backupFullPath);
    }

    final String dbPath = serverInstance.getDatabaseDirectory() + iDatabaseName;

    // MOVE THE DATABASE ON CURRENT NODE
    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
        "moving existent database '%s' located in '%s' to '%s' and get a fresh copy from a remote node...", iDatabaseName, dbPath,
        backupFullPath);

    final File oldDirectory = new File(dbPath);
    if (!oldDirectory.renameTo(backupFullPath)) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
          "error on moving existent database '%s' located in '%s' to '%s'. Try to move the database directory manually and retry",
          iDatabaseName, dbPath, backupFullPath);

      throw new ODistributedException("Error on moving existent database '" + iDatabaseName + "' located in '" + dbPath + "' to '"
          + backupFullPath + "'. Try to move the database directory manually and retry");
    }
  }

  /**
   * Returns the clusters where sync is required.
   */
  protected Set<String> installDatabaseFromNetwork(final String dbPath, final String databaseName,
      final OHazelcastDistributedDatabase distrDatabase, final String iNode, final ODistributedDatabaseChunk value) {
    // DISCARD ALL THE MESSAGES BEFORE THE BACKUP
    distrDatabase.setWaitForMessage(value.getLastOperationId());

    final String fileName = Orient.getTempPath() + "install_" + databaseName + ".zip";

    final String localNodeName = getLocalNodeName();

    ODistributedServerLog.info(this, localNodeName, iNode, DIRECTION.IN, "copying remote database '%s' to: %s", databaseName,
        fileName);

    final File file = new File(fileName);
    if (file.exists())
      file.delete();

    try {
      file.getParentFile().mkdirs();
      file.createNewFile();
    } catch (IOException e) {
      throw new ODistributedException("Error on creating temp database file to install locally", e);
    }

    // DELETE ANY PREVIOUS .COMPLETED FILE
    final File completedFile = new File(file.getAbsolutePath() + ".completed");
    if (completedFile.exists())
      completedFile.delete();

    try {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.currentThread().setName("OrientDB installDatabase node=" + nodeName + " db=" + databaseName);
            ODistributedDatabaseChunk chunk = value;

            final FileOutputStream fOut = new FileOutputStream(fileName, false);
            try {

              long fileSize = writeDatabaseChunk(1, chunk, fOut);
              for (int chunkNum = 2; !chunk.last; chunkNum++) {
                final Object result = sendRequest(databaseName, null, Collections.singleton(iNode),
                    new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length),
                    EXECUTION_MODE.RESPONSE);

                if (result instanceof Boolean)
                  continue;
                else if (result instanceof Exception) {
                  ODistributedServerLog.error(this, getLocalNodeName(), iNode, DIRECTION.IN,
                      "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
                } else if (result instanceof ODistributedDatabaseChunk) {
                  chunk = (ODistributedDatabaseChunk) result;
                  fileSize += writeDatabaseChunk(chunkNum, chunk, fOut);
                }
              }

              fOut.flush();

              // CREATE THE .COMPLETED FILE TO SIGNAL EOF
              new File(file.getAbsolutePath() + ".completed").createNewFile();

              ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "database copied correctly, size=%s",
                  OFileUtils.getSizeAsString(fileSize));
            } finally {
              try {
                fOut.flush();
                fOut.close();
              } catch (IOException e) {
              }
            }

          } catch (Exception e) {
            ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
                "error on transferring database '%s' to '%s'", e, databaseName, fileName);
            throw new ODistributedException("Error on transferring database", e);
          }
        }
      }).start();

    } catch (Exception e) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE, "error on transferring database '%s' to '%s'", e,
          databaseName, fileName);
      throw new ODistributedException("Error on transferring database", e);
    }

    final ODatabaseDocumentTx db = installDatabaseOnLocalNode(distrDatabase, databaseName, dbPath, iNode, fileName);

    if (db != null) {
      db.close();
      final OStorage stg = Orient.instance().getStorage(databaseName);
      if (stg != null)
        stg.close();

      final Lock lock = getLock("orientdb." + databaseName + ".cfg");
      lock.lock();
      try {
        // GET LAST VERSION IN LOCK
        final ODistributedConfiguration cfg = getDatabaseConfiguration(db.getName());

        distrDatabase.configureDatabase(false, true, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            final boolean distribCfgDirty = installDbClustersForLocalNode(db, cfg);
            if (distribCfgDirty) {
              OLogManager.instance().warn(this, "Distributed configuration modified");
              updateCachedDatabaseConfiguration(db.getName(), cfg.serialize(), true, true);
            }
            return null;
          }
        });
      } finally {
        lock.unlock();
      }

      db.activateOnCurrentThread();
      db.close();
    }

    final ODistributedConfiguration cfg = getDatabaseConfiguration(db.getName());

    // ASK FOR INDIVIDUAL CLUSTERS IN CASE OF SHARDING AND NO LOCAL COPY
    final Set<String> localManagedClusters = cfg.getClustersOnServer(localNodeName);
    final Set<String> sourceNodeClusters = cfg.getClustersOnServer(iNode);
    localManagedClusters.removeAll(sourceNodeClusters);

    final HashSet<String> toSynchClusters = new HashSet<String>();
    for (String cl : localManagedClusters) {
      // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
      if (!cfg.getServers(cl, localNodeName).isEmpty())
        toSynchClusters.add(cl);
    }

    return toSynchClusters;
  }

  /**
   * Guarantees, foreach class, that has own master cluster.
   */
  @Override
  public void propagateSchemaChanges(final ODatabaseInternal iDatabase) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    for (OClass c : iDatabase.getMetadata().getSchema().getClasses()) {
      if (!(c.getClusterSelection() instanceof OLocalClusterStrategy))
        // INSTALL ONLY ON NON-ENHANCED CLASSES
        installClustersOfClass(iDatabase, c);
    }
  }

  /**
   * Guarantees that each class has own master cluster.
   */
  public synchronized void installClustersOfClass(final ODatabaseInternal iDatabase, final OClass iClass) {

    final String databaseName = iDatabase.getName();

    if (!(iClass.getClusterSelection() instanceof OLocalClusterStrategy))
      // INJECT LOCAL CLUSTER STRATEGY
      ((OClassImpl) iClass).setClusterSelectionInternal(new OLocalClusterStrategy(this, databaseName, iClass));

    if (iClass.isAbstract())
      return;

    final int[] clusterIds = iClass.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int clusterId : clusterIds)
      clusterNames.add(iDatabase.getClusterNameById(clusterId));

    boolean distributedCfgDirty = false;

    final Lock lock = getLock("orientdb." + databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());

      // CHECK IF EACH NODE HAS IS MASTER OF ONE CLUSTER
      final Set<String> servers = cfg.getServers(null);
      for (String server : servers) {
        final String bestCluster = cfg.getLocalCluster(clusterNames, server);
        if (bestCluster == null) {
          // TRY TO FIND A CLUSTER PREVIOUSLY ASSIGNED TO THE LOCAL NODE
          final String newClusterName = (iClass.getName() + "_" + server).toLowerCase();

          final Set<String> cfgClusterNames = new HashSet<String>();
          for (String cl : cfg.getClusterNames())
            cfgClusterNames.add(cl);

          if (cfgClusterNames.contains(newClusterName)) {
            // FOUND A CLUSTER PREVIOUSLY ASSIGNED TO THE LOCAL ONE: CHANGE ASSIGNMENT TO LOCAL NODE AGAIN
            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE,
                "class %s, change mastership of cluster '%s' (id=%d) to node '%s'", iClass, newClusterName,
                iDatabase.getClusterIdByName(newClusterName), server);
            cfg.setMasterServer(newClusterName, server);
            distributedCfgDirty = true;
          } else {

            // CREATE A NEW CLUSTER WHERE CURRENT NODE IS THE MASTER
            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "class %s, creation of new cluster '%s' (id=%d)",
                iClass, newClusterName, iDatabase.getClusterIdByName(newClusterName));

            final OScenarioThreadLocal.RUN_MODE currentDistributedMode = OScenarioThreadLocal.INSTANCE.get();
            if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
              OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);

            try {
              iClass.addCluster(newClusterName);
            } catch (OCommandSQLParsingException e) {
              if (!e.getMessage().endsWith("already exists"))
                throw e;
            } catch (Exception e) {
              ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "error on creating cluster '%s' in class '%s'",
                  newClusterName, iClass);
              throw new ODistributedException("Error on creating cluster '" + newClusterName + "' in class '" + iClass + "'", e);
            } finally {

              if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
                // RESTORE PREVIOUS MODE
                OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);
            }

            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE,
                "class '%s', set mastership of cluster '%s' (id=%d) to '%s'", iClass, newClusterName,
                iDatabase.getClusterIdByName(newClusterName), server);
            cfg.setMasterServer(newClusterName, server);
            distributedCfgDirty = true;
          }
        }
      }

      if (distributedCfgDirty)
        updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);

    } finally {
      lock.unlock();
    }
  }

  protected void checkDatabaseEvent(final EntryEvent<String, Object> iEvent, final String databaseName) {
    updateLastClusterChange();

    installNewDatabases(false);
    updateCachedDatabaseConfiguration(databaseName, (ODocument) iEvent.getValue(), true, false);
    serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());

    updateLastClusterChange();
  }

  protected boolean installDbClustersForLocalNode(final ODatabaseInternal iDatabase, final ODistributedConfiguration cfg) {
    final String nodeName = getLocalNodeName();
    final ODistributedConfiguration.ROLES role = cfg.getServerRole(nodeName);
    if (role != ODistributedConfiguration.ROLES.MASTER)
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return false;

    if (iDatabase.isClosed())
      getServerInstance().openDatabase(iDatabase);

    // OVERWRITE CLUSTER SELECTION STRATEGY
    final OSchema schema = ((ODatabaseInternal<?>) iDatabase).getDatabaseOwner().getMetadata().getSchema();

    boolean distribCfgDirty = false;
    for (final OClass c : schema.getClasses())
      if (installLocalClusterPerClass(iDatabase, cfg, c))
        distribCfgDirty = true;

    return distribCfgDirty;
  }

  protected void installDbClustersLocalStrategy(final ODatabaseInternal iDatabase) {
    if (iDatabase.isClosed())
      getServerInstance().openDatabase(iDatabase);

    // OVERWRITE CLUSTER SELECTION STRATEGY
    final OSchema schema = ((ODatabaseInternal<?>) iDatabase).getDatabaseOwner().getMetadata().getSchema();

    for (OClass c : schema.getClasses()) {
      ((OClassImpl) c).setClusterSelectionInternal(new OLocalClusterStrategy(this, iDatabase.getName(), c));
    }
  }

  protected void assignNodeName() {
    // ORIENTDB_NODE_NAME ENV VARIABLE OR JVM SETTING
    nodeName = OSystemVariableResolver.resolveVariable(NODE_NAME_ENV);

    if (nodeName != null) {
      nodeName = nodeName.trim();
      if (nodeName.isEmpty())
        nodeName = null;
    }

    if (nodeName == null) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println("+---------------------------------------------------------------+");
      System.out.println("|         WARNING: FIRST DISTRIBUTED RUN CONFIGURATION          |");
      System.out.println("+---------------------------------------------------------------+");
      System.out.println("| This is the first time that the server is running as          |");
      System.out.println("| distributed. Please type the name you want to assign to the   |");
      System.out.println("| current server node.                                          |");
      System.out.println("|                                                               |");
      System.out.println("| To avoid this message set the environment variable or JVM     |");
      System.out.println("| setting ORIENTDB_NODE_NAME to the server node name to use.    |");
      System.out.println("+---------------------------------------------------------------+");
      System.out.print("\nNode name [BLANK=auto generate it]: ");

      OConsoleReader reader = new DefaultConsoleReader();
      try {
        nodeName = reader.readLine();
      } catch (IOException e) {
      }
      if (nodeName != null) {
        nodeName = nodeName.trim();
        if (nodeName.isEmpty())
          nodeName = null;
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
          h.parameters[h.parameters.length - 1] = new OServerParameterConfiguration("nodeName", this.nodeName);
        }

        try {
          serverInstance.saveConfiguration();
        } catch (IOException e) {
          throw new OConfigurationException("Cannot save server configuration", e);
        }
        break;
      }
    }
  }

  protected HazelcastInstance configureHazelcast() throws FileNotFoundException {
    FileSystemXmlConfig config = new FileSystemXmlConfig(hazelcastConfigFile);
    config.setClassLoader(this.getClass().getClassLoader());
    return Hazelcast.newHazelcastInstance(config);
  }

  /**
   * Initializes all the available server's databases as distributed.
   */
  protected void loadDistributedDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "opening database '%s'...", databaseName);

        ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

        final boolean hotAlignment = cfg.isHotAlignment();

        // if (!hotAlignment) {
        // // REMOVE ALL NODES NOT ACTIVE
        // final Set<String> nodeToRemoveInCfgBecauseNotActive = cfg.getAllConfiguredServers();
        // nodeToRemoveInCfgBecauseNotActive.remove(getLocalNodeName());
        // nodeToRemoveInCfgBecauseNotActive.removeAll(getAvailableNodeNames(databaseName));
        // for (String s : nodeToRemoveInCfgBecauseNotActive)
        // cfg.removeNodeInServerList(s, true);
        // }

        final OServerUserConfiguration replicatorUser = serverInstance.getUser(ODistributedAbstractPlugin.REPLICATOR_USER);

        if (!getConfigurationMap().containsKey(CONFIG_DATABASE_PREFIX + databaseName)) {
          // PUBLISH CFG FIRST TIME
          ODocument cfgDoc = cfg.serialize();
          ORecordInternal.setRecordSerializer(cfgDoc, ODatabaseDocumentTx.getDefaultSerializer());
          getConfigurationMap().put(CONFIG_DATABASE_PREFIX + databaseName, cfgDoc);
        }

        final OHazelcastDistributedDatabase db = messageService.registerDatabase(databaseName).configureDatabase(hotAlignment,
            hotAlignment, null);

        final ODatabaseDocumentTx database = (ODatabaseDocumentTx) serverInstance.openDatabase("document", databaseName,
            replicatorUser.name, replicatorUser.password, null);
        try {
          // ASSIGN CLUSTERS AT STARTUP
          cfg = getDatabaseConfiguration(databaseName);
          final boolean distribCfgDirty = installDbClustersForLocalNode(database, cfg);
          if (distribCfgDirty) {
            OLogManager.instance().info(this, "Distributed configuration modified");
            updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
          }
        } finally {
          database.close();
        }

      }
    }
  }

  protected void installNewDatabases(final boolean iStartup) {
    if (activeNodes.size() <= 1)
      // NO OTHER NODES WHERE ALIGN
      return;

    // LOCKING THIS RESOURCE PREVENT CONCURRENT INSTALL OF THE SAME DB
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

  protected long writeDatabaseChunk(final int iChunkId, final ODistributedDatabaseChunk chunk, final FileOutputStream out)
      throws IOException {

    ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId,
        chunk.offset, OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }

  protected ODatabaseDocumentTx installDatabaseOnLocalNode(final OHazelcastDistributedDatabase distrDatabase,
      final String databaseName, final String dbPath, final String iNode, final String iDatabaseCompressedFile) {
    ODistributedServerLog.warn(this, getLocalNodeName(), iNode, DIRECTION.IN, "installing database '%s' to: %s...", databaseName,
        dbPath);

    try {
      final File f = new File(iDatabaseCompressedFile);
      final File fCompleted = new File(iDatabaseCompressedFile + ".completed");

      new File(dbPath).mkdirs();
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath);

      // USES A CUSTOM WRAPPER OF IS TO WAIT FOR FILE IS WRITTEN (ASYNCH)
      final FileInputStream in = new FileInputStream(f) {
        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
          while (true) {
            final int read = super.read(b, off, len);
            if (read > 0)
              return read;

            if (fCompleted.exists())
              return 0;

            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }
      };

      try {
        db.restore(in, null, null, this);
      } finally {
        in.close();
      }
      db.close();

      // Orient.instance().unregisterStorageByName(db.getURL().substring(db.getStorage().getType().length() + 1));

      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "installed database '%s'", databaseName);

      return db;

    } catch (IOException e) {
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.IN, "error on copying database '%s' on local server", e,
          databaseName);
    }
    return null;
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
      if (sleep > 3000)
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

  private synchronized boolean installLocalClusterPerClass(final ODatabaseInternal iDatabase, final ODistributedConfiguration cfg,
      final OClass iClass) {
    ((OClassImpl) iClass).setClusterSelectionInternal(new OLocalClusterStrategy(this, iDatabase.getName(), iClass));
    if (iClass.isAbstract())
      return false;

    final int[] clusterIds = iClass.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int clusterId : clusterIds)
      clusterNames.add(iDatabase.getClusterNameById(clusterId));

    String bestCluster = cfg.getLocalCluster(clusterNames, nodeName);
    if (bestCluster == null) {
      // TRY TO FIND A CLUSTER PREVIOUSLY ASSIGNED TO THE LOCAL NODE
      final String newClusterName = (iClass.getName() + "_" + getLocalNodeName()).toLowerCase();

      final Set<String> cfgClusterNames = new HashSet<String>();
      for (String cl : cfg.getClusterNames())
        cfgClusterNames.add(cl.toLowerCase());

      if (cfgClusterNames.contains(newClusterName)) {
        // FOUND A CLUSTER PREVIOUSLY ASSIGNED TO THE LOCAL ONE: CHANGE ASSIGNMENT TO LOCAL NODE AGAIN
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE,
            "class '%s', change mastership of cluster '%s' (id=%d) to local node '%s'", iClass, newClusterName,
            iDatabase.getClusterIdByName(newClusterName), nodeName);
        cfg.setMasterServer(newClusterName, nodeName);
      } else {

        // CREATE A NEW CLUSTER WHERE LOCAL NODE IS THE MASTER
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "class '%s', creation of new local cluster '%s' (id=%d)",
            iClass, newClusterName, iDatabase.getClusterIdByName(newClusterName));

        final OScenarioThreadLocal.RUN_MODE currentDistributedMode = OScenarioThreadLocal.INSTANCE.get();
        if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
          OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);

        try {
          iClass.addCluster(newClusterName);
        } catch (OCommandSQLParsingException e) {
          if (!e.getMessage().endsWith("already exists"))
            throw e;
        } catch (Exception e) {
          ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "error on creating cluster '%s' in class '%s': ",
              newClusterName, iClass, e);
          throw new ODistributedException("Error on creating cluster '" + newClusterName + "' in class '" + iClass + "'", e);
        } finally {

          if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
            // RESTORE PREVIOUS MODE
            OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);
        }

        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE,
            "class '%s', set mastership of cluster '%s' (id=%d) to '%s'", iClass, newClusterName,
            iDatabase.getClusterIdByName(newClusterName), nodeName);
        cfg.setMasterServer(newClusterName, nodeName);
      }

      return true;
    }

    return false;
  }

  @Override
  public OHazelcastPlugin registerLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.add(iListener);
    return this;
  }

  @Override
  public OHazelcastPlugin unregisterLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.remove(iListener);
    return this;
  }

  public void unjoinNode(final String iNode) {
    final Set<String> databases = new HashSet<String>();

    for (Map.Entry<String, Object> entry : getConfigurationMap().entrySet()) {
      if (entry.getKey().toString().startsWith(CONFIG_DBSTATUS_PREFIX)) {

        final String nodeDb = entry.getKey().toString().substring(CONFIG_DBSTATUS_PREFIX.length());

        if (nodeDb.startsWith(iNode))
          databases.add(entry.getKey());
      }
    }

    // PUT DATABASES OFFLINE
    for (String k : databases)
      getConfigurationMap().put(k, DB_STATUS.OFFLINE);

    // GET THE SENDER'S RESPONSE QUEUE
    final IQueue<ODistributedResponse> queue = messageService
        .getQueue(OHazelcastDistributedMessageService.getResponseQueueName(iNode));

    final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(-1, nodeName, iNode, new ORestartNodeTask());

    try {
      if (!queue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
        throw new ODistributedException("Timeout on dispatching restart node request to node '" + iNode + "'");
    } catch (InterruptedException e) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "Interrupted request to restart node '%s'", iNode);
    }

  }

  private List<String> getRegisteredNodes() {
    final List<String> registeredNodes = new ArrayList<String>();

    for (Map.Entry entry : getConfigurationMap().entrySet()) {
      if (entry.getKey().toString().startsWith(CONFIG_NODE_PREFIX))
        registeredNodes.add(entry.getKey().toString().substring(CONFIG_NODE_PREFIX.length()));
    }

    return registeredNodes;
  }

  public Set<String> getAvailableNodeNames(final String iDatabaseName) {
    final Set<String> nodes = new HashSet<String>();

    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        nodes.add(entry.getKey());
    }
    return nodes;
  }
}
