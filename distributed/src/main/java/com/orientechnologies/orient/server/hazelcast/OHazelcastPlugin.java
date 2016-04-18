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
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.sql.OCommandExecutorSQLSyncCluster;
import com.orientechnologies.orient.server.distributed.task.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation for clustering.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin
    implements MembershipListener, EntryListener<String, Object>, OCommandOutputListener {

  public static final String                                     CONFIG_DATABASE_PREFIX            = "database.";

  protected static final String                                  NODE_NAME_ENV                     = "ORIENTDB_NODE_NAME";
  protected static final String                                  CONFIG_NODE_PREFIX                = "node.";
  protected static final String                                  CONFIG_DBSTATUS_PREFIX            = "dbstatus.";
  protected static final String                                  CONFIG_REGISTEREDNODES            = "registeredNodes";
  protected static final int                                     DEPLOY_DB_MAX_RETRIES             = 10;
  public static final String                                     REPLICATOR_USER                   = "_CrossServerTempUser";

  protected String                                               nodeUuid;
  protected String                                               hazelcastConfigFile               = "hazelcast.xml";
  protected Map<String, Member>                                  activeNodes                       = new ConcurrentHashMap<String, Member>();
  protected List<String>                                         registeredNodeById;
  protected Map<String, Integer>                                 registeredNodeByName;
  protected ODistributedMessageServiceImpl                       messageService;
  protected Date                                                 startedOn                         = new Date();

  protected volatile NODE_STATUS                                 status                            = NODE_STATUS.OFFLINE;

  protected String                                               membershipListenerRegistration;

  protected volatile HazelcastInstance                           hazelcastInstance;
  protected long                                                 lastClusterChangeOn;
  protected List<ODistributedLifecycleListener>                  listeners                         = new ArrayList<ODistributedLifecycleListener>();
  protected final ConcurrentMap<String, ORemoteServerController> remoteServers                     = new ConcurrentHashMap<String, ORemoteServerController>();
  protected TimerTask                                            publishLocalNodeConfigurationTask = null;

  // LOCAL MSG COUNTER
  protected AtomicLong                                           localMessageIdCounter             = new AtomicLong();

  protected OClusterOwnershipAssignmentStrategy                  clusterAssignmentStrategy         = new ODefaultClusterOwnershipAssignmentStrategy(
      this);

  protected Map<String, Long>                                    lastLSNWriting                    = new HashMap<String, Long>();

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
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    serverInstance.addTemporaryUser(REPLICATOR_USER, "" + new SecureRandom().nextLong(), "*");

    super.startup();

    status = NODE_STATUS.STARTING;

    final String localNodeName = nodeName;

    activeNodes.clear();

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    registeredNodeById = null;
    registeredNodeByName = null;

    try {
      hazelcastInstance = configureHazelcast();

      nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

      OLogManager.instance().info(this, "Starting distributed server '%s' (hzID=%s)...", localNodeName, nodeId);

      activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());

      membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

      OServer.registerServerInstance(localNodeName, serverInstance);

      final IMap<String, Object> configurationMap = (IMap<String, Object>) getConfigurationMap();
      configurationMap.addEntryListener(this, true);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (!m.getUuid().equals(getLocalNodeId())) {
          final String memberName = getNodeName(m);
          if (memberName != null) {
            activeNodes.put(memberName, m);
          } else if (!m.equals(hazelcastInstance.getCluster().getLocalMember()))
            ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.NONE, "Cannot find configuration for member: %s", m);
        }
      }

      final Lock lock = getLock("registeredNodes");
      lock.lock();
      try {
        final ODocument registeredNodesFromCluster = new ODocument();
        final String registeredNodesFromClusterAsJson = (String) getConfigurationMap().get(CONFIG_REGISTEREDNODES);

        if (registeredNodesFromClusterAsJson != null) {
          registeredNodesFromCluster.fromJSON(registeredNodesFromClusterAsJson);
          registeredNodeById = registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST);
          registeredNodeByName = registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP);

          if (registeredNodeByName.containsKey(nodeName)) {
            nodeId = registeredNodeByName.get(nodeName);
          } else {
            // ADD CURRENT NODE
            registeredNodeById.add(nodeName);
            nodeId = registeredNodeById.size() - 1;
            registeredNodeByName.put(nodeName, nodeId);
          }
        } else {
          // FIRST TIME: CREATE NEW CFG
          nodeId = 0;

          registeredNodeById = new ArrayList<String>();
          registeredNodeById.add(nodeName);
          registeredNodesFromCluster.field("ids", registeredNodeById, OType.EMBEDDEDLIST);

          registeredNodeByName = new HashMap<String, Integer>();
          registeredNodeByName.put(nodeName, nodeId);
          registeredNodesFromCluster.field("names", registeredNodeByName, OType.EMBEDDEDMAP);
        }

        // SAVE NEW CFG
        getConfigurationMap().put(CONFIG_REGISTEREDNODES, registeredNodesFromCluster.toJSON());

      } finally {
        lock.unlock();
      }

      publishLocalNodeConfiguration();

      if (!configurationMap.containsKey(CONFIG_NODE_PREFIX + nodeUuid)) {
        // NODE NOT REGISTERED, FORCING SHUTTING DOWN
        ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on registering local node on cluster");
        throw new ODistributedStartupException("Error on registering local node on cluster");
      }

      // CONNECTS TO ALL THE AVAILABLE NODES
      for (String m : activeNodes.keySet())
        if (!m.equals(nodeName))
          getRemoteServer(m);

      messageService = new ODistributedMessageServiceImpl(this);

      installNewDatabasesFromCluster(true);

      loadLocalDatabases();

      // REGISTER CURRENT MEMBERS
      setNodeStatus(NODE_STATUS.ONLINE);

      publishLocalNodeConfiguration();

      final long delay = OGlobalConfiguration.DISTRIBUTED_PUBLISH_NODE_STATUS_EVERY.getValueAsLong();
      if (delay > 0) {
        publishLocalNodeConfigurationTask = new TimerTask() {
          @Override
          public void run() {
            try {
              publishLocalNodeConfiguration();
            } catch (Throwable e) {
              OLogManager.instance().debug(this, "Error on distributed configuration node updater", e);
            }
          }
        };
        Orient.instance().scheduleTask(publishLocalNodeConfigurationTask, delay, delay);
      }

    } catch (Exception e) {
      ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on starting distributed plugin", e);
      throw OException.wrapException(new ODistributedStartupException("Error on starting distributed plugin"), e);
    }

  }

  protected void publishLocalNodeConfiguration() {
    final ODocument cfg = getLocalNodeConfiguration();
    ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
    getConfigurationMap().put(CONFIG_NODE_PREFIX + nodeUuid, cfg);
  }

  @Override
  public Throwable convertException(final Throwable original) {
    if (original instanceof HazelcastException || original instanceof HazelcastInstanceNotActiveException)
      return new IOException("Hazelcast wrapped exception: " + original.getMessage(), original.getCause());

    if (original instanceof IllegalMonitorStateException)
      // THIS IS RAISED WHEN INTERNAL LOCKING IS BROKEN BECAUSE HARD SHUTDOWN
      return new IOException("Illegal monitor state: " + original.getMessage(), original.getCause());

    return original;
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OLogManager.instance().warn(this, "Shutting down node %s...", nodeName);

    super.shutdown();

    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    try {
      final Set<String> databases = new HashSet<String>();

      for (Map.Entry<String, Object> entry : getConfigurationMap().entrySet()) {
        if (entry.getKey().toString().startsWith(CONFIG_DBSTATUS_PREFIX)) {

          final String nodeDb = entry.getKey().toString().substring(CONFIG_DBSTATUS_PREFIX.length());

          if (nodeDb.startsWith(nodeName))
            databases.add(entry.getKey());
        }
      }

      // PUT DATABASES OFFLINE
      for (String k : databases)
        getConfigurationMap().put(k, DB_STATUS.OFFLINE);
    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    if (publishLocalNodeConfigurationTask != null)
      publishLocalNodeConfigurationTask.cancel();

    if (messageService != null)
      messageService.shutdown();

    activeNodes.clear();

    if (membershipListenerRegistration != null) {
      try {
        hazelcastInstance.getCluster().removeMembershipListener(membershipListenerRegistration);
      } catch (HazelcastInstanceNotActiveException e) {
        // HZ IS ALREADY DOWN, IGNORE IT
      }
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

    cluster.field("localName", getName());
    cluster.field("localId", instance.getCluster().getLocalMember().getUuid());

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    // members.add(getLocalNodeConfiguration());
    for (Member member : activeNodes.values()) {
      members.add(getNodeConfigurationByUuid(member.getUuid()));
    }

    return cluster;
  }

  public ODocument getNodeConfigurationByUuid(final String iNodeId) {
    final ODocument doc = (ODocument) getConfigurationMap().get(CONFIG_NODE_PREFIX + iNodeId);
    if (doc == null)
      ODistributedServerLog.debug(this, nodeName, null, DIRECTION.OUT, "Cannot find node with id '%s'", iNodeId);

    return doc;
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();

    nodeCfg.field("id", nodeId);
    nodeCfg.field("uuid", nodeUuid);
    nodeCfg.field("name", nodeName);
    nodeCfg.field("startedOn", startedOn);
    nodeCfg.field("status", getNodeStatus());
    nodeCfg.field("connections", serverInstance.getClientConnectionManager().getTotal());

    List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : serverInstance.getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress(true));
    }

    // STORE THE TEMP USER/PASSWD USED FOR REPLICATION
    nodeCfg.field("user_replicator", serverInstance.getUser(REPLICATOR_USER).password);

    nodeCfg.field("databases", getManagedDatabases());

    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;

    nodeCfg.field("usedMemory", usedMem);
    nodeCfg.field("freeMemory", freeMem);
    nodeCfg.field("maxMemory", maxMem);

    onLocalNodeConfigurationRequest(nodeCfg);

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

    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Updated node status to '%s'", status);
  }

  public boolean checkNodeStatus(final NODE_STATUS iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  @Override
  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName) {
    final DB_STATUS status = (DB_STATUS) getConfigurationMap()
        .get(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
    return status != null ? status : DB_STATUS.OFFLINE;
  }

  @Override
  public void setDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    getConfigurationMap().put(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName, iStatus);

    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      l.onDatabaseChangeStatus(iNode, iDatabaseName, iStatus);
    }
  }

  @Override
  public ODistributedResponse sendRequest(final String iDatabaseName, final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes, final ORemoteTask iTask, final EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback) {

    final ODistributedRequest req = new ODistributedRequest(nodeId, getNextMessageIdCounter(), iDatabaseName, iTask,
        iExecutionMode);

    final ODatabaseDocument currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (currentDatabase != null && currentDatabase.getUser() != null)
      // SET CURRENT DATABASE NAME
      req.setUserRID(currentDatabase.getUser().getIdentity().getIdentity());

    final ODistributedDatabaseImpl db = messageService.getDatabase(iDatabaseName);

    if (iTargetNodes == null || iTargetNodes.isEmpty()) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.OUT, "No nodes configured for partition '%s.%s' request: %s",
          iDatabaseName, iClusterNames, req);
      throw new ODistributedException(
          "No nodes configured for partition '" + iDatabaseName + "." + iClusterNames + "' request: " + req);
    }

    if (db == null) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.OUT, "Distributed database '%s' not found", iDatabaseName);
      throw new ODistributedException("Distributed database '" + iDatabaseName + "' not found on server '" + nodeName + "'");
    }

    return db.send2Nodes(req, iClusterNames, iTargetNodes, iExecutionMode, localResult, iAfterSentCallback);
  }

  /**
   * Executes the request on local node. In case of error returns the Exception itself
   */
  @Override
  public Serializable executeOnLocalNode(final ODistributedRequestId reqId, final ORemoteTask task,
      final ODatabaseDocumentTx database) {
    if (database != null && !(database.getStorage() instanceof ODistributedStorage))
      throw new ODistributedException("Distributed storage was not installed for database '" + database.getName()
          + "'. Implementation found: " + database.getStorage().getClass().getName());

    final OHazelcastPlugin manager = this;

    return (Serializable) OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          final Serializable result = (Serializable) task.execute(reqId, serverInstance, manager, database);

          if (result instanceof Throwable && !(result instanceof OException))
            // EXCEPTION
            ODistributedServerLog.error(this, nodeName, getNodeNameById(reqId.getNodeId()), DIRECTION.IN,
                "Error on executing request %d (%s) on local node: ", (Throwable) result, reqId, task);
          else {
            // OK
            final String sourceNodeName = task.getNodeSource();
            Long last = lastLSNWriting.get(sourceNodeName);
            if (last == null)
              last = 0l;

            if (task instanceof OAbstractRecordReplicatedTask && System.currentTimeMillis() - last > 2000) {
              final ODistributedDatabaseImpl ddb = getMessageService().getDatabase(database.getName());
              final OLogSequenceNumber lastLSN = ((OAbstractRecordReplicatedTask) task).getLastLSN();
              if (lastLSN != null)
                ddb.getSyncConfiguration().setLSN(task.getNodeSource(), lastLSN);

              ODistributedServerLog.debug(this, nodeName, task.getNodeSource(), DIRECTION.NONE,
                  "Updating LSN table to the value %s", lastLSN);

              lastLSNWriting.put(sourceNodeName, System.currentTimeMillis());
            }
          }

          return result;

        } catch (Throwable e) {
          if (!(e instanceof OException))
            ODistributedServerLog.error(this, nodeName, getNodeNameById(reqId.getNodeId()), DIRECTION.IN,
                "error on executing distributed request %s on local node: %s", e, reqId, task);

          return e;
        }
      }
    });
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) throws IOException {
    ORemoteServerController remoteServer = remoteServers.get(rNodeName);
    if (remoteServer == null) {
      final Member member = activeNodes.get(rNodeName);
      if (member == null)
        throw new ODistributedException("Cannot find node '" + rNodeName + "'");

      final ODocument cfg = getNodeConfigurationByUuid(member.getUuid());

      final Collection<Map<String, Object>> listeners = (Collection<Map<String, Object>>) cfg.field("listeners");
      if (listeners == null)
        throw new ODatabaseException(
            "Cannot connect to a remote node because bad distributed configuration: missing 'listeners' array field");

      String url = null;
      for (Map<String, Object> listener : listeners) {
        if (((String) listener.get("protocol")).equals("ONetworkProtocolBinary")) {
          url = (String) listener.get("listen");
          break;
        }
      }

      if (url == null)
        throw new ODatabaseException("Cannot connect to a remote node because the url was not found");

      final String userPassword = cfg.field("user_replicator");

      remoteServer = new ORemoteServerController(this, rNodeName, url, REPLICATOR_USER, userPassword);
      final ORemoteServerController old = remoteServers.putIfAbsent(rNodeName, remoteServer);
      if (old != null) {
        remoteServer.close();
        remoteServer = old;
      }
    }
    return remoteServer;
  }

  public Set<String> getManagedDatabases() {
    return messageService != null ? messageService.getDatabases() : Collections.EMPTY_SET;
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public int getLocalNodeId() {
    return nodeId;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {

      if (getConfigurationMap().containsKey(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabase.getName()))
        throw new ODistributedException("Cannot create a new database with the same name of one available distributed");

      final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
      cfg.addNewNodeInServerList(nodeName);
      updateCachedDatabaseConfiguration(iDatabase.getName(), cfg.serialize(), true, true);

      final ODistributedDatabaseImpl distribDatabase = messageService.registerDatabase(iDatabase.getName());
      distribDatabase.setOnline();

      onOpen(iDatabase);

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  @Override
  public void onLocalNodeConfigurationRequest(final ODocument iConfiguration) {
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
      if (cfg == null)
        return;

      if (!(iDatabase.getStorage() instanceof ODistributedStorage)
          || ((ODistributedStorage) iDatabase.getStorage()).getDistributedManager().isOffline()) {

        ODistributedStorage storage = storages.get(iDatabase.getURL());
        if (storage == null) {
          storage = new ODistributedStorage(serverInstance, (OAbstractPaginatedStorage) iDatabase.getStorage().getUnderlying());
          final ODistributedStorage oldStorage = storages.putIfAbsent(iDatabase.getURL(), storage);
          if (oldStorage != null)
            storage = oldStorage;
        }

        iDatabase.replaceStorage(storage);

        if (isNodeOnline(nodeName, iDatabase.getName()))
          installDbClustersLocalStrategy(iDatabase);
      }
    } catch (HazelcastInstanceNotActiveException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

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
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    if (installClustersOfClass(iDatabase, iClass))
      updateCachedDatabaseConfiguration(iDatabase.getName(), cfg.serialize(), true, true);
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final String dbName = iDatabase.getName();

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Dropping database %s...", dbName);

    super.onDrop(iDatabase);

    getConfigurationMap().remove(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + nodeName + "." + dbName);

    final int availableNodes = getAvailableNodes(dbName);
    if (availableNodes == 0) {
      // LAST NODE HOLDING THE DATABASE, DELETE DISTRIBUTED CFG TOO
      getConfigurationMap().remove(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName);
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Dropped last copy of database %s, removing it from the cluster", dbName);
    }
  }

  @SuppressWarnings("unchecked")
  public ODocument getStats() {
    final ODocument doc = new ODocument();

    final Map<String, HashMap<String, Object>> nodes = new HashMap<String, HashMap<String, Object>>();
    doc.field("nodes", nodes);

    Map<String, Object> localNode = new HashMap<String, Object>();
    doc.field("localNode", localNode);

    localNode.put("name", nodeName);
    localNode.put("averageResponseTime", messageService.getAverageResponseTime());

    Map<String, Object> databases = new HashMap<String, Object>();
    localNode.put("databases", databases);
    for (String dbName : messageService.getDatabases()) {
      Map<String, Object> db = new HashMap<String, Object>();
      databases.put(dbName, db);
    }

    return doc;
  }

  public String getNodeName(final Member iMember) {
    if (iMember == null)
      return "?";

    final ODocument cfg = getNodeConfigurationByUuid(iMember.getUuid());
    if (cfg != null)
      return cfg.field("name");

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    return "ext:" + iMember.getUuid();
  }

  @Override
  public String getNodeNameById(final int id) {
    if (id < 0)
      throw new IllegalArgumentException("Node id " + id + " is invalid");

    synchronized (registeredNodeById) {
      if (id < registeredNodeById.size())
        return registeredNodeById.get(id);
    }
    return null;
  }

  @Override
  public int getNodeIdByName(final String name) {
    synchronized (registeredNodeByName) {
      return registeredNodeByName.get(name);
    }
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      updateLastClusterChange();
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Added new node id=%s name=%s", iEvent.getMember(),
          getNodeName(iEvent.getMember()));

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
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
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      updateLastClusterChange();

      final Member member = iEvent.getMember();
      final String nodeLeftName = getNodeName(member);
      if (nodeLeftName != null) {
        final int nodeLeftId = getNodeIdByName(nodeLeftName);

        // REMOVE INTRA SERVER CONNECTION
        closeRemoteServer(nodeLeftName);

        // NOTIFY NODE LEFT
        for (ODistributedLifecycleListener l : listeners)
          try {
            l.onNodeLeft(nodeLeftName);
          } catch (Exception e) {
            // IGNORE IT
          }

        // UNLOCK ANY PENDING LOCKS
        if (messageService != null)
          for (String dbName : messageService.getDatabases())
            messageService.getDatabase(dbName).handleUnreachableNode(nodeLeftId);

        final HashSet<String> entriesToRemove = new HashSet<String>();

        final Map<String, Object> map = getConfigurationMap();

        // UNREGISTER DB STATUSES
        for (Iterator<String> it = map.keySet().iterator(); it.hasNext();) {
          final String n = it.next();

          if (n.startsWith(CONFIG_DBSTATUS_PREFIX)) {
            final String part = n.substring(CONFIG_DBSTATUS_PREFIX.length());
            final int pos = part.indexOf(".");
            if (pos > -1) {
              // CHECK ANY DB STATUS OF THE LEFT NODE
              if (part.substring(0, pos).equals(nodeLeftName)) {
                ODistributedServerLog.debug(this, nodeName, null, DIRECTION.NONE,
                    "Removing dbstatus for the node %s that just left: %s", nodeLeftName, n);
                entriesToRemove.add(n);
              }
            }
          }
        }

        // REMOVE THE ENTRIES
        for (String entry : entriesToRemove) {
          map.remove(entry);
        }

        activeNodes.remove(nodeLeftName);

        ODistributedServerLog.warn(this, nodeLeftName, null, DIRECTION.NONE, "Node removed id=%s name=%s", member, nodeLeftName);

        if (nodeLeftName.startsWith("ext:")) {
          final List<String> registeredNodes = getRegisteredNodes();

          ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
              "Removed node id=%s name=%s has not being recognized. Remove the node manually (registeredNodes=%s)", member,
              nodeLeftName, registeredNodes);
        }

        for (String databaseName : getManagedDatabases()) {
          final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);
          if (electCurrentNodeAsNewCoordinator(cfg, nodeLeftName, databaseName)) {
            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Distributed configuration modified");
            updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
          }
        }

        // REMOVE NODE IN DB CFG
        if (messageService != null)
          messageService.handleUnreachableNode(nodeLeftName);
      }

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
  }

  /**
   * Elects current node as new coordinator.
   *
   * @param cfg
   * @param nodeLeftName
   */
  protected boolean electCurrentNodeAsNewCoordinator(final ODistributedConfiguration cfg, final String nodeLeftName,
      final String databaseName) {
    // TODO: ALL THE NODES SHOULD CONCUR TO ELECTION IN CASE THE 2ND IN CHARGE DIES BEFORE TO BE ELECTED
    String nextServer = getNextEligibleServerCoordinator(cfg);
    while (!isNodeAvailable(nextServer)) {
      // SET THE SERVER AS OFFLINE IN CFG
      cfg.setServerOffline(nextServer, null);

      // GET THE NEXT ONE
      final String nextCandidate = getNextEligibleServerCoordinator(cfg);
      if (nextServer.equals(nextCandidate)) {
        // NO MORE CANDIDATES, SET LOCAL NODE
        nextServer = nodeName;
        break;
      }

      nextServer = nextCandidate;
    }

    return electCurrentNodeAsNewCoordinator(nodeLeftName, nextServer, databaseName, cfg);
  }

  protected boolean electCurrentNodeAsNewCoordinator(final String nodeLeftName, final String nodeToElect, final String databaseName,
      final ODistributedConfiguration cfg) {
    boolean modifiedCfg = false;

    final String coordinator = getCoordinatorServer(cfg);
    if (nodeLeftName.equals(coordinator)) {
      if (nodeName.equals(nodeToElect)) {
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Electing node '%s' as new coordinator", nodeName);

        // ELECT CURRENT NODE AS NEW COORDINATOR
        cfg.setServerOffline(nodeLeftName, nodeName);
        modifiedCfg = true;
      }

      // COLLECT ALL THE CLUSTERS WITH REMOVED NODE AS OWNER
      final Set<String> clustersWithNotAvailableOwner = cfg.getClustersOwnedByServer(nodeLeftName);
      if (reassignClustersOwnership(nodeName, cfg, databaseName, clustersWithNotAvailableOwner, false))
        modifiedCfg = true;
    }

    return modifiedCfg;
  }

  protected boolean reassignClustersOwnership(final String iNode, final ODistributedConfiguration cfg, final String databaseName,
      final Set<String> clustersWithNotAvailableOwner, final boolean rebalance) {

    // REASSIGN CLUSTERS WITHOUT AN OWNER, AVOIDING TO REBALANCE EXISTENT
    final ODatabaseDocumentTx database = (ODatabaseDocumentTx) serverInstance.openDatabase(databaseName, "internal", "internal",
        null, true);
    try {
      return rebalanceClusterOwnership(iNode, database, cfg, clustersWithNotAvailableOwner, rebalance);
    } finally {
      database.close();
    }
  }

  private String getCoordinatorServer(final ODistributedConfiguration cfg) {
    final List<String> servers = cfg.getOriginalServers("*");
    if (servers == null || servers.isEmpty())
      return null;

    String next = servers.get(0);
    if (ODistributedConfiguration.NEW_NODE_TAG.equals(next)) {
      // GET THE NEXT ONE
      next = servers.size() > 1 ? servers.get(1) : null;
    }

    return next;
  }

  private boolean isLocalNodeTheCoordinator(final ODistributedConfiguration cfg) {
    final List<String> servers = cfg.getOriginalServers("*");
    return !servers.isEmpty() && nodeName.equalsIgnoreCase(servers.get(0));
  }

  /**
   * Returns the next eligible server coordinator (2nd in server list).
   */
  private String getNextEligibleServerCoordinator(final ODistributedConfiguration cfg) {
    final List<String> servers = cfg.getOriginalServers("*");
    if (servers.size() < 2)
      return null;

    String next = servers.get(1);
    if (ODistributedConfiguration.NEW_NODE_TAG.equals(next)) {
      // GET THE NEXT ONE
      next = servers.size() > 2 ? servers.get(2) : null;
    }

    return next;
  }

  @Override
  public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
  }

  @Override
  public void entryAdded(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      if (iEvent.getMember() == null)
        // IGNORE IT
        return;

      final String key = iEvent.getKey();
      if (key.startsWith(CONFIG_NODE_PREFIX)) {
        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
          final ODocument cfg = (ODocument) iEvent.getValue();
          final String joinedNodeName = (String) cfg.field("name");

          if (this.nodeName.equals(joinedNodeName)) {
            ODistributedServerLog.error(this, joinedNodeName, getNodeName(iEvent.getMember()), DIRECTION.IN,
                "Found a new node with the same name as current: '" + joinedNodeName
                    + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file");

            throw new ODistributedException("Found a new node with the same name as current: '" + joinedNodeName
                + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file");
          }

          // NOTIFY NODE IS GOING TO BE ADDED. EVERYBODY IS OK?
          for (ODistributedLifecycleListener l : listeners) {
            if (!l.onNodeJoining(joinedNodeName)) {
              // DENY JOIN
              ODistributedServerLog.info(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN,
                  "denied node to join the cluster id=%s name=%s", iEvent.getMember(), getNodeName(iEvent.getMember()));
              return;
            }
          }

          activeNodes.put(joinedNodeName, (Member) iEvent.getMember());
          try {
            getRemoteServer(joinedNodeName);
          } catch (IOException e) {
            ODistributedServerLog.error(this, nodeName, joinedNodeName, DIRECTION.OUT, "Error on connecting to node %s",
                joinedNodeName);
          }

          ODistributedServerLog.info(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN,
              "Added node configuration id=%s name=%s, now %d nodes are configured", iEvent.getMember(),
              getNodeName(iEvent.getMember()), activeNodes.size());

          // installNewDatabasesFromCluster(false);

          // NOTIFY NODE WAS ADDED SUCCESSFULLY
          for (ODistributedLifecycleListener l : listeners)
            l.onNodeJoined(joinedNodeName);
        }

      } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
        // SYNCHRONIZE ADDING OF CLUSTERS TO AVOID DEADLOCKS
        final String databaseName = key.substring(CONFIG_DATABASE_PREFIX.length());

        onDatabaseEvent((ODocument) iEvent.getValue(), databaseName);

      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.info(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN, "Received new status %s=%s",
            key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

        // REASSIGN HIS CLUSTER
        final String dbNode = key.substring(CONFIG_DBSTATUS_PREFIX.length());
        final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
        final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);

        onDatabaseEvent(nodeName, databaseName, (DB_STATUS) iEvent.getValue());

      }
    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
  }

  @Override
  public void entryUpdated(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      final String key = iEvent.getKey();
      final String eventNodeName = getNodeName(iEvent.getMember());

      if (key.startsWith(CONFIG_NODE_PREFIX)) {
        ODistributedServerLog.debug(this, nodeName, eventNodeName, DIRECTION.NONE, "Updated node configuration id=%s name=%s",
            iEvent.getMember(), eventNodeName);

        final ODocument cfg = (ODocument) iEvent.getValue();

        if (!activeNodes.containsKey((String) cfg.field("name")))
          updateLastClusterChange();

        activeNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());

      } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
          final String databaseName = key.substring(CONFIG_DATABASE_PREFIX.length());

          ODistributedServerLog.info(this, nodeName, eventNodeName, DIRECTION.IN, "Updated configuration db=%s", databaseName);
          onDatabaseEvent((ODocument) iEvent.getValue(), databaseName);
        }
      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.info(this, nodeName, eventNodeName, DIRECTION.IN, "Received updated status %s=%s",
            key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

        // CALL DATABASE EVENT
        final String dbNode = key.substring(CONFIG_DBSTATUS_PREFIX.length());
        final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
        final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);

        onDatabaseEvent(nodeName, databaseName, (DB_STATUS) iEvent.getValue());

      } else if (key.startsWith(CONFIG_REGISTEREDNODES)) {
        ODistributedServerLog.info(this, nodeName, eventNodeName, DIRECTION.IN, "Received updated about registered nodes");
        reloadRegisteredNodes();
      }
    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }

  }

  @Override
  public void entryRemoved(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      final String key = iEvent.getKey();
      if (key.startsWith(CONFIG_NODE_PREFIX)) {
        final String nName = getNodeName(iEvent.getMember());
        if (nName != null) {
          ODistributedServerLog.debug(this, nodeName, null, DIRECTION.NONE, "Removed node configuration id=%s name=%s",
              iEvent.getMember(), nName);
          activeNodes.remove(nName);
          closeRemoteServer(nName);
        }

        updateLastClusterChange();

      } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
        synchronized (cachedDatabaseConfiguration) {
          cachedDatabaseConfiguration.remove(key.substring(CONFIG_DATABASE_PREFIX.length()));
        }
        updateLastClusterChange();

      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.debug(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN, "received removed status %s=%s",
            key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

        // CALL DATABASE EVENT
        final String dbNode = key.substring(CONFIG_DBSTATUS_PREFIX.length());
        final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
        final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);

        onDatabaseEvent(nodeName, databaseName, (DB_STATUS) iEvent.getValue());
      }
    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
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

  @Override
  public boolean isNodeAvailable(final String iNodeName) {
    return activeNodes.containsKey(iNodeName);
  }

  @Override
  public boolean isNodeOnline(final String iNodeName, final String iDatabaseName) {
    return getDatabaseStatus(iNodeName, iDatabaseName) == DB_STATUS.ONLINE;
  }

  public boolean isOffline() {
    return status != NODE_STATUS.ONLINE;
  }

  /**
   * Returns the available nodes (not offline) and clears the node list by removing the offline nodes.
   */
  public int getAvailableNodes(final Collection<String> iNodes, final String databaseName) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext();) {
      final String node = it.next();

      if (!isNodeAvailable(node, databaseName))
        it.remove();
    }
    return iNodes.size();
  }

  public void waitUntilOnline() throws InterruptedException {
    while (!status.equals(NODE_STATUS.ONLINE))
      Thread.sleep(100);
  }

  public HazelcastInstance getHazelcastInstance() {
    for (int retry = 1; hazelcastInstance == null && !Thread.currentThread().isInterrupted(); ++retry) {
      if (retry > 25)
        throw new ODistributedException("Hazelcast instance is not available");

      // WAIT UNTIL THE INSTANCE IS READY, FOR MAXIMUM 5 SECS (25 x 200ms)
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return hazelcastInstance;
  }

  public Lock getLock(final String iName) {
    return getHazelcastInstance().getLock("orientdb." + iName);
  }

  @Override
  public String toString() {
    return nodeName;
  }

  @Override
  public ODistributedMessageServiceImpl getMessageService() {
    return messageService;
  }

  public void updateCachedDatabaseConfiguration(String iDatabaseName, ODocument cfg, boolean iSaveToDisk,
      boolean iDeployToCluster) {
    final boolean updated = super.updateCachedDatabaseConfiguration(iDatabaseName, cfg, iSaveToDisk);

    if (updated) {
      if (iDeployToCluster) {
        // DEPLOY THE CONFIGURATION TO THE CLUSTER
        ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
        getConfigurationMap().put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabaseName, cfg);
      }

      serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());
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

  @Override
  public int getAvailableNodes(final String iDatabaseName) {
    int availableNodes = 0;
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        availableNodes++;
    }
    return availableNodes;
  }

  @Override
  public ConcurrentMap<String, Object> getConfigurationMap() {
    return getHazelcastInstance().getMap("orientdb");
  }

  public boolean installDatabase(final boolean iStartup, final String databaseName, final ODocument config) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Current node started as %s for database '%s'",
        cfg.getServerRole(nodeName), databaseName);

    final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();
    if (!iStartup && configuredDatabases.contains(databaseName))
      return false;

    final ODistributedDatabaseImpl distrDatabase = messageService.registerDatabase(databaseName);

    // CREATE THE DISTRIBUTED QUEUE
    if (distrDatabase.getSyncConfiguration().isEmpty()) {
      // FIRST TIME, ASK FOR FULL REPLICA

      return requestFullDatabase(databaseName, iStartup, distrDatabase);

    } else {
      try {

        // TRY WITH DELTA
        return requestDatabaseDelta(distrDatabase, databaseName);
        // return requestFullDatabase(databaseName, backupDatabase, distrDatabase);

      } catch (ODistributedDatabaseDeltaSyncException e) {
        // SWITCH TO FULL
        return requestFullDatabase(databaseName, iStartup, distrDatabase);
      }
    }

  }

  protected boolean requestFullDatabase(String databaseName, boolean backupDatabase, ODistributedDatabaseImpl distrDatabase) {
    for (int retry = 0; retry < DEPLOY_DB_MAX_RETRIES; ++retry) {
      // ASK DATABASE TO THE FIRST NODE, THE FIRST ATTEMPT, OTHERWISE ASK TO EVERYONE
      if (requestDatabaseFullSync(distrDatabase, backupDatabase, databaseName, retry > 0))
        // DEPLOYED
        return true;
    }
    // RETRY COUNTER EXCEED
    return false;
  }

  public boolean requestDatabaseDelta(final ODistributedDatabaseImpl distrDatabase, final String databaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, nodeName);

    filterAvailableNodes(nodes, databaseName);

    ODistributedServerLog.warn(this, nodeName, nodes.toString(), DIRECTION.OUT,
        "requesting delta database sync for '%s' on local server...", databaseName);

    // CREATE A MAP OF NODE/LSN BY READING LAST LSN SAVED
    final Map<String, OLogSequenceNumber> selectedNodes = new HashMap<String, OLogSequenceNumber>(nodes.size());
    for (String node : nodes) {
      final OLogSequenceNumber lsn = distrDatabase.getSyncConfiguration().getLSN(node);
      if (lsn != null) {
        selectedNodes.put(node, lsn);
      } else
        ODistributedServerLog.info(this, nodeName, node, DIRECTION.OUT,
            "Last LSN not found for database '%s', skip delta database sync", databaseName);
    }

    if (selectedNodes.isEmpty()) {
      // FORCE FULL DATABASE SYNC
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
          "No LSN found for delta sync for database %s. Asking for full database sync...", databaseName);
      throw new ODistributedDatabaseDeltaSyncException("Requested database delta sync but no LSN was found");
    }

    for (Map.Entry<String, OLogSequenceNumber> entry : selectedNodes.entrySet()) {

      final OSyncDatabaseDeltaTask deployTask = new OSyncDatabaseDeltaTask(entry.getValue());
      for (String clName : cfg.getClusterNames()) {
        if (!cfg.isReplicationActive(clName, nodeName))
          deployTask.excludeClusterName(clName);
      }

      final List<String> targetNodes = new ArrayList<String>(1);
      targetNodes.add(entry.getKey());

      ODistributedServerLog.info(this, nodeName, entry.getKey(), DIRECTION.OUT, "Requesting database delta sync for '%s' LSN=%s...",
          databaseName, entry.getValue());

      final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, targetNodes, deployTask,
          EXECUTION_MODE.RESPONSE, null, null).getPayload();

      ODistributedServerLog.info(this, nodeName, entry.getKey(), DIRECTION.IN, "Receiving delta sync for '%s'...", databaseName);

      ODistributedServerLog.debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Database delta sync returned: %s",
          results);

      final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

      // EXTRACT THE REAL RESULT
      for (Entry<String, Object> r : results.entrySet()) {
        final Object value = r.getValue();

        if (value instanceof Boolean)
          continue;
        else if (value instanceof ODistributedDatabaseDeltaSyncException) {
          ODistributedServerLog.error(this, nodeName, r.getKey(), DIRECTION.IN,
              "Error on installing database delta %s, requesting full database sync...", databaseName, dbPath);
          throw (ODistributedDatabaseDeltaSyncException) value;
        } else if (value instanceof Throwable) {
          ODistributedServerLog.error(this, nodeName, r.getKey(), DIRECTION.IN, "Error on installing database delta %s in %s (%s)",
              (Exception) value, databaseName, dbPath, value);
        } else if (value instanceof ODistributedDatabaseChunk) {

          final Set<String> toSyncClusters = installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, r.getKey(),
              (ODistributedDatabaseChunk) value, true);

          // SYNC ALL THE CLUSTERS
          for (String cl : toSyncClusters) {
            // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
            OCommandExecutorSQLSyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
          }

          ODistributedServerLog.info(this, nodeName, entry.getKey(), DIRECTION.IN, "Installed delta of database '%s'...",
              databaseName);

        } else
          throw new IllegalArgumentException("Type " + value + " not supported");
      }
    }

    return true;
  }

  protected boolean requestDatabaseFullSync(final ODistributedDatabaseImpl distrDatabase, final boolean backupDatabase,
      final String databaseName, final boolean iAskToAllNodes) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    // GET ALL THE OTHER SERVERS
    final Collection<String> nodes = cfg.getServers(null, nodeName);

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

    ODistributedServerLog.warn(this, nodeName, selectedNodes.toString(), DIRECTION.OUT,
        "requesting deploy of database '%s' on local server...", databaseName);

    final OAbstractReplicatedTask deployTask = new OSyncDatabaseTask();

    final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, selectedNodes, deployTask,
        EXECUTION_MODE.RESPONSE, null, null).getPayload();

    ODistributedServerLog.debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Deploy returned: %s", results);

    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    // EXTRACT THE REAL RESULT
    for (Entry<String, Object> r : results.entrySet()) {
      final Object value = r.getValue();

      if (value instanceof Boolean)
        continue;
      else if (value instanceof Throwable) {
        ODistributedServerLog.error(this, nodeName, r.getKey(), DIRECTION.IN, "Error on installing database %s in %s",
            (Exception) value, databaseName, dbPath);
      } else if (value instanceof ODistributedDatabaseChunk) {
        if (backupDatabase)
          backupCurrentDatabase(databaseName);

        final Set<String> toSyncClusters = installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, r.getKey(),
            (ODistributedDatabaseChunk) value, false);

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

    // MOVE DIRECTORY TO ../backup/databases/<db-name>
    final String backupDirectory = OGlobalConfiguration.DISTRIBUTED_BACKUP_DIRECTORY.getValueAsString();
    if (backupDirectory == null || OIOUtils.getStringContent(backupDirectory).trim().isEmpty())
      // SKIP BACKUP
      return;

    final String backupPath = serverInstance.getDatabaseDirectory() + "/" + backupDirectory + "/" + iDatabaseName;
    final File backupFullPath = new File(backupPath);
    final File f = new File(backupDirectory);
    if (f.exists())
      OFileUtils.deleteRecursively(backupFullPath);
    else
      f.mkdirs();

    final String dbPath = serverInstance.getDatabaseDirectory() + iDatabaseName;

    // MOVE THE DATABASE ON CURRENT NODE
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
        "moving existent database '%s' in '%s' to '%s' and get a fresh copy from a remote node...", iDatabaseName, dbPath,
        backupPath);

    final File oldDirectory = new File(dbPath);
    if (!oldDirectory.renameTo(backupFullPath)) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
          "error on moving existent database '%s' located in '%s' to '%s'. Deleting old database...", iDatabaseName, dbPath,
          backupFullPath);

      OFileUtils.deleteRecursively(oldDirectory);
    }
  }

  /**
   * Returns the clusters where sync is required.
   */
  protected Set<String> installDatabaseFromNetwork(final String dbPath, final String databaseName,
      final ODistributedDatabaseImpl distrDatabase, final String iNode, final ODistributedDatabaseChunk firstChunk,
      final boolean delta) {

    final String fileName = Orient.getTempPath() + "install_" + databaseName + ".zip";

    final String localNodeName = nodeName;

    ODistributedServerLog.info(this, localNodeName, iNode, DIRECTION.IN, "Copying remote database '%s' to: %s", databaseName,
        fileName);

    final File file = new File(fileName);
    if (file.exists())
      file.delete();

    try {
      file.getParentFile().mkdirs();
      file.createNewFile();
    } catch (IOException e) {
      throw OException.wrapException(new ODistributedException("Error on creating temp database file to install locally"), e);
    }

    // DELETE ANY PREVIOUS .COMPLETED FILE
    final File completedFile = new File(file.getAbsolutePath() + ".completed");
    if (completedFile.exists())
      completedFile.delete();

    final AtomicReference<OLogSequenceNumber> lsn = new AtomicReference<OLogSequenceNumber>();

    try {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.currentThread().setName("OrientDB installDatabase node=" + nodeName + " db=" + databaseName);
            ODistributedDatabaseChunk chunk = firstChunk;

            lsn.set(chunk.lsn);

            final OutputStream fOut = new FileOutputStream(fileName, false);
            try {

              long fileSize = writeDatabaseChunk(1, chunk, fOut);
              for (int chunkNum = 2; !chunk.last; chunkNum++) {
                final ODistributedResponse response = sendRequest(databaseName, null, OMultiValue.getSingletonList(iNode),
                    new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                    EXECUTION_MODE.RESPONSE, null, null);

                final Object result = response.getPayload();
                if (result instanceof Boolean)
                  continue;
                else if (result instanceof Exception) {
                  ODistributedServerLog.error(this, nodeName, iNode, DIRECTION.IN,
                      "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
                } else if (result instanceof ODistributedDatabaseChunk) {
                  chunk = (ODistributedDatabaseChunk) result;
                  fileSize += writeDatabaseChunk(chunkNum, chunk, fOut);
                }
              }

              fOut.flush();

              // CREATE THE .COMPLETED FILE TO SIGNAL EOF
              new File(file.getAbsolutePath() + ".completed").createNewFile();

              if (lsn.get() != null) {
                // UPDATE LSN VERSUS THE TARGET NODE
                try {
                  final ODistributedDatabase distrDatabase = getMessageService().getDatabase(databaseName);

                  distrDatabase.getSyncConfiguration().setLSN(iNode, lsn.get());

                } catch (IOException e) {
                  ODistributedServerLog.error(this, nodeName, iNode, DIRECTION.IN,
                      "Error on updating distributed-sync.json file for database '%s'. Next request of delta of changes will contains old records too",
                      e, databaseName);
                }
              } else
                ODistributedServerLog.warn(this, nodeName, iNode, DIRECTION.IN,
                    "LSN not found in database from network, database delta sync will be not available for database '%s'",
                    databaseName);

              ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Database copied correctly, size=%s",
                  OFileUtils.getSizeAsString(fileSize));

            } finally {
              try {
                fOut.flush();
                fOut.close();
              } catch (IOException e) {
              }
            }

          } catch (Exception e) {
            ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "Error on transferring database '%s' to '%s'", e,
                databaseName, fileName);
            throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
          }
        }
      }).start();

    } catch (Exception e) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "Error on transferring database '%s' to '%s'", e,
          databaseName, fileName);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    }

    final ODatabaseDocumentTx db = installDatabaseOnLocalNode(databaseName, dbPath, iNode, fileName, delta);

    if (db != null) {
      db.activateOnCurrentThread();
      db.close();
      final OStorage stg = Orient.instance().getStorage(databaseName);
      if (stg != null)
        stg.close();

      final Lock lock = getLock(databaseName + ".cfg");
      lock.lock();
      try {
        // GET LAST VERSION IN LOCK
        final ODistributedConfiguration cfg = getDatabaseConfiguration(db.getName());

        final boolean distribCfgDirty = rebalanceClusterOwnership(nodeName, db, cfg, new HashSet<String>(), true);
        if (distribCfgDirty) {
          ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Distributed configuration modified");
          updateCachedDatabaseConfiguration(db.getName(), cfg.serialize(), true, true);
        }

        distrDatabase.setOnline();
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

    boolean cfgUpdated = false;
    for (OClass c : iDatabase.getMetadata().getSchema().getClasses()) {
      if (!(c.getClusterSelection() instanceof OLocalClusterStrategy))
        // INSTALL ONLY ON NON-ENHANCED CLASSES
        if (installClustersOfClass(iDatabase, c))
          cfgUpdated = true;
    }

    if (cfgUpdated)
      updateCachedDatabaseConfiguration(iDatabase.getName(), cfg.serialize(), true, true);
  }

  /**
   * Guarantees that each class has own master cluster.
   */
  public synchronized boolean installClustersOfClass(final ODatabaseInternal iDatabase, final OClass iClass) {

    final String databaseName = iDatabase.getName();

    if (!(iClass.getClusterSelection() instanceof OLocalClusterStrategy))
      // INJECT LOCAL CLUSTER STRATEGY
      ((OClassImpl) iClass).setClusterSelectionInternal(new OLocalClusterStrategy(this, databaseName, iClass));

    ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());

    if (iClass.isAbstract())
      return false;

    final int[] clusterIds = iClass.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int clusterId : clusterIds)
      clusterNames.add(iDatabase.getClusterNameById(clusterId));

    final Lock lock = getLock(databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      cfg = getDatabaseConfiguration(iDatabase.getName());

      final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

      return clusterAssignmentStrategy.assignClusterOwnershipOfClass(iDatabase, cfg, iClass, availableNodes, new HashSet<String>(),
          true);

    } finally {
      lock.unlock();
    }
  }

  protected void onDatabaseEvent(final ODocument config, final String databaseName) {
    updateCachedDatabaseConfiguration(databaseName, config, true, false);
    installNewDatabase(false, databaseName, config);
  }

  protected synchronized boolean rebalanceClusterOwnership(final String iNode, final ODatabaseInternal iDatabase,
      final ODistributedConfiguration cfg, Set<String> clustersWithNotAvailableOwner, final boolean rebalance) {
    final ODistributedConfiguration.ROLES role = cfg.getServerRole(iNode);
    if (role != ODistributedConfiguration.ROLES.MASTER)
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return false;

    if (iDatabase.isClosed())
      getServerInstance().openDatabase(iDatabase);

    ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Reassigning cluster ownership for database %s",
        iDatabase.getName());

    // OVERWRITE CLUSTER SELECTION STRATEGY BY SUFFIX
    boolean distributedCfgDirty = false;

    final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

    final OSchema schema = ((ODatabaseInternal<?>) iDatabase).getDatabaseOwner().getMetadata().getSchema();
    for (final OClass clazz : schema.getClasses()) {
      if (clusterAssignmentStrategy.assignClusterOwnershipOfClass(iDatabase, cfg, clazz, availableNodes,
          clustersWithNotAvailableOwner, rebalance))
        distributedCfgDirty = true;

      if (!rebalance && clustersWithNotAvailableOwner.isEmpty())
        // NO MORE CLUSTER TO REASSIGN
        break;
    }

    return distributedCfgDirty;
  }

  protected void installDbClustersLocalStrategy(final ODatabaseInternal iDatabase) {
    final boolean useASuperUserDb = iDatabase.isClosed() || iDatabase.getUser() != null;

    // USE A DATABASE WITH SUPER PRIVILEGES
    final ODatabaseInternal db = useASuperUserDb ? messageService.getDatabase(iDatabase.getName()).getDatabaseInstance()
        : iDatabase;

    try {
      // OVERWRITE CLUSTER SELECTION STRATEGY
      final OSchema schema = db.getDatabaseOwner().getMetadata().getSchema();

      for (OClass c : schema.getClasses()) {
        ((OClassImpl) c).setClusterSelectionInternal(new OLocalClusterStrategy(this, db.getName(), c));
      }

    } finally {
      if (useASuperUserDb) {
        // REPLACE CURRENT DB
        db.close();
        iDatabase.activateOnCurrentThread();
      }
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
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |         WARNING: FIRST DISTRIBUTED RUN CONFIGURATION          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | This is the first time that the server is running as          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | distributed. Please type the name you want to assign to the   |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | current server node.                                          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |                                                               |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | setting ORIENTDB_NODE_NAME to the server node name to use.    |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.print(OAnsiCode.format("\n$ANSI{yellow Node name [BLANK=auto generate it]: }"));

      OConsoleReader reader = new ODefaultConsoleReader();
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
          throw OException.wrapException(new OConfigurationException("Cannot save server configuration"), e);
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
  protected void loadLocalDatabases() {
    for (Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Opening database '%s'...", databaseName);

        ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Current node started as %s for database '%s'",
            cfg.getServerRole(nodeName), databaseName);

        final boolean publishCfg = !getConfigurationMap().containsKey(CONFIG_DATABASE_PREFIX + databaseName);
        if (publishCfg) {
          // PUBLISH CFG FIRST TIME
          ODocument cfgDoc = cfg.serialize();
          ORecordInternal.setRecordSerializer(cfgDoc, ODatabaseDocumentTx.getDefaultSerializer());
          getConfigurationMap().put(CONFIG_DATABASE_PREFIX + databaseName, cfgDoc);
        }

        final ODistributedDatabaseImpl ddb = messageService.registerDatabase(databaseName);

        // 1ST NODE TO HAVE THE DATABASE: FORCE THE COORDINATOR TO BE LOCAL NODE
        cfg = getDatabaseConfiguration(databaseName);

        cfg.addNewNodeInServerList(nodeName);

        final String coordinator = getCoordinatorServer(cfg);
        if (!nodeName.equals(coordinator)) {
          ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE,
              "First load of database '%s' in the cluster, set local node as coordinator (was '%s')", databaseName, coordinator);
          electCurrentNodeAsNewCoordinator(cfg, coordinator, databaseName);
        }

        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Distributed configuration modified");
        updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);

        ddb.setOnline();
      }
    }
  }

  protected void installNewDatabasesFromCluster(final boolean iStartup) {
    if (activeNodes.size() <= 1) {
      // NO OTHER NODES WHERE ALIGN
      return;
    }

    for (Entry<String, Object> entry : getConfigurationMap().entrySet()) {
      if (entry.getKey().startsWith(CONFIG_DATABASE_PREFIX)) {
        final String databaseName = entry.getKey().substring(CONFIG_DATABASE_PREFIX.length());
        installNewDatabase(iStartup, databaseName, (ODocument) entry.getValue());
      }
    }
  }

  private boolean installNewDatabase(boolean iStartup, final String databaseName, final ODocument config) {
    final Boolean autoDeploy = config.field("autoDeploy");
    if (autoDeploy != null && autoDeploy) {
      return installDatabase(iStartup, databaseName, config);
    }
    return false;
  }

  protected long writeDatabaseChunk(final int iChunkId, final ODistributedDatabaseChunk chunk, final OutputStream out)
      throws IOException {

    ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId,
        chunk.offset, OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }

  protected ODatabaseDocumentTx installDatabaseOnLocalNode(final String databaseName, final String dbPath, final String iNode,
      final String iDatabaseCompressedFile, final boolean delta) {
    ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN, "Installing database '%s' to: %s...", databaseName, dbPath);

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

        @Override
        public int available() throws IOException {
          while (true) {
            final int avail = super.available();
            if (avail > 0)
              return avail;

            if (fCompleted.exists())
              return 0;

            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }
      };

      final Lock lock = getLock(databaseName + ".cfg");
      lock.lock();
      try {
        if (delta) {

          new OIncrementalServerSync().importDelta(serverInstance, db, in, iNode);

        } else {

          // IMPORT FULL DATABASE
          db.restore(in, null, null, this);

        }
      } finally {
        in.close();
        lock.unlock();
      }

      ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Installed database '%s'", databaseName);

      return db;

    } catch (IOException e) {
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.IN, "Error on copying database '%s' on local server", e,
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
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Loaded database configuration from active cluster");
        updateCachedDatabaseConfiguration(iDatabaseName, cfg, false, false);
        return cfg;
      }
    }

    // NO NODE IN CLUSTER, LOAD FROM FILE
    return super.loadDatabaseConfiguration(iDatabaseName, file);
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

  public void stopNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of stopping node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(nodeId, getNextMessageIdCounter(), null, new OStopNodeTask(),
        EXECUTION_MODE.NO_RESPONSE);

    getRemoteServer(iNode).sendRequest(request, iNode);
  }

  public void restartNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of restarting node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(nodeId, getNextMessageIdCounter(), null, new ORestartNodeTask(),
        EXECUTION_MODE.NO_RESPONSE);

    getRemoteServer(iNode).sendRequest(request, iNode);
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

  public void filterAvailableNodes(Collection<String> iNodes, final String databaseName) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext();) {
      final String nodeName = it.next();
      if (!isNodeAvailable(nodeName, databaseName))
        it.remove();
    }
  }

  private void reloadRegisteredNodes() {
    final ODocument registeredNodesFromCluster = new ODocument();
    final String registeredNodesFromClusterAsJson = (String) getConfigurationMap().get(CONFIG_REGISTEREDNODES);

    if (registeredNodesFromClusterAsJson != null) {
      registeredNodesFromCluster.fromJSON(registeredNodesFromClusterAsJson);
      registeredNodeById = registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST);
      registeredNodeByName = registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP);
    } else
      throw new ODistributedException("Cannot find distributed registeredNodes configuration");

  }

  public long getNextMessageIdCounter() {
    return localMessageIdCounter.getAndIncrement();
  }

  private void closeRemoteServer(final String node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  protected void onDatabaseEvent(final String nodeName, final String databaseName, final DB_STATUS status) {
    updateLastClusterChange();
    //
    // final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);
    //
    // if (status != null)
    // switch (status) {
    // case ONLINE:
    // final List<String> foundPartition = cfg.addNewNodeInServerList(nodeName);
    //
    // if (foundPartition != null) {
    // ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Adding node '%s' in partition: db=%s %s",
    // nodeName, databaseName, foundPartition);
    //
    // if (reassignClustersOwnership(nodeName, cfg, databaseName, new HashSet<String>(), true))
    // updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
    // }
    // }
  }

  protected boolean isRelatedToLocalServer(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return false;
    } else if (dbUrl.startsWith("remote:"))
      return false;

    return true;
  }
}
