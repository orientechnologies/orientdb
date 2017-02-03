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

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCallableNoParamNoReturn;
import com.orientechnologies.common.util.OCallableUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.*;
import com.orientechnologies.orient.server.distributed.impl.task.OAbstractSyncDatabaseTask;
import com.orientechnologies.orient.server.distributed.impl.task.ODropDatabaseTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseConfigurationTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.OBeforeDatabaseOpenNetworkEventListener;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

/**
 * Hazelcast implementation for clustering.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin
    implements MembershipListener, EntryListener<String, Object>, LifecycleListener, OBeforeDatabaseOpenNetworkEventListener {

  public static final String CONFIG_DATABASE_PREFIX = "database.";

  public static final String CONFIG_NODE_PREFIX     = "node.";
  public static final String CONFIG_DBSTATUS_PREFIX = "dbstatus.";
  public static final String CONFIG_COORDINATOR     = "coordinator";
  public static final String CONFIG_REGISTEREDNODES = "registeredNodes";

  protected String hazelcastConfigFile = "hazelcast.xml";
  protected          Config            hazelcastConfig;
  protected          String            membershipListenerRegistration;
  protected          String            membershipListenerMapRegistration;
  protected volatile HazelcastInstance hazelcastInstance;

  // THIS MAP IS BACKED BY HAZELCAST EVENTS. IN THIS WAY WE AVOID TO USE HZ MAP DIRECTLY
  protected OHazelcastDistributedMap configurationMap;

  public OHazelcastPlugin() {
  }

  // Must be set before startup() is called.
  public void setHazelcastConfig(final Config config) {
    hazelcastConfig = config;
  }

  // Must be set before config() is called.
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);

    if (nodeName == null)
      assignNodeName();

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast")) {
        hazelcastConfigFile = OSystemVariableResolver.resolveSystemVariables(param.value);
        hazelcastConfigFile = OFileUtils.getPath(hazelcastConfigFile);
      }
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    Orient.instance().setRunningDistributed(true);

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    serverInstance.addTemporaryUser(REPLICATOR_USER, "" + new SecureRandom().nextLong(), "*");

    super.startup();

    status = NODE_STATUS.STARTING;

    final String localNodeName = nodeName;

    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    registeredNodeById.clear();
    registeredNodeByName.clear();

    try {
      hazelcastInstance = configureHazelcast();

      nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

      final LifecycleService lifecycleService = hazelcastInstance.getLifecycleService();
      lifecycleService.addLifecycleListener(this);

      OLogManager.instance().info(this, "Starting distributed server '%s' (hzID=%s)...", localNodeName, nodeUuid);

      final long clusterTime = getClusterTime();
      final long deltaTime = System.currentTimeMillis() - clusterTime;
      OLogManager.instance()
          .info(this, "Distributed cluster time=%s (delta from local node=%d)...", new Date(clusterTime), deltaTime);

      activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());
      activeNodesNamesByUuid.put(nodeUuid, localNodeName);
      activeNodesUuidByName.put(localNodeName, nodeUuid);

      configurationMap = new OHazelcastDistributedMap(this, hazelcastInstance);

      OServer.registerServerInstance(localNodeName, serverInstance);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (!m.getUuid().equals(nodeUuid)) {
          final String memberName = getNodeName(m, false);
          if (memberName != null && !memberName.startsWith("ext:")) {
            activeNodes.put(memberName, m);
            activeNodesNamesByUuid.put(m.getUuid(), memberName);
            activeNodesUuidByName.put(memberName, m.getUuid());
          } else if (!m.equals(hazelcastInstance.getCluster().getLocalMember()))
            ODistributedServerLog
                .warn(this, localNodeName, null, DIRECTION.NONE, "Cannot find configuration for member: %s, uuid", m, m.getUuid());
        }
      }

      while (coordinatorServer == null) {
        if (activeNodes.size() == 1) {
          // ONLY CURRENT NODE ONLINE, SET IT AS INITIAL COORDINATOR
          coordinatorServer = nodeName;
          if (configurationMap.putIfAbsent(CONFIG_COORDINATOR, coordinatorServer) == null)
            break;
        } else {
          coordinatorServer = (String) configurationMap.get(CONFIG_COORDINATOR);
          if (coordinatorServer != null)
            break;
        }

        Thread.sleep(100);
      }

      getLockManagerRequester().setCoordinatorServer(coordinatorServer);

      OLogManager.instance().info(this, "Distributed coordinator server is '%s'", coordinatorServer);

      initRegisteredNodeIds();

      messageService = new ODistributedMessageServiceImpl(this);

      initSystemDatabase();

      ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.NONE, "Servers in cluster: %s", activeNodes.keySet());

      // REMOVE ANY PREVIOUS REGISTERED SERVER WITH THE SAME NODE NAME
      final Set<String> node2Remove = new HashSet<String>();
      for (Iterator<Map.Entry<String, Object>> it = configurationMap.getHazelcastMap().entrySet().iterator(); it.hasNext(); ) {
        final Map.Entry<String, Object> entry = it.next();
        if (entry.getKey().startsWith(CONFIG_NODE_PREFIX)) {
          final ODocument nodeCfg = (ODocument) entry.getValue();
          if (nodeName.equals(nodeCfg.field("name"))) {
            // SAME NODE NAME: REMOVE IT
            node2Remove.add(entry.getKey());
          }
        }
      }

      for (String n : node2Remove)
        configurationMap.getHazelcastMap().remove(n);

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

      publishLocalNodeConfiguration();

      installNewDatabasesFromCluster();

      loadLocalDatabases();

      membershipListenerMapRegistration = configurationMap.getHazelcastMap().addEntryListener(this, true);
      membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

      // REGISTER CURRENT MEMBERS
      setNodeStatus(NODE_STATUS.ONLINE);

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

      final long healthChecker = OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY.getValueAsLong();
      if (healthChecker > 0) {
        healthCheckerTask = new OClusterHealthChecker(this);
        Orient.instance().scheduleTask(healthCheckerTask, healthChecker, healthChecker);
      }

      for (OServerNetworkListener nl : serverInstance.getNetworkListeners())
        nl.registerBeforeConnectNetworkEventListener(this);

      // WAIT ALL THE MESSAGES IN QUEUE ARE PROCESSED OR MAX 10 SECONDS
      waitStartupIsCompleted();

    } catch (Exception e) {
      ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on starting distributed plugin", e);
      throw OException.wrapException(new ODistributedStartupException("Error on starting distributed plugin"), e);
    }

    dumpServersStatus();
  }

  /**
   * Protecte system database from being replicated
   */
  protected void initSystemDatabase() {
    final ODocument defaultCfg = getStorage(OSystemDatabase.SYSTEM_DB_NAME)
        .loadDatabaseConfiguration(getDefaultDatabaseConfigFile());
    defaultCfg.field("autoDeploy", false);
    final OModifiableDistributedConfiguration sysCfg = new OModifiableDistributedConfiguration(defaultCfg);
    sysCfg.removeServer("<NEW_NODE>");
    messageService.registerDatabase(OSystemDatabase.SYSTEM_DB_NAME, sysCfg);
  }

  private void initRegisteredNodeIds() {
    final ILock lock = hazelcastInstance.getLock("orientdb." + CONFIG_REGISTEREDNODES);
    lock.lock();
    try {
      // RE-CREATE THE CFG IN LOCK
      registeredNodeById.clear();
      registeredNodeByName.clear();

      final ODocument registeredNodesFromCluster = new ODocument();

      final String registeredNodesFromClusterAsJson = (String) configurationMap.get(CONFIG_REGISTEREDNODES);
      if (registeredNodesFromClusterAsJson != null) {
        registeredNodesFromCluster.fromJSON(registeredNodesFromClusterAsJson);
        registeredNodeById.addAll((Collection<? extends String>) registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST));
        registeredNodeByName
            .putAll((Map<? extends String, ? extends Integer>) registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP));

        if (registeredNodeByName.containsKey(nodeName)) {
          nodeId = registeredNodeByName.get(nodeName);
        } else {
          // ADD CURRENT NODE
          registeredNodeById.add(nodeName);
          nodeId = registeredNodeById.size() - 1;
          registeredNodeByName.put(nodeName, nodeId);
        }
      } else {
        if (hazelcastInstance.getCluster().getMembers().size() <= 1) {
          // FIRST TIME: CREATE NEW CFG
          nodeId = 0;
          registeredNodeById.add(nodeName);
          registeredNodeByName.put(nodeName, nodeId);

        } else
          // NO CONFIG_REGISTEREDNODES, BUT MORE THAN ONE NODE PRESENT: REPAIR THE CONFIGURATION
          repairActiveServers();

      }

      registeredNodesFromCluster.field("ids", registeredNodeById, OType.EMBEDDEDLIST);
      registeredNodesFromCluster.field("names", registeredNodeByName, OType.EMBEDDEDMAP);

      configurationMap.put(CONFIG_REGISTEREDNODES, registeredNodesFromCluster.toJSON());

    } finally {
      lock.unlock();
    }
  }

  private void repairActiveServers() {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
        "Error on retrieving '%s' from cluster configuration. Repairing the configuration...", CONFIG_REGISTEREDNODES);

    final Set<Member> members = hazelcastInstance.getCluster().getMembers();

    for (Member m : members) {
      final ODocument node = (ODocument) configurationMap.get(CONFIG_NODE_PREFIX + m.getUuid());
      if (node != null) {
        final String mName = node.field("name");
        final Integer mId = node.field("id");

        if (nodeName.equals(mName))
          nodeId = mId;

        if (mId >= registeredNodeById.size()) {
          // CREATE EMPTY ENTRIES IF NEEDED
          while (mId > registeredNodeById.size()) {
            registeredNodeById.add(null);
          }
          registeredNodeById.add(mName);
        } else
          registeredNodeById.set(mId, mName);

        registeredNodeByName.put(mName, mId);
      }
    }

    ODistributedServerLog
        .warn(this, nodeName, null, DIRECTION.NONE, "Repairing of '%s' completed, registered %d servers", CONFIG_REGISTEREDNODES,
            members.size());
  }

  @Override
  public int getNodeIdByName(final String name) {
    int id = super.getNodeIdByName(name);
    if (name == null) {
      repairActiveServers();
      id = super.getNodeIdByName(name);
    }
    return id;
  }

  @Override
  public String getNodeNameById(final int id) {
    String name = super.getNodeNameById(id);
    if (name == null) {
      repairActiveServers();
      name = super.getNodeNameById(id);
    }
    return name;
  }

  protected void waitStartupIsCompleted() throws InterruptedException {
    long totalReceivedRequests = getMessageService().getReceivedRequests();
    long totalProcessedRequests = getMessageService().getProcessedRequests();

    final long start = System.currentTimeMillis();
    while (totalProcessedRequests < totalReceivedRequests - 2 && (System.currentTimeMillis() - start
        < OGlobalConfiguration.DISTRIBUTED_MAX_STARTUP_DELAY.getValueAsInteger())) {
      Thread.sleep(300);
      totalProcessedRequests = getMessageService().getProcessedRequests();
      totalReceivedRequests = getMessageService().getReceivedRequests();
    }

    serverStarted.countDown();
  }

  protected void publishLocalNodeConfiguration() {
    try {
      final ODocument cfg = getLocalNodeConfiguration();
      ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
      configurationMap.put(CONFIG_NODE_PREFIX + nodeUuid, cfg);
    } catch (Throwable t) {
      ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE, "Error on publishing local server configuration");
    }
  }

  @Override
  public Throwable convertException(final Throwable original) {
    if (!Orient.instance().isActive() || isOffline())
      return new OOfflineNodeException("Server " + nodeName + " is offline");

    if (original instanceof HazelcastException || original instanceof HazelcastInstanceNotActiveException)
      return new IOException("Hazelcast wrapped exception: " + original.getMessage(), original.getCause());

    if (original instanceof IllegalMonitorStateException)
      // THIS IS RAISED WHEN INTERNAL LOCKING IS BROKEN BECAUSE HARD SHUTDOWN
      return new IOException("Illegal monitor state: " + original.getMessage(), original.getCause());

    return original;
  }

  @Override
  public long getClusterTime() {
    try {
      return hazelcastInstance.getCluster().getClusterTime();
    } catch (HazelcastInstanceNotActiveException e) {
      return -1;
    }
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    for (OServerNetworkListener nl : serverInstance.getNetworkListeners())
      nl.unregisterBeforeConnectNetworkEventListener(this);

    OLogManager.instance().warn(this, "Shutting down node '%s'...", nodeName);
    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    try {
      final Set<String> databases = new HashSet<String>();

      if (hazelcastInstance.getLifecycleService().isRunning())
        for (Map.Entry<String, Object> entry : configurationMap.entrySet()) {
          if (entry.getKey().toString().startsWith(CONFIG_DBSTATUS_PREFIX)) {

            final String nodeDb = entry.getKey().toString().substring(CONFIG_DBSTATUS_PREFIX.length());

            if (nodeDb.startsWith(nodeName))
              databases.add(entry.getKey());
          }
        }

      // PUT DATABASES AS NOT_AVAILABLE
      for (String k : databases)
        configurationMap.put(k, DB_STATUS.NOT_AVAILABLE);

    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }

    try {
      super.shutdown();
    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }

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

    OCallableUtils.executeIgnoringAnyExceptions(new OCallableNoParamNoReturn() {
      @Override
      public void call() {
        configurationMap.destroy();
      }
    });

    OCallableUtils.executeIgnoringAnyExceptions(new OCallableNoParamNoReturn() {
      @Override
      public void call() {
        configurationMap.getHazelcastMap().removeEntryListener(membershipListenerMapRegistration);
      }
    });

    coordinatorServer = null;
    setNodeStatus(NODE_STATUS.OFFLINE);
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) throws IOException {
    if (rNodeName == null)
      throw new IllegalArgumentException("Server name is NULL");

    ORemoteServerController remoteServer = remoteServers.get(rNodeName);
    if (remoteServer == null) {
      Member member = activeNodes.get(rNodeName);
      if (member == null) {
        // SYNC PROBLEMS? TRY TO RETRIEVE THE SERVER INFORMATION FROM THE CLUSTER MAP
        for (Iterator<Map.Entry<String, Object>> it = getConfigurationMap().localEntrySet().iterator(); it.hasNext(); ) {
          final Map.Entry<String, Object> entry = it.next();
          if (entry.getKey().startsWith(CONFIG_NODE_PREFIX)) {
            final ODocument nodeCfg = (ODocument) entry.getValue();
            if (rNodeName.equals(nodeCfg.field("name"))) {
              // FOUND: USE THIS
              final String uuid = entry.getKey().substring(CONFIG_NODE_PREFIX.length());

              for (Member m : hazelcastInstance.getCluster().getMembers()) {
                if (m.getUuid().equals(uuid)) {
                  member = m;
                  registerNode(member, rNodeName);
                  break;
                }
              }
            }
          }
        }

        if (member == null)
          throw new ODistributedException("Cannot find node '" + rNodeName + "'");
      }

      for (int retry = 0; retry < 100; ++retry) {
        ODocument cfg = getNodeConfigurationByUuid(member.getUuid(), false);
        if (cfg == null || cfg.field("listeners") == null) {
          try {
            Thread.sleep(100);
            continue;

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ODistributedException("Cannot find node '" + rNodeName + "'");
          }
        }

        final String url = ODistributedAbstractPlugin.getListeningBinaryAddress(cfg);

        if (url == null)
          throw new ODatabaseException("Cannot connect to a remote node because the url was not found");

        final String userPassword = cfg.field("user_replicator");

        if (userPassword != null) {
          // OK
          remoteServer = new ORemoteServerController(this, rNodeName, url, REPLICATOR_USER, userPassword);
          final ORemoteServerController old = remoteServers.putIfAbsent(rNodeName, remoteServer);
          if (old != null) {
            remoteServer.close();
            remoteServer = old;
          }
          break;
        }

        // RETRY TO GET USR+PASSWORD IN A WHILE
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new OInterruptedException("Cannot connect to remote sevrer " + rNodeName);
        }

      }
    }

    if (remoteServer == null)
      throw new ODistributedException("Cannot find node '" + rNodeName + "'");

    return remoteServer;
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

  protected HazelcastInstance configureHazelcast() throws FileNotFoundException {

    // If hazelcastConfig is null, use the file system XML config.
    if (hazelcastConfig == null) {
      hazelcastConfig = new FileSystemXmlConfig(hazelcastConfigFile);
      hazelcastConfig.setClassLoader(this.getClass().getClassLoader());
    }

    hazelcastConfig.getMapConfig(CONFIG_REGISTEREDNODES).setBackupCount(6);
    hazelcastConfig.getMapConfig(OHazelcastDistributedMap.ORIENTDB_MAP).setMergePolicy(OHazelcastMergeStrategy.class.getName());

    return Hazelcast.newHazelcastInstance(hazelcastConfig);
  }

  @Override
  public String getPublicAddress() {
    return hazelcastConfig.getNetworkConfig().getPublicAddress();
  }

  /**
   * Initializes all the available server's databases as distributed.
   */
  protected void loadLocalDatabases() {
    for (Map.Entry<String, String> storageEntry : serverInstance.getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      if (messageService.getDatabase(databaseName) == null) {
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Opening database '%s'...", databaseName);

        // INIT THE STORAGE
        final ODistributedStorage stg = getStorage(databaseName);

        executeInDistributedDatabaseLock(databaseName, 0, null, new OCallable<Object, OModifiableDistributedConfiguration>() {
          @Override
          public Object call(OModifiableDistributedConfiguration cfg) {
            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Current node started as %s for database '%s'",
                cfg.getServerRole(nodeName), databaseName);

            final ODistributedDatabaseImpl ddb = messageService.registerDatabase(databaseName, cfg);
            ddb.resume();

            // 1ST NODE TO HAVE THE DATABASE
            cfg.addNewNodeInServerList(nodeName);

            // COLLECT ALL THE CLUSTERS WITH REMOVED NODE AS OWNER
            reassignClustersOwnership(nodeName, databaseName, cfg);

            try {
              ddb.getSyncConfiguration().setLastLSN(nodeName, ((OLocalPaginatedStorage) stg.getUnderlying()).getLSN(), false);
            } catch (IOException e) {
              ODistributedServerLog
                  .error(this, nodeName, null, DIRECTION.NONE, "Error on saving distributed LSN for database '%s' (err=%s).",
                      databaseName, e.getMessage());
            }

            ddb.setOnline();

            return null;
          }
        });
      }
    }
  }

  @Override
  public OHazelcastDistributedMap getConfigurationMap() {
    return configurationMap;
  }

  @Override
  public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
  }

  public boolean updateCachedDatabaseConfiguration(final String databaseName, final OModifiableDistributedConfiguration cfg,
      final boolean iDeployToCluster) {
    // VALIDATE THE CONFIGURATION FIRST
    getDistributedStrategy().validateConfiguration(cfg);

    boolean updated = super.updateCachedDatabaseConfiguration(databaseName, cfg);

    if (!updated && !getConfigurationMap().containsKey(CONFIG_DATABASE_PREFIX + databaseName))
      // FIRST TIME, FORCE PUBLISHING
      updated = true;

    final ODocument document = cfg.getDocument();

    if (updated) {
      if (iDeployToCluster) {
        // WRITE TO THE MAP TO BE READ BY NEW SERVERS ON JOIN
        ORecordInternal.setRecordSerializer(document, ODatabaseDocumentTx.getDefaultSerializer());
        configurationMap.put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName, document);

        // SEND A DISTRIBUTED MSG TO ALL THE SERVERS
        final Set<String> servers = new HashSet<String>(getActiveServers());
        servers.remove(nodeName);

        if (!servers.isEmpty() && messageService.getDatabase(databaseName) != null) {
          final ODistributedResponse dResponse = sendRequest(databaseName, null, servers,
              new OUpdateDatabaseConfigurationTask(databaseName, document), getNextMessageIdCounter(),
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
        }

      } else
        configurationMap.putInLocalCache(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName, document);

      // SEND NEW CFG TO ALL THE CONNECTED CLIENTS
      serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());

      dumpServersStatus();
    }

    return updated;
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

          registerNode(iEvent.getMember(), joinedNodeName);
        }

      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.info(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN, "Received new status %s=%s",
            key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

        // REASSIGN HIS CLUSTER
        final String dbNode = key.substring(CONFIG_DBSTATUS_PREFIX.length());
        final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
        final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);

        onDatabaseEvent(nodeName, databaseName, (DB_STATUS) iEvent.getValue());
        invokeOnDatabaseStatusChange(nodeName, databaseName, (DB_STATUS) iEvent.getValue());

        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember()) && DB_STATUS.ONLINE
            .equals(iEvent.getValue())) {
          final DB_STATUS s = getDatabaseStatus(getLocalNodeName(), databaseName);
          if (s == DB_STATUS.NOT_AVAILABLE) {
            // INSTALL THE DATABASE
            installDatabase(false, databaseName, false,
                OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());
          }
        }
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
        ODistributedServerLog
            .debug(this, nodeName, eventNodeName, DIRECTION.NONE, "Updated node configuration id=%s name=%s", iEvent.getMember(),
                eventNodeName);

        final ODocument cfg = (ODocument) iEvent.getValue();

        if (!activeNodes.containsKey((String) cfg.field("name")))
          updateLastClusterChange();

        activeNodes.put((String) cfg.field("name"), (Member) iEvent.getMember());
        if (iEvent.getMember().getUuid() != null) {
          activeNodesNamesByUuid.put(iEvent.getMember().getUuid(), (String) cfg.field("name"));
          activeNodesUuidByName.put((String) cfg.field("name"), iEvent.getMember().getUuid());
        }

        dumpServersStatus();

      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.info(this, nodeName, eventNodeName, DIRECTION.IN, "Received updated status %s=%s",
            key.substring(CONFIG_DBSTATUS_PREFIX.length()), iEvent.getValue());

        // CALL DATABASE EVENT
        final String dbNode = key.substring(CONFIG_DBSTATUS_PREFIX.length());
        final String nodeName = dbNode.substring(0, dbNode.indexOf("."));
        final String databaseName = dbNode.substring(dbNode.indexOf(".") + 1);

        onDatabaseEvent(nodeName, databaseName, (DB_STATUS) iEvent.getValue());
        invokeOnDatabaseStatusChange(nodeName, databaseName, (DB_STATUS) iEvent.getValue());

        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember()) && DB_STATUS.ONLINE
            .equals(iEvent.getValue())) {
          final DB_STATUS s = getDatabaseStatus(getLocalNodeName(), databaseName);
          if (s == DB_STATUS.NOT_AVAILABLE) {
            // INSTALL THE DATABASE
            installDatabase(false, databaseName, false,
                OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());
          }
        }

      } else if (key.startsWith(CONFIG_REGISTEREDNODES)) {
        ODistributedServerLog.info(this, nodeName, eventNodeName, DIRECTION.IN, "Received updated about registered nodes");
        reloadRegisteredNodes((String) iEvent.getValue());
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
          ODistributedServerLog
              .debug(this, nodeName, null, DIRECTION.NONE, "Removed node configuration id=%s name=%s", iEvent.getMember(), nName);
          activeNodes.remove(nName);
          activeNodesNamesByUuid.remove(iEvent.getMember().getUuid());
          activeNodesUuidByName.remove(nName);
          closeRemoteServer(nName);
        }

        updateLastClusterChange();

        dumpServersStatus();

      } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
        final String dbName = key.substring(CONFIG_DATABASE_PREFIX.length());
        final ODistributedStorage stg = storages.remove(dbName);
        if (stg != null) {
          stg.close(true, false);
        }

        updateLastClusterChange();

      } else if (key.startsWith(CONFIG_DBSTATUS_PREFIX)) {
        ODistributedServerLog.debug(this, nodeName, getNodeName(iEvent.getMember()), DIRECTION.IN, "Received removed status %s=%s",
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
  public void entryEvicted(final EntryEvent<String, Object> iEvent) {
  }

  @Override
  public void mapEvicted(final MapEvent iEvent) {
  }

  @Override
  public void mapCleared(final MapEvent event) {
  }

  /**
   * Removes the node map entry.
   */
  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    try {
      updateLastClusterChange();

      if (iEvent.getMember() == null)
        return;

      final String nodeLeftName = getNodeName(iEvent.getMember());
      if (nodeLeftName == null)
        return;

      removeServer(nodeLeftName, true);

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (Throwable e) {
      OLogManager.instance()
          .error(this, "Error on removing the server '%s' (err=%s)", getNodeName(iEvent.getMember()), e.getMessage());
    }
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      updateLastClusterChange();
      final String addedNodeName = getNodeName(iEvent.getMember());
      ODistributedServerLog
          .warn(this, nodeName, null, DIRECTION.NONE, "Added new node id=%s name=%s", iEvent.getMember(), addedNodeName);

      // REMOVE THE NODE FROM AUTO REMOVAL
      autoRemovalOfServers.remove(addedNodeName);

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
  }

  @Override
  public void stateChanged(final LifecycleEvent event) {
    final LifecycleEvent.LifecycleState state = event.getState();
    if (state == LifecycleEvent.LifecycleState.MERGED) {
      ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Server merged the existent cluster");

      coordinatorServer = (String) configurationMap.getHazelcastMap().get(CONFIG_COORDINATOR);

      configurationMap.clearLocalCache();

      // TEMPORARY PATCH TO FIX HAZELCAST'S BEHAVIOUR THAT ENQUEUES THE MERGING ITEM EVENT WITH THIS AND ACTIVE NODES MAP COULD BE STILL NOT FILLED
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            // IGNORE IT
          }

          for (final String databaseName : getMessageService().getDatabases()) {
            executeInDistributedDatabaseLock(databaseName, 0, null, new OCallable<Object, OModifiableDistributedConfiguration>() {
              @Override
              public Object call(final OModifiableDistributedConfiguration cfg) {
                for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
                  final String server = entry.getKey();
                  if (!cfg.getRegisteredServers().contains(server)) {
                    if (getDatabaseStatus(server, databaseName) != DB_STATUS.OFFLINE)
                      cfg.addNewNodeInServerList(server);
                  }
                }
                return null;
              }
            });
          }
        }
      }).start();
    }
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    if (status != NODE_STATUS.ONLINE)
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {

      final String dbName = iDatabase.getName();

      final ODocument dCfg = (ODocument) configurationMap.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName);
      if (dCfg != null && getAvailableNodes(dbName) > 0) {
        throw new ODistributedException(
            "Cannot create the new database '" + dbName + "' because it is already present in distributed configuration");
      }

      // INIT THE STORAGE
      getStorage(dbName);

      final ODistributedConfiguration cfg = getDatabaseConfiguration(dbName);

      final ODistributedDatabaseImpl distribDatabase = messageService.registerDatabase(dbName, cfg);
      distribDatabase.resume();
      distribDatabase.setOnline();

      // TODO: TEMPORARY PATCH TO WAIT FOR DB PROPAGATION IN CFG TO ALL THE OTHER SERVERS
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ODistributedException("Error on creating database '" + dbName + "' on distributed nodes");
      }

      // WAIT UNTIL THE DATABASE HAS BEEN PROPAGATED TO ALL THE SERVERS
      final Set<String> servers = cfg.getAllConfiguredServers();
      if (servers.size() > 1) {
        int retry = 0;
        for (; retry < 100; ++retry) {
          boolean allServersAreOnline = true;
          for (String server : servers) {
            if (!isNodeOnline(server, dbName)) {
              allServersAreOnline = false;
              break;
            }
          }

          if (allServersAreOnline)
            break;

          // WAIT FOR ANOTHER RETRY
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ODistributedException("Error on creating database '" + dbName + "' on distributed nodes");
          }
        }

        if (retry >= 100)
          ODistributedServerLog
              .warn(this, getLocalNodeName(), null, DIRECTION.NONE, "Timeout waiting for all nodes to be up for database %s",
                  dbName);
      }

      onOpen(iDatabase);

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final String dbName = iDatabase.getName();

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Dropping database %s...", dbName);

    if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      // DROP THE DATABASE ON ALL THE SERVERS
      final ODistributedConfiguration dCfg = getDatabaseConfiguration(dbName);

      final Set<String> servers = dCfg.getAllConfiguredServers();
      servers.remove(nodeName);

      final long start = System.currentTimeMillis();

      // WAIT ALL THE SERVERS BECOME ONLINE
      boolean allServersAreOnline = false;
      while (!allServersAreOnline && System.currentTimeMillis() - start < 5000) {
        allServersAreOnline = true;
        for (String s : servers) {
          final DB_STATUS st = getDatabaseStatus(s, dbName);
          if (st == DB_STATUS.NOT_AVAILABLE || st == DB_STATUS.SYNCHRONIZING || st == DB_STATUS.BACKUP) {
            allServersAreOnline = false;
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }

      if (!servers.isEmpty() && messageService.getDatabase(dbName) != null)
        sendRequest(dbName, null, servers, new ODropDatabaseTask(), getNextMessageIdCounter(),
            ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);
    }

    super.onDrop(iDatabase);

    if (configurationMap != null) {
      configurationMap.remove(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + nodeName + "." + dbName);

      if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
        // LAST NODE HOLDING THE DATABASE, DELETE DISTRIBUTED CFG TOO
        configurationMap.remove(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName);
        configurationMap.remove(OAbstractSyncDatabaseTask.DEPLOYDB + dbName);
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Dropped last copy of database '%s', removing it from the cluster", dbName);
      }
    }
  }

  public ODocument getNodeConfigurationByUuid(final String iNodeId, final boolean useCache) {
    final ODocument doc = (ODocument) (useCache ?
        configurationMap.getLocalCachedValue(CONFIG_NODE_PREFIX + iNodeId) :
        configurationMap.get(CONFIG_NODE_PREFIX + iNodeId));

    if (doc == null)
      ODistributedServerLog.debug(this, nodeName, null, DIRECTION.OUT, "Cannot find node with id '%s'", iNodeId);

    return doc;
  }

  @Override
  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName) {
    final DB_STATUS status = (DB_STATUS) configurationMap
        .getLocalCachedValue(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
    return status != null ? status : DB_STATUS.NOT_AVAILABLE;
  }

  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName, final boolean useCache) {
    final String key = OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName;
    final DB_STATUS status = (DB_STATUS) (useCache ? configurationMap.getLocalCachedValue(key) : configurationMap.get(key));
    return status != null ? status : DB_STATUS.NOT_AVAILABLE;
  }

  @Override
  public void setDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    final String key = OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName;

    final DB_STATUS currStatus = (DB_STATUS) configurationMap.get(key);

    if (currStatus == null || currStatus != iStatus) {
      configurationMap.put(key, iStatus);
      invokeOnDatabaseStatusChange(iNode, iDatabaseName, iStatus);
    }
  }

  private void invokeOnDatabaseStatusChange(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      try {
        l.onDatabaseChangeStatus(iNode, iDatabaseName, iStatus);
      } catch (Exception e) {
        // IGNORE IT
      }

    }
  }

  protected void installNewDatabasesFromCluster() {
    if (activeNodes.size() <= 1) {
      // NO OTHER NODES WHERE ALIGN
      return;
    }

    for (Map.Entry<String, Object> entry : configurationMap.entrySet()) {
      if (entry.getKey().startsWith(CONFIG_DATABASE_PREFIX)) {
        final String databaseName = entry.getKey().substring(CONFIG_DATABASE_PREFIX.length());

        final Set<String> availableServers = getAvailableNodeNames(databaseName);
        if (availableServers.isEmpty())
          // NO NODE HAS THIS DATABASE AVAILABLE
          continue;

        final DB_STATUS currStatus = getDatabaseStatus(nodeName, databaseName);
        if (currStatus == DB_STATUS.SYNCHRONIZING || currStatus == DB_STATUS.ONLINE || currStatus == DB_STATUS.BACKUP)
          // FIX PREVIOUS STATUS OF DATABASE
          setDatabaseStatus(nodeName, databaseName, DB_STATUS.NOT_AVAILABLE);

        try {
          installDatabase(true, databaseName, false,
              OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());
        } catch (Exception e) {
          ODistributedServerLog
              .error(this, getLocalNodeName(), null, DIRECTION.IN, "Error on installing database '%s' on local node", databaseName);
          setDatabaseStatus(getLocalNodeName(), databaseName, DB_STATUS.NOT_AVAILABLE);
        }
      }
    }
  }

  public void reloadRegisteredNodes(String registeredNodesFromClusterAsJson) {
    final ODocument registeredNodesFromCluster = new ODocument();

    if (registeredNodesFromClusterAsJson == null)
      // LOAD FROM THE CLUSTER CFG
      registeredNodesFromClusterAsJson = (String) configurationMap.get(CONFIG_REGISTEREDNODES);

    if (registeredNodesFromClusterAsJson != null) {
      registeredNodesFromCluster.fromJSON(registeredNodesFromClusterAsJson);
      registeredNodeById.clear();
      registeredNodeById.addAll((Collection<? extends String>) registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST));

      registeredNodeByName.clear();
      registeredNodeByName
          .putAll((Map<? extends String, ? extends Integer>) registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP));
    } else
      throw new ODistributedException("Cannot find distributed 'registeredNodes' configuration");
  }

  private List<String> getRegisteredNodes() {
    final List<String> registeredNodes = new ArrayList<String>();

    for (Map.Entry entry : configurationMap.entrySet()) {
      if (entry.getKey().toString().startsWith(CONFIG_NODE_PREFIX))
        registeredNodes.add(entry.getKey().toString().substring(CONFIG_NODE_PREFIX.length()));
    }

    return registeredNodes;
  }

  public void removeNodeFromConfiguration(final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Removing server '%s' from all the databases (removeOnlyDynamicServers=%s)...", nodeLeftName, removeOnlyDynamicServers);

    for (String dbName : getManagedDatabases()) {
      removeNodeFromConfiguration(nodeLeftName, dbName, removeOnlyDynamicServers, false);
    }
  }

  public boolean removeNodeFromConfiguration(final String nodeLeftName, final String databaseName,
      final boolean removeOnlyDynamicServers, final boolean statusOffline) {
    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Removing server '%s' from database configuration '%s' (removeOnlyDynamicServers=%s)...", nodeLeftName, databaseName,
        removeOnlyDynamicServers);

    final OModifiableDistributedConfiguration cfg = getDatabaseConfiguration(databaseName).modify();

    if (removeOnlyDynamicServers) {
      // CHECK THE SERVER IS NOT REGISTERED STATICALLY
      final String dc = cfg.getDataCenterOfServer(nodeLeftName);
      if (dc != null) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in data center '%s' configuration for database '%s'", nodeLeftName,
            dc, databaseName);
        return false;
      }

      // CHECK THE SERVER IS NOT REGISTERED IN SERVERS
      final Set<String> registeredServers = cfg.getRegisteredServers();
      if (registeredServers.contains(nodeLeftName)) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in 'servers' of the distributed configuration for database '%s'",
            nodeLeftName, databaseName);
        return false;
      }
    }

    final boolean found = executeInDistributedDatabaseLock(databaseName, 0, cfg,
        new OCallable<Boolean, OModifiableDistributedConfiguration>() {
          @Override
          public Boolean call(OModifiableDistributedConfiguration cfg) {
            return cfg.removeServer(nodeLeftName) != null;
          }
        });

    final DB_STATUS nodeLeftStatus = getDatabaseStatus(nodeLeftName, databaseName);
    if (statusOffline && nodeLeftStatus != DB_STATUS.OFFLINE)
      setDatabaseStatus(nodeLeftName, databaseName, DB_STATUS.OFFLINE);
    else if (!statusOffline && nodeLeftStatus != DB_STATUS.NOT_AVAILABLE)
      setDatabaseStatus(nodeLeftName, databaseName, DB_STATUS.NOT_AVAILABLE);

    return found;
  }

  @Override
  public synchronized void removeServer(final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    if (nodeLeftName == null)
      return;

    final Member member = activeNodes.remove(nodeLeftName);
    if (member == null)
      return;

    ODistributedServerLog
        .debug(this, nodeName, nodeLeftName, DIRECTION.NONE, "Distributed server '%s' is unreachable", nodeLeftName);

    try {
      // REMOVE INTRA SERVER CONNECTION
      closeRemoteServer(nodeLeftName);

      // NOTIFY ABOUT THE NODE HAS LEFT
      for (ODistributedLifecycleListener l : listeners)
        try {
          l.onNodeLeft(nodeLeftName);
        } catch (Exception e) {
          // IGNORE IT
        }

      if (nodeLeftName.equals(coordinatorServer))
        electNewCoordinator();

      getLockManagerExecutor().handleUnreachableServer(nodeLeftName);
      getLockManagerRequester().handleUnreachableServer(nodeLeftName);

      // UNLOCK ANY PENDING LOCKS
      if (messageService != null) {
        for (String dbName : messageService.getDatabases())
          messageService.getDatabase(dbName).handleUnreachableNode(nodeLeftName);
      }

      if (member.getUuid() != null)
        activeNodesNamesByUuid.remove(member.getUuid());
      activeNodesUuidByName.remove(nodeLeftName);

      if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
        return;

      final long autoRemoveOffLineServer = OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS.getValueAsLong();
      if (autoRemoveOffLineServer == 0)
        // REMOVE THE NODE RIGHT NOW
        removeNodeFromConfiguration(nodeLeftName, removeOnlyDynamicServers);
      else if (autoRemoveOffLineServer > 0) {
        // SCHEDULE AUTO REMOVAL IN A WHILE
        autoRemovalOfServers.put(nodeLeftName, System.currentTimeMillis());
        Orient.instance().scheduleTask(new TimerTask() {
          @Override
          public void run() {
            try {
              final Long lastTimeNodeLeft = autoRemovalOfServers.get(nodeLeftName);
              if (lastTimeNodeLeft == null)
                // NODE WAS BACK ONLINE
                return;

              if (System.currentTimeMillis() - lastTimeNodeLeft >= autoRemoveOffLineServer) {
                removeNodeFromConfiguration(nodeLeftName, removeOnlyDynamicServers);
              }
            } catch (Exception e) {
              // IGNORE IT
            }
          }
        }, autoRemoveOffLineServer, 0);
      }

      for (String databaseName : getManagedDatabases()) {
        final DB_STATUS nodeLeftStatus = getDatabaseStatus(nodeLeftName, databaseName);
        if (nodeLeftStatus != DB_STATUS.OFFLINE && nodeLeftStatus != DB_STATUS.NOT_AVAILABLE)
          configurationMap.put(CONFIG_DBSTATUS_PREFIX + nodeLeftName + "." + databaseName, DB_STATUS.NOT_AVAILABLE);
      }

      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Node removed id=%s name=%s", member, nodeLeftName);

      if (nodeLeftName.startsWith("ext:")) {
        final List<String> registeredNodes = getRegisteredNodes();

        ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
            "Removed node id=%s name=%s has not being recognized. Remove the node manually (registeredNodes=%s)", member,
            nodeLeftName, registeredNodes);
      }

      for (String databaseName : getManagedDatabases()) {
        try {
          reassignClustersOwnership(nodeName, databaseName, null);
        } catch (Exception e) {
          // IGNORE IT
          ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
              "Cannot re-balance the cluster for database '%s' because the coordinator is not available (err=%s)", databaseName,
              e.getMessage());
        }
      }

      if (nodeLeftName.equalsIgnoreCase(nodeName))
        // CURRENT NODE: EXIT
        System.exit(1);

    } finally {
      // REMOVE NODE IN DB CFG
      if (messageService != null)
        messageService.handleUnreachableNode(nodeLeftName);
    }
  }

  /**
   * Elects a new server as coordinator. The election browse the ordered server list.
   */
  private synchronized void electNewCoordinator() {
    // TRY ALL THE SERVERS IN ORDER (ALL THE SERVERS HAVE THE SAME LIST)
    final List<String> sortedServers = new ArrayList<String>(getActiveServers());
    Collections.sort(sortedServers);

    int coordinatorServerId = -1;
    if (registeredNodeByName.containsKey(coordinatorServer))
      coordinatorServerId = registeredNodeByName.get(coordinatorServer);

    final String originalCoordinator = coordinatorServer;

    int currIndex = coordinatorServerId;
    for (int i = 0; i < registeredNodeById.size(); ++i) {
      currIndex++;
      if (currIndex >= registeredNodeById.size())
        // RESTART FROM THE FIRST
        currIndex = 0;

      final String newServer = registeredNodeById.get(currIndex);
      if (activeNodes.containsKey(newServer)) {
        // TODO: IMPROVE ELECTION BY CHECKING AL THE NODES AGREE ON IT

        coordinatorServer = newServer;
        getLockManagerRequester().setCoordinatorServer(coordinatorServer);

        // TRY TO ACQUIRE A LOCK AGAINST THE NEW COORDINATOR TO VALIDATE IT
        try {
          executeInDistributedDatabaseLock(OSystemDatabase.SYSTEM_DB_NAME, 10000, null,
              new OCallable<Object, OModifiableDistributedConfiguration>() {
                @Override
                public Object call(OModifiableDistributedConfiguration iArgument) {
                  ODistributedServerLog
                      .info(this, nodeName, null, DIRECTION.NONE, "Elected server '%s' as new coordinator (old=%s)", newServer,
                          originalCoordinator);

                  configurationMap.put(CONFIG_COORDINATOR, newServer);
                  return null;
                }
              });

          break;

        } catch (ODistributedOperationException e) {
          // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED, GO AHEAD WITH THE NEXT IN THE LIST
          ODistributedServerLog
              .info(this, nodeName, null, DIRECTION.NONE, "Cannot elect server '%s' because is offline", newServer);
        }
      }
    }
  }

  @Override
  public Set<String> getActiveServers() {
    return activeNodes.keySet();
  }

  @Override
  public void onBeforeDatabaseOpen(final String url) {
    final ODistributedDatabaseImpl dDatabase = getMessageService().getDatabase(OUtils.getDatabaseNameFromURL(url));
    if (dDatabase != null)
      dDatabase.waitForOnline();
  }

  protected void registerNode(final Member member, final String joinedNodeName) {
    if (activeNodes.containsKey(joinedNodeName))
      // ALREADY REGISTERED: SKIP IT
      return;

    if (joinedNodeName.startsWith("ext:"))
      // NODE HAS NOT IS YET
      return;

    // NOTIFY NODE IS GOING TO BE ADDED. EVERYBODY IS OK?
    for (ODistributedLifecycleListener l : listeners) {
      if (!l.onNodeJoining(joinedNodeName)) {
        // DENY JOIN
        ODistributedServerLog
            .info(this, nodeName, getNodeName(member), DIRECTION.IN, "Denied node to join the cluster id=%s name=%s", member,
                getNodeName(member));
        return;
      }
    }

    activeNodes.put(joinedNodeName, member);
    activeNodesNamesByUuid.put(member.getUuid(), joinedNodeName);
    activeNodesUuidByName.put(joinedNodeName, member.getUuid());

    try {
      getRemoteServer(joinedNodeName);
    } catch (IOException e) {
      ODistributedServerLog.error(this, nodeName, joinedNodeName, DIRECTION.OUT, "Error on connecting to node %s", joinedNodeName);
    }

    ODistributedServerLog.info(this, nodeName, getNodeName(member), DIRECTION.IN,
        "Added node configuration id=%s name=%s, now %d nodes are configured", member, getNodeName(member), activeNodes.size());

    // NOTIFY NODE WAS ADDED SUCCESSFULLY
    for (ODistributedLifecycleListener l : listeners)
      l.onNodeJoined(joinedNodeName);

    // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN
    for (String db : messageService.getDatabases())
      if (getDatabaseStatus(joinedNodeName, db) == DB_STATUS.ONLINE)
        setDatabaseStatus(joinedNodeName, db, DB_STATUS.NOT_AVAILABLE);

    dumpServersStatus();
  }
}
