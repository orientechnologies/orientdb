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
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedMessageServiceImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation for clustering.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener, EntryListener<String, Object> {

  public static final String           CONFIG_DATABASE_PREFIX = "database.";

  protected static final String        CONFIG_NODE_PREFIX     = "node.";
  protected static final String        CONFIG_DBSTATUS_PREFIX = "dbstatus.";
  protected static final String        CONFIG_REGISTEREDNODES = "registeredNodes";

  protected String                     hazelcastConfigFile    = "hazelcast.xml";
  protected String                     membershipListenerRegistration;
  protected String                     membershipListenerMapRegistration;
  protected volatile HazelcastInstance hazelcastInstance;

  // THIS MAP IS BACKED BY HAZELCAST EVENTS. IN THIS WAY WE AVOID TO USE HZ MAP DIRECTLY
  protected OHazelcastDistributedMap   configurationMap;
  protected ODistributedDatabaseRepair repair;

  public OHazelcastPlugin() {
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
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    serverInstance.addTemporaryUser(REPLICATOR_USER, "" + new SecureRandom().nextLong(), "*");

    super.startup();

    status = NODE_STATUS.STARTING;

    final String localNodeName = nodeName;

    activeNodes.clear();
    activeNodesNamesByMemberId.clear();

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    registeredNodeById = null;
    registeredNodeByName = null;

    try {
      hazelcastInstance = configureHazelcast();

      nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

      OLogManager.instance().info(this, "Starting distributed server '%s' (hzID=%s)...", localNodeName, nodeUuid);

      activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());
      activeNodesNamesByMemberId.put(nodeUuid, localNodeName);

      configurationMap = new OHazelcastDistributedMap(hazelcastInstance);

      OServer.registerServerInstance(localNodeName, serverInstance);

      // REGISTER CURRENT NODES
      for (Member m : hazelcastInstance.getCluster().getMembers()) {
        if (!m.getUuid().equals(nodeUuid)) {
          final String memberName = getNodeName(m);
          if (memberName != null && !memberName.startsWith("ext:")) {
            activeNodes.put(memberName, m);
            activeNodesNamesByMemberId.put(m.getUuid(), memberName);
          } else if (!m.equals(hazelcastInstance.getCluster().getLocalMember()))
            ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.NONE, "Cannot find configuration for member: %s", m);
        }
      }

      final Lock lock = getLock("registeredNodes");
      lock.lock();
      try {
        final ODocument registeredNodesFromCluster = new ODocument();
        final String registeredNodesFromClusterAsJson = (String) configurationMap.get(CONFIG_REGISTEREDNODES);

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
        configurationMap.put(CONFIG_REGISTEREDNODES, registeredNodesFromCluster.toJSON());

      } finally {
        lock.unlock();
      }

      messageService = new ODistributedMessageServiceImpl(this);

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

      repair = new ODistributedDatabaseRepair(this);
      repair.start();

      installNewDatabasesFromCluster(true);

      loadLocalDatabases();

      membershipListenerMapRegistration = configurationMap.getHazelcastMap().addEntryListener(this, true);
      membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

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

      final long healthChecker = OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_EVERY.getValueAsLong();
      if (healthChecker > 0) {
        healthCheckerTask = new OClusterHealthChecker(this);
        Orient.instance().scheduleTask(healthCheckerTask, healthChecker, healthChecker);
      }

      // WAIT ALL THE MESSAGES IN QUEUE ARE PROCESSED OR MAX 10 SECONDS
      waitStartupIsCompleted();

    } catch (Exception e) {
      ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE, "Error on starting distributed plugin", e);
      throw OException.wrapException(new ODistributedStartupException("Error on starting distributed plugin"), e);
    }

    dumpServersStatus();
  }

  protected void waitStartupIsCompleted() throws InterruptedException {
    long totalReceivedRequests = getMessageService().getReceivedRequests();
    long totalProcessedRequests = getMessageService().getProcessedRequests();

    final long start = System.currentTimeMillis();
    while (totalProcessedRequests < totalReceivedRequests - 2 && (System.currentTimeMillis() - start < 10000)) {
      Thread.sleep(300);
      totalProcessedRequests = getMessageService().getProcessedRequests();
      totalReceivedRequests = getMessageService().getReceivedRequests();
    }

    // WAIT FOR THE COMPLETION OF ALL THE REQUESTS
    Thread.sleep(OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsInteger());

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
  public void repairRecord(final String databaseName, final ORecordId rid) {
    try {
      repair.repair(databaseName, rid);
    } catch (IllegalStateException e) {
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
          "Error on adding record %s to the check queue because it is full");
    }
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OLogManager.instance().warn(this, "Shutting down node '%s'...", nodeName);
    setNodeStatus(NODE_STATUS.SHUTTINGDOWN);

    try {
      final Set<String> databases = new HashSet<String>();

      for (Map.Entry<String, Object> entry : configurationMap.entrySet()) {
        if (entry.getKey().toString().startsWith(CONFIG_DBSTATUS_PREFIX)) {

          final String nodeDb = entry.getKey().toString().substring(CONFIG_DBSTATUS_PREFIX.length());

          if (nodeDb.startsWith(nodeName))
            databases.add(entry.getKey());
        }
      }

      // PUT DATABASES OFFLINE
      for (String k : databases)
        configurationMap.put(k, DB_STATUS.OFFLINE);
    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }

    super.shutdown();

    if (repair != null)
      repair.sendShutdown();

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

    setNodeStatus(NODE_STATUS.OFFLINE);
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
    FileSystemXmlConfig config = new FileSystemXmlConfig(hazelcastConfigFile);
    config.setClassLoader(this.getClass().getClassLoader());
    return Hazelcast.newHazelcastInstance(config);
  }

  @Override
  protected ODocument loadDatabaseConfiguration(final String iDatabaseName, final File file, final boolean saveCfgToDisk) {
    // FIRST LOOK IN THE CLUSTER
    if (hazelcastInstance != null) {
      final ODocument cfg = (ODocument) configurationMap.getLocalCachedValue(CONFIG_DATABASE_PREFIX + iDatabaseName);
      if (cfg != null) {
        ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Loaded database configuration from the active cluster");
        updateCachedDatabaseConfiguration(iDatabaseName, cfg, false, false);
        return cfg;
      }
    }

    // NO NODE IN CLUSTER, LOAD FROM FILE
    return super.loadDatabaseConfiguration(iDatabaseName, file, saveCfgToDisk);
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
        getStorage(databaseName);

        executeInDistributedDatabaseLock(databaseName, 0, new OCallable<Object, ODistributedConfiguration>() {
          @Override
          public Object call(ODistributedConfiguration cfg) {
            ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Current node started as %s for database '%s'",
                cfg.getServerRole(nodeName), databaseName);

            if (!configurationMap.containsKey(CONFIG_DATABASE_PREFIX + databaseName))
              // PUBLISH CFG THE FIRST TIME
              updateCachedDatabaseConfiguration(databaseName, cfg.getDocument(), false, true);

            final ODistributedDatabaseImpl ddb = messageService.registerDatabase(databaseName);

            // 1ST NODE TO HAVE THE DATABASE
            cfg.addNewNodeInServerList(nodeName);

            final Set<String> clustersWithNotAvailableOwner = new HashSet<String>();

            // COLLECT ALL THE CLUSTERS OWNED BY OFFLINE SERVERS (ALL BUT CURRENT ONE BECAUSE IT'S 1ST RUN)
            final Set<String> servers = cfg.getAllConfiguredServers();
            for (String server : servers) {
              if (!nodeName.equals(server)) {
                clustersWithNotAvailableOwner.addAll(cfg.getClustersOwnedByServer(server));
              }
            }

            // COLLECT ALL THE CLUSTERS WITH REMOVED NODE AS OWNER
            if (reassignClustersOwnership(nodeName, databaseName, clustersWithNotAvailableOwner, false))
              updateCachedDatabaseConfiguration(databaseName, cfg.getDocument(), true, true);

            ddb.setOnline();
            return null;
          }
        });
      }
    }
  }

  @Override
  public ConcurrentMap<String, Object> getConfigurationMap() {
    return configurationMap;
  }

  @Override
  protected ODistributedConfiguration getLastDatabaseConfiguration(final String databaseName) {
    ODocument distributedCfg = (ODocument) configurationMap.get(CONFIG_DATABASE_PREFIX + databaseName);
    if (distributedCfg != null)
      return new ODistributedConfiguration(distributedCfg);

    // NOT AVAILABLE YET (STARTUP PHASE), USE THE LOCAL ONE
    return getDatabaseConfiguration(databaseName);
  }

  public Lock getLock(final String iName) {
    return getHazelcastInstance().getLock("orientdb." + iName);
  }

  @Override
  public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {
  }

  public void updateCachedDatabaseConfiguration(final String iDatabaseName, final ODocument cfg, final boolean iSaveToDisk,
      final boolean iDeployToCluster) {
    // VALIDATE THE CONFIGURATION FIRST
    final ODistributedConfiguration dCfg = new ODistributedConfiguration(cfg);
    getDistributedStrategy().validateConfiguration(dCfg);

    final boolean updated = super.updateCachedDatabaseConfiguration(iDatabaseName, cfg, iSaveToDisk);

    if (updated) {
      if (iDeployToCluster) {
        ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentTx.getDefaultSerializer());
        configurationMap.put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabaseName, cfg);
      } else
        configurationMap.putInLocalCache(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + iDatabaseName, cfg);
    }
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
          activeNodesNamesByMemberId.put(iEvent.getMember().getUuid(), joinedNodeName);
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

          dumpServersStatus();
        }

      } else if (key.startsWith(CONFIG_DATABASE_PREFIX)) {
        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
          // SYNCHRONIZE ADDING OF CLUSTERS TO AVOID DEADLOCKS
          final String databaseName = key.substring(CONFIG_DATABASE_PREFIX.length());

          onDatabaseEvent((ODocument) iEvent.getValue(), databaseName);
        }
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
        activeNodesNamesByMemberId.put(iEvent.getMember().getUuid(), (String) cfg.field("name"));

        dumpServersStatus();

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
          activeNodesNamesByMemberId.remove(iEvent.getMember().getUuid());
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

      removeServer(nodeLeftName);

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
      return;

    try {
      updateLastClusterChange();
      final String addedNodeName = getNodeName(iEvent.getMember());
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Added new node id=%s name=%s", iEvent.getMember(),
          addedNodeName);

      // REMOVE THE NODE FROM AUTO REMOVAL
      autoRemovalOfServers.remove(addedNodeName);

    } catch (HazelcastInstanceNotActiveException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    } catch (RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running");
    }
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final ODatabaseDocumentInternal currDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {

      final String dbName = iDatabase.getName();

      if (configurationMap.containsKey(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName))
        throw new ODistributedException(
            "Cannot create the new database '" + dbName + "' because it is already present in distributed configuration");

      // INIT THE STORAGE
      getStorage(dbName);

      final ODistributedDatabaseImpl distribDatabase = messageService.registerDatabase(dbName);
      distribDatabase.setOnline();

      // TODO: TEMPORARY PATCH TO WAIT FOR DB PROPAGATION
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ODistributedException("Error on creating database '" + dbName + "' on distributed nodes");
      }

      // WAIT UNTIL THE DATABASE HAS BEEN PROPAGATED IN ALL THE NODES
      final ODistributedConfiguration cfg = getDatabaseConfiguration(dbName);

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
          ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
              "Timeout waiting for all nodes to be up for database %s", dbName);
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

    super.onDrop(iDatabase);

    if (configurationMap != null) {
      configurationMap.remove(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + nodeName + "." + dbName);

      final int availableNodes = getAvailableNodes(dbName);
      if (availableNodes == 0) {
        // LAST NODE HOLDING THE DATABASE, DELETE DISTRIBUTED CFG TOO
        configurationMap.remove(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + dbName);
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Dropped last copy of database '%s', removing it from the cluster", dbName);
      }
    }
  }

  public ODocument getNodeConfigurationByUuid(final String iNodeId, final boolean useCache) {
    final ODocument doc = (ODocument) (useCache ? configurationMap.getLocalCachedValue(CONFIG_NODE_PREFIX + iNodeId)
        : configurationMap.get(CONFIG_NODE_PREFIX + iNodeId));

    if (doc == null)
      ODistributedServerLog.debug(this, nodeName, null, DIRECTION.OUT, "Cannot find node with id '%s'", iNodeId);

    return doc;
  }

  @Override
  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName) {
    final DB_STATUS status = (DB_STATUS) configurationMap
        .getLocalCachedValue(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
    return status != null ? status : DB_STATUS.OFFLINE;
  }

  @Override
  public void setDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    configurationMap.put(OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName, iStatus);

    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      l.onDatabaseChangeStatus(iNode, iDatabaseName, iStatus);
    }
  }

  protected void installNewDatabasesFromCluster(final boolean iStartup) {
    if (activeNodes.size() <= 1) {
      // NO OTHER NODES WHERE ALIGN
      return;
    }

    for (Map.Entry<String, Object> entry : configurationMap.entrySet()) {
      if (entry.getKey().startsWith(CONFIG_DATABASE_PREFIX)) {
        final String databaseName = entry.getKey().substring(CONFIG_DATABASE_PREFIX.length());
        installDatabase(iStartup, databaseName, (ODocument) entry.getValue());
      }
    }
  }

  private void reloadRegisteredNodes() {
    final ODocument registeredNodesFromCluster = new ODocument();
    final String registeredNodesFromClusterAsJson = (String) configurationMap.getHazelcastMap().get(CONFIG_REGISTEREDNODES);

    if (registeredNodesFromClusterAsJson != null) {
      registeredNodesFromCluster.fromJSON(registeredNodesFromClusterAsJson);
      registeredNodeById = registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST);
      registeredNodeByName = registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP);
    } else
      throw new ODistributedException("Cannot find distributed registeredNodes configuration");

  }

  private List<String> getRegisteredNodes() {
    final List<String> registeredNodes = new ArrayList<String>();

    for (Map.Entry entry : configurationMap.entrySet()) {
      if (entry.getKey().toString().startsWith(CONFIG_NODE_PREFIX))
        registeredNodes.add(entry.getKey().toString().substring(CONFIG_NODE_PREFIX.length()));
    }

    return registeredNodes;
  }

  public void removeNodeFromConfiguration(final String nodeLeftName) {
    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Removing server '%s' from all the databases...",
        nodeLeftName);

    for (String dbName : getManagedDatabases()) {
      removeNodeFromConfiguration(nodeLeftName, dbName);
    }
  }

  public boolean removeNodeFromConfiguration(final String nodeLeftName, final String databaseName) {
    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Removing server '%s' from database configuration '%s'...", nodeLeftName, databaseName);

    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    final boolean found = cfg.removeServer(nodeLeftName) != null;

    if (found)
      // SERVER REMOVED CORRECTLY
      updateCachedDatabaseConfiguration(databaseName, cfg.getDocument(), true, true);

    configurationMap.remove(CONFIG_DBSTATUS_PREFIX + nodeLeftName + "." + databaseName);

    return found;
  }

  @Override
  public synchronized void removeServer(final String nodeLeftName) {
    if (nodeLeftName == null)
      return;

    ODistributedServerLog.debug(this, nodeName, nodeLeftName, DIRECTION.NONE, "Distributed server is '%s' unreachable",
        nodeLeftName);

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

      // UNLOCK ANY PENDING LOCKS
      if (messageService != null) {
        final int nodeLeftId = getNodeIdByName(nodeLeftName);
        for (String dbName : messageService.getDatabases())
          messageService.getDatabase(dbName).handleUnreachableNode(nodeLeftId);
      }

      final Member member = activeNodes.remove(nodeLeftName);
      if (member != null)
        activeNodesNamesByMemberId.remove(member.getUuid());

      if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
        return;

      final long autoRemoveOffLineServer = OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS.getValueAsLong();
      if (autoRemoveOffLineServer == 0)
        // REMOVE THE NODE RIGHT NOW
        removeNodeFromConfiguration(nodeLeftName);
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
                removeNodeFromConfiguration(nodeLeftName);
              }
            } catch (Exception e) {
              // IGNORE IT
            }
          }
        }, autoRemoveOffLineServer, 0);
      }

      for (String databaseName : getManagedDatabases()) {
        configurationMap.remove(CONFIG_DBSTATUS_PREFIX + nodeLeftName + "." + databaseName);
      }

      ODistributedServerLog.warn(this, nodeLeftName, null, DIRECTION.NONE, "Node removed id=%s name=%s", member, nodeLeftName);

      if (nodeLeftName.startsWith("ext:")) {
        final List<String> registeredNodes = getRegisteredNodes();

        ODistributedServerLog.error(this, nodeName, null, DIRECTION.NONE,
            "Removed node id=%s name=%s has not being recognized. Remove the node manually (registeredNodes=%s)", member,
            nodeLeftName, registeredNodes);
      }

      for (String databaseName : getManagedDatabases()) {
        final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

        // COLLECT ALL THE CLUSTERS WITH REMOVED NODE AS OWNER
        final Set<String> clustersWithNotAvailableOwner = cfg.getClustersOwnedByServer(nodeLeftName);
        if (reassignClustersOwnership(nodeName, databaseName, clustersWithNotAvailableOwner, false))
          updateCachedDatabaseConfiguration(databaseName, cfg.getDocument(), true, true);

      }
    } finally {
      // REMOVE NODE IN DB CFG
      if (messageService != null)
        messageService.handleUnreachableNode(nodeLeftName);
    }
  }
}
