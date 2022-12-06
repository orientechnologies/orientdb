package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCallableNoParamNoReturn;
import com.orientechnologies.common.util.OCallableUtils;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncDatabaseTask;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class OHazelcastClusterMetadataManager
    implements MembershipListener, EntryListener<String, Object>, LifecycleListener {

  public static final String CONFIG_DATABASE_PREFIX = "database.";
  public static final String CONFIG_NODE_PREFIX = "node.";
  public static final String CONFIG_DBSTATUS_PREFIX = "dbstatus.";
  public static final String CONFIG_REGISTEREDNODES = "registeredNodes";

  protected String hazelcastConfigFile = "hazelcast.xml";
  protected Config hazelcastConfig;
  protected String membershipListenerRegistration;
  protected String membershipListenerMapRegistration;
  protected volatile HazelcastInstance hazelcastInstance;

  // THIS MAP IS BACKED BY HAZELCAST EVENTS. IN THIS WAY WE AVOID TO USE HZ MAP DIRECTLY
  protected OHazelcastDistributedMap configurationMap;
  private ODistributedLockManager distributedLockManager;

  protected ConcurrentMap<String, Member> activeNodes = new ConcurrentHashMap<>();
  protected ConcurrentMap<String, String> activeNodesNamesByUuid = new ConcurrentHashMap<>();
  protected ConcurrentMap<String, String> activeNodesUuidByName = new ConcurrentHashMap<>();
  protected final List<String> registeredNodeById = new CopyOnWriteArrayList<>();
  protected final ConcurrentMap<String, Integer> registeredNodeByName = new ConcurrentHashMap<>();
  protected ConcurrentMap<String, Long> autoRemovalOfServers = new ConcurrentHashMap<>();

  protected TimerTask publishLocalNodeConfigurationTask = null;

  protected volatile ODistributedServerManager.NODE_STATUS status =
      ODistributedServerManager.NODE_STATUS.OFFLINE;

  protected long lastClusterChangeOn;
  private String nodeUuid;
  private int nodeId = -1;

  private String nodeName = null;
  private OServer serverInstance;

  private final ODistributedPlugin distributedPlugin;

  public OHazelcastClusterMetadataManager(ODistributedPlugin distributedPlugin) {
    this.distributedPlugin = distributedPlugin;
  }

  public void configHazelcastPlugin(
      OServer server, OServerParameterConfiguration[] params, String nodeName) {
    this.nodeName = nodeName;
    this.serverInstance = server;
    for (OServerParameterConfiguration param : params) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast")) {
        hazelcastConfigFile = OSystemVariableResolver.resolveSystemVariables(param.value);
        hazelcastConfigFile = OFileUtils.getPath(hazelcastConfigFile);
      }
    }
  }

  public void startupHazelcastPlugin() throws IOException, InterruptedException {
    status = ODistributedServerManager.NODE_STATUS.STARTING;

    final String localNodeName = nodeName;

    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    registeredNodeById.clear();
    registeredNodeByName.clear();

    hazelcastInstance = configureHazelcast();
    distributedLockManager = new OHazelcastLockManager(this.hazelcastInstance);

    nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

    final LifecycleService lifecycleService = hazelcastInstance.getLifecycleService();
    lifecycleService.addLifecycleListener(this);

    OLogManager.instance()
        .info(this, "Starting distributed server '%s' (hzID=%s)...", localNodeName, nodeUuid);

    final long clusterTime = getClusterTime();
    final long deltaTime = System.currentTimeMillis() - clusterTime;
    OLogManager.instance()
        .info(
            this,
            "Distributed cluster time=%s (delta from local node=%d)...",
            new Date(clusterTime),
            deltaTime);

    activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());
    activeNodesNamesByUuid.put(nodeUuid, localNodeName);
    activeNodesUuidByName.put(localNodeName, nodeUuid);

    configurationMap = new OHazelcastDistributedMap(this, hazelcastInstance);

    OServer.registerServerInstance(localNodeName, serverInstance);

    initRegisteredNodeIds();

    // PUBLISH CURRENT NODE NAME
    final ODocument nodeCfg = new ODocument();
    nodeCfg.setTrackingChanges(false);

    // REMOVE ANY PREVIOUS REGISTERED SERVER WITH THE SAME NODE NAME
    final Set<String> node2Remove = new HashSet<String>();

    for (String nodeUUid : configurationMap.getNodes()) {
      final ODocument nCfg = configurationMap.getNodeConfig(nodeUUid);
      if (nodeName.equals(nCfg.field("name"))) {
        // SAME NODE NAME: REMOVE IT
        node2Remove.add(nodeUUid);
      }
    }

    for (String n : node2Remove) configurationMap.removeNode(n);

    nodeCfg.field("id", nodeId);
    nodeCfg.field("uuid", nodeUuid);
    nodeCfg.field("name", nodeName);
    ORecordInternal.setRecordSerializer(nodeCfg, ODatabaseDocumentAbstract.getDefaultSerializer());
    configurationMap.putNodeConfig(nodeUuid, nodeCfg);

    // REGISTER CURRENT NODES
    for (Member m : hazelcastInstance.getCluster().getMembers()) {
      if (!m.getUuid().equals(nodeUuid)) {
        boolean found = false;
        for (int retry = 0; retry < 10; ++retry) {
          final String memberName = getNodeName(m, false);

          if (memberName == null || memberName.startsWith("ext:")) {
            // ACTIVE NODE IN HZ, BUT NOT YET REGISTERED, WAIT AND RETRY
            Thread.sleep(1000);
            continue;
          }

          found = true;
          activeNodes.put(memberName, m);
          activeNodesNamesByUuid.put(m.getUuid(), memberName);
          activeNodesUuidByName.put(memberName, m.getUuid());

          break;
        }

        if (!found)
          ODistributedServerLog.warn(
              this,
              localNodeName,
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "Cannot find configuration for member: %s, uuid",
              m,
              m.getUuid());
      }
    }

    ODistributedServerLog.info(
        this,
        localNodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Servers in cluster: %s",
        activeNodes.keySet());

    publishLocalNodeConfiguration();

    if (!configurationMap.existsNode(nodeUuid)) {
      // NODE NOT REGISTERED, FORCING SHUTTING DOWN
      ODistributedServerLog.error(
          this,
          localNodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on registering local node on cluster");
      throw new ODistributedStartupException("Error on registering local node on cluster");
    }

    // CONNECTS TO ALL THE AVAILABLE NODES
    distributedPlugin.connectToAllNodes(activeNodes.keySet());

    publishLocalNodeConfiguration();

    new Thread(
            () -> {
              distributedPlugin.installNewDatabasesFromCluster();
              distributedPlugin.loadLocalDatabases();
              distributedPlugin.notifyStarted();
            })
        .start();

    membershipListenerMapRegistration =
        configurationMap.getHazelcastMap().addEntryListener(this, true);
    membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);

    // REGISTER CURRENT MEMBERS
    setNodeStatus(ODistributedServerManager.NODE_STATUS.ONLINE);

    publishLocalNodeConfiguration();

    final long delay = OGlobalConfiguration.DISTRIBUTED_PUBLISH_NODE_STATUS_EVERY.getValueAsLong();
    if (delay > 0) {
      publishLocalNodeConfigurationTask =
          Orient.instance().scheduleTask(this::publishLocalNodeConfiguration, delay, delay);
    }
  }

  private void initRegisteredNodeIds() {
    distributedLockManager.acquireExclusiveLock(
        "orientdb." + CONFIG_REGISTEREDNODES, getLocalNodeName(), 0);
    try {
      // RE-CREATE THE CFG IN LOCK
      registeredNodeById.clear();
      registeredNodeByName.clear();

      final ODocument registeredNodesFromCluster = configurationMap.getRegisteredNodes();

      if (registeredNodesFromCluster.hasProperty("ids")
          && registeredNodesFromCluster.hasProperty("names")) {
        registeredNodeById.addAll(registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST));
        registeredNodeByName.putAll(registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP));

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

      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Registered local server with nodeId=%d",
          nodeId);

      registeredNodesFromCluster.field("ids", registeredNodeById, OType.EMBEDDEDLIST);
      registeredNodesFromCluster.field("names", registeredNodeByName, OType.EMBEDDEDMAP);

      configurationMap.putRegisteredNodes(registeredNodesFromCluster);

    } finally {
      distributedLockManager.releaseExclusiveLock(
          "orientdb." + CONFIG_REGISTEREDNODES, getLocalNodeName());
    }

    if (nodeId == -1)
      throw new OConfigurationException(
          "Cannot join the cluster (nodeId=-1). Please restart the server.");
  }

  private void repairActiveServers() {
    ODistributedServerLog.warn(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Error on retrieving '%s' from cluster configuration. Repairing the configuration...",
        CONFIG_REGISTEREDNODES);

    final Set<Member> members = hazelcastInstance.getCluster().getMembers();

    for (Member m : members) {
      final ODocument node = configurationMap.getNodeConfig(m.getUuid());
      if (node != null) {
        final String mName = node.field("name");
        final Integer mId = node.field("id");

        if (mId == null) {
          ODistributedServerLog.warn(
              this,
              nodeName,
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "Found server '%s' with a NULL id",
              mName);
          continue;
        } else if (mId < 0) {
          ODistributedServerLog.warn(
              this,
              nodeName,
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "Found server '%s' with an invalid id %d",
              mName,
              mId);
          continue;
        }

        if (nodeName.equals(mName)) {
          nodeId = mId;
        }

        if (mId >= registeredNodeById.size()) {
          // CREATE EMPTY ENTRIES IF NEEDED
          while (mId > registeredNodeById.size()) {
            registeredNodeById.add(null);
          }
          registeredNodeById.add(mName);
        } else registeredNodeById.set(mId, mName);

        registeredNodeByName.put(mName, mId);
      }
    }

    ODistributedServerLog.warn(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Repairing of '%s' completed, registered %d servers",
        CONFIG_REGISTEREDNODES,
        members.size());
  }

  public boolean isWriteQuorumPresent(final String databaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);
    if (cfg != null) {
      final int availableServers = getAvailableNodes(databaseName);
      if (availableServers == 0) return false;

      final int quorum = cfg.getWriteQuorum(null, cfg.getMasterServers().size(), nodeName);
      return availableServers >= quorum;
    }
    return false;
  }

  public int getNodeIdByName(final String name) {
    int id = tryGetNodeIdByName(name);
    if (name == null) {
      repairActiveServers();
      id = tryGetNodeIdByName(name);
    }
    return id;
  }

  public String getNodeNameById(final int id) {
    String name = tryGetNodeNameById(id);
    if (name == null) {
      repairActiveServers();
      name = tryGetNodeNameById(id);
    }
    return name;
  }

  public boolean isNodeAvailable(final String iNodeName) {
    if (iNodeName == null) return false;
    Member member = activeNodes.get(iNodeName);
    return member != null && hazelcastInstance.getCluster().getMembers().contains(member);
  }

  public boolean isNodeAvailable(final String iNodeName, final String iDatabaseName) {
    final ODistributedServerManager.DB_STATUS s = getDatabaseStatus(iNodeName, iDatabaseName);
    return s != ODistributedServerManager.DB_STATUS.OFFLINE
        && s != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  protected void publishLocalNodeConfiguration() {
    try {
      final ODocument cfg = distributedPlugin.getLocalNodeConfiguration();
      ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentAbstract.getDefaultSerializer());
      configurationMap.putNodeConfig(nodeUuid, cfg);
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on publishing local server configuration",
          e);
    }
  }

  public long getClusterTime() {
    if (hazelcastInstance == null) throw new HazelcastInstanceNotActiveException();

    try {
      return hazelcastInstance.getCluster().getClusterTime();
    } catch (HazelcastInstanceNotActiveException e) {
      return -1;
    }
  }

  public ODistributedLockManager getLockManagerRequester() {
    return distributedLockManager;
  }

  public ODistributedLockManager getLockManagerExecutor() {
    return distributedLockManager;
  }

  public void prepareHazelcastPluginShutdown() {
    try {
      final Set<String> databases = new HashSet<String>();

      if (hazelcastInstance != null && hazelcastInstance.getLifecycleService().isRunning())
        for (Map.Entry<String, Object> entry : configurationMap.entrySet()) {
          if (OHazelcastDistributedMap.isDatabaseStatus(entry.getKey())) {

            final String values =
                OHazelcastDistributedMap.getDatabaseStatusKeyValues(entry.getKey());
            final String nodeName = values.substring(0, values.indexOf("."));
            final String databaseName = values.substring(values.indexOf(".") + 1);

            if (nodeName.equals(this.nodeName)) {
              databases.add(databaseName);
            }
          }
        }

      // PUT DATABASES AS NOT_AVAILABLE
      for (String k : databases) {
        configurationMap.setDatabaseStatus(
            this.nodeName, k, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
      }

    } catch (HazelcastInstanceNotActiveException e) {
      // HZ IS ALREADY DOWN, IGNORE IT
    }

    if (publishLocalNodeConfigurationTask != null) publishLocalNodeConfigurationTask.cancel();
  }

  public void hazelcastPluginShutdown() {
    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    if (membershipListenerRegistration != null) {
      try {
        Cluster instance = hazelcastInstance.getCluster();
        if (instance != null) {
          instance.removeMembershipListener(membershipListenerRegistration);
        }
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

    OCallableUtils.executeIgnoringAnyExceptions(
        new OCallableNoParamNoReturn() {
          @Override
          public void call() {
            configurationMap.destroy();
          }
        });

    OCallableUtils.executeIgnoringAnyExceptions(
        new OCallableNoParamNoReturn() {
          @Override
          public void call() {
            configurationMap
                .getHazelcastMap()
                .removeEntryListener(membershipListenerMapRegistration);
          }
        });

    setNodeStatus(ODistributedServerManager.NODE_STATUS.OFFLINE);
    OServer.unregisterServerInstance(nodeName);
  }

  public Member getClusterMemberByName(final String rNodeName) {
    Member member = activeNodes.get(rNodeName);
    if (member == null) {
      for (String uuid : getConfigurationMap().getNodeUuidByName(rNodeName)) {
        for (Member m : hazelcastInstance.getCluster().getMembers()) {
          if (m.getUuid().equals(uuid)) {
            member = m;
            registerNode(member, rNodeName);
            break;
          }
        }
      }

      if (member == null) throw new ODistributedException("Cannot find node '" + rNodeName + "'");
    }
    return member;
  }

  public HazelcastInstance getHazelcastInstance() {
    for (int retry = 1;
        hazelcastInstance == null && !Thread.currentThread().isInterrupted();
        ++retry) {
      if (retry > 25) throw new ODistributedException("Hazelcast instance is not available");

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
    // Disabled the shudown hook of hazelcast, shutdown is managed by orient hook
    hazelcastConfig.setProperty("hazelcast.shutdownhook.enabled", "false");

    return Hazelcast.newHazelcastInstance(hazelcastConfig);
  }

  public String getPublicAddress() {
    return hazelcastConfig.getNetworkConfig().getPublicAddress();
  }

  public OHazelcastDistributedMap getConfigurationMap() {
    return configurationMap;
  }

  @Override
  public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {}

  public boolean updateCachedDatabaseConfiguration(
      final String databaseName, final OModifiableDistributedConfiguration cfg) {
    // VALIDATE THE CONFIGURATION FIRST
    distributedPlugin.getDistributedStrategy().validateConfiguration(cfg);

    boolean updated = tryUpdatingDatabaseConfigurationLocally(databaseName, cfg);

    if (!updated && !getConfigurationMap().existsDatabaseConfiguration(databaseName))
      // FIRST TIME, FORCE PUBLISHING
      updated = true;

    final ODocument document = cfg.getDocument();

    if (updated) {
      // WRITE TO THE MAP TO BE READ BY NEW SERVERS ON JOIN
      ORecordInternal.setRecordSerializer(
          document, ODatabaseDocumentAbstract.getDefaultSerializer());
      configurationMap.setDatabaseConfiguration(databaseName, document);
      distributedPlugin.onDbConfigUpdated(databaseName, document, updated);

      // SEND NEW CFG TO ALL THE CONNECTED CLIENTS
      serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());

      distributedPlugin.dumpServersStatus();
    }

    return updated;
  }

  public boolean tryUpdatingDatabaseConfigurationLocally(
      final String iDatabaseName, final OModifiableDistributedConfiguration cfg) {
    ODistributedDatabaseImpl local =
        distributedPlugin.getMessageService().getDatabase(iDatabaseName);
    if (local == null) return false;

    final ODistributedConfiguration dCfg = local.getDistributedConfiguration();

    ODocument oldCfg = dCfg != null ? dCfg.getDocument() : null;
    Integer oldVersion = oldCfg != null ? (Integer) oldCfg.field("version") : null;
    if (oldVersion == null) oldVersion = 0;

    int currVersion = cfg.getVersion();

    final boolean modified = currVersion > oldVersion;

    if (oldCfg != null && !modified) {
      // NO CHANGE, SKIP IT
      OLogManager.instance()
          .debug(
              this,
              "Skip saving of distributed configuration file for database '%s' because is unchanged (version %d)",
              iDatabaseName,
              currVersion);
      return false;
    }

    // SAVE IN NODE'S LOCAL RAM
    local.setDistributedConfiguration(cfg);

    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Broadcasting new distributed configuration for database: %s (version=%d)\n",
        iDatabaseName,
        currVersion);

    return modified;
  }

  @Override
  public void entryAdded(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning()) return;

    try {
      if (iEvent.getMember() == null)
        // IGNORE IT
        return;

      final String eventNodeName = getNodeName(iEvent.getMember(), true);
      if ("?".equals(eventNodeName))
        // MOM ALWAYS SAYS: DON'T ACCEPT CHANGES FROM STRANGERS NODES
        return;

      final String key = iEvent.getKey();
      if (OHazelcastDistributedMap.isNodeConfigKey(key)) {
        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
          final ODocument cfg = (ODocument) iEvent.getValue();
          final String joinedNodeName = cfg.field("name");

          if (this.nodeName.equals(joinedNodeName)) {
            ODistributedServerLog.error(
                this,
                joinedNodeName,
                eventNodeName,
                ODistributedServerLog.DIRECTION.IN,
                "Found a new node (%s) with the same name as current: '"
                    + joinedNodeName
                    + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file",
                iEvent.getMember());

            throw new ODistributedException(
                "Found a new node ("
                    + iEvent.getMember().toString()
                    + ") with the same name as current: '"
                    + joinedNodeName
                    + "'. The node has been excluded. Change the name in its config/orientdb-dserver-config.xml file");
          }

          registerNode(iEvent.getMember(), joinedNodeName);
        }

      } else if (OHazelcastDistributedMap.isDatabaseStatus(key)) {
        String values = OHazelcastDistributedMap.getDatabaseStatusKeyValues(key);
        ODistributedServerLog.info(
            this,
            nodeName,
            eventNodeName,
            ODistributedServerLog.DIRECTION.IN,
            "Received new status %s=%s",
            values,
            iEvent.getValue());

        // REASSIGN HIS CLUSTER
        final String nodeName = values.substring(0, values.indexOf("."));
        final String databaseName = values.substring(values.indexOf(".") + 1);

        distributedPlugin.onDatabaseEvent(
            nodeName, databaseName, (ODistributedServerManager.DB_STATUS) iEvent.getValue());
        distributedPlugin.invokeOnDatabaseStatusChange(
            nodeName, databaseName, (ODistributedServerManager.DB_STATUS) iEvent.getValue());

        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())
            && ODistributedServerManager.DB_STATUS.ONLINE.equals(iEvent.getValue())) {
          distributedPlugin.onDbStatusOnline(databaseName);
        }
      }
    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running", e);
    }
  }

  @Override
  public void entryUpdated(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning()) return;

    try {
      final String key = iEvent.getKey();

      final String eventNodeName = getNodeName(iEvent.getMember(), true);
      if ("?".equals(eventNodeName))
        // MOM ALWAYS SAYS: DON'T ACCEPT CHANGES FROM STRANGERS NODES
        return;

      if (OHazelcastDistributedMap.isNodeConfigKey(key)) {
        ODistributedServerLog.debug(
            this,
            nodeName,
            eventNodeName,
            ODistributedServerLog.DIRECTION.NONE,
            "Updated node configuration id=%s name=%s",
            iEvent.getMember(),
            eventNodeName);

        final ODocument cfg = (ODocument) iEvent.getValue();

        String name = cfg.field("name");
        if (!activeNodes.containsKey(name)) updateLastClusterChange();

        activeNodes.put(name, iEvent.getMember());
        if (iEvent.getMember().getUuid() != null) {
          activeNodesNamesByUuid.put(iEvent.getMember().getUuid(), name);
          activeNodesUuidByName.put(name, iEvent.getMember().getUuid());
        }
        distributedPlugin.dumpServersStatus();

      } else if (OHazelcastDistributedMap.isDatabaseStatus(key)) {
        String values = OHazelcastDistributedMap.getDatabaseStatusKeyValues(key);
        ODistributedServerLog.info(
            this,
            nodeName,
            eventNodeName,
            ODistributedServerLog.DIRECTION.IN,
            "Received updated status %s=%s",
            values,
            iEvent.getValue());

        // CALL DATABASE EVENT
        final String nodeName = values.substring(0, values.indexOf("."));
        final String databaseName = values.substring(values.indexOf(".") + 1);

        distributedPlugin.onDatabaseEvent(
            nodeName, databaseName, (ODistributedServerManager.DB_STATUS) iEvent.getValue());
        distributedPlugin.invokeOnDatabaseStatusChange(
            nodeName, databaseName, (ODistributedServerManager.DB_STATUS) iEvent.getValue());

        if (!iEvent.getMember().equals(hazelcastInstance.getCluster().getLocalMember())
            && ODistributedServerManager.DB_STATUS.ONLINE.equals(iEvent.getValue())) {
          distributedPlugin.onDbStatusOnline(databaseName);
        }

      } else if (OHazelcastDistributedMap.isRegisteredNodes(key)) {
        ODistributedServerLog.info(
            this,
            nodeName,
            eventNodeName,
            ODistributedServerLog.DIRECTION.IN,
            "Received updated about registered nodes");
        reloadRegisteredNodes();
      }

    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running", e);
    }
  }

  @Override
  public void entryRemoved(final EntryEvent<String, Object> iEvent) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning()) return;

    try {
      final String key = iEvent.getKey();

      final String eventNodeName = getNodeName(iEvent.getMember(), true);
      if ("?".equals(eventNodeName))
        // MOM ALWAYS SAYS: DON'T ACCEPT CHANGES FROM STRANGERS NODES
        return;

      if (OHazelcastDistributedMap.isNodeConfigKey(key)) {
        if (eventNodeName != null) {
          ODistributedServerLog.debug(
              this,
              nodeName,
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "Removed node configuration id=%s name=%s",
              iEvent.getMember(),
              eventNodeName);
          activeNodes.remove(eventNodeName);
          activeNodesNamesByUuid.remove(iEvent.getMember().getUuid());
          activeNodesUuidByName.remove(eventNodeName);
          distributedPlugin.onServerRemoved(eventNodeName);
        }

        updateLastClusterChange();

        distributedPlugin.dumpServersStatus();

      } else if (OHazelcastDistributedMap.isDatabaseConfiguration(key)) {
        updateLastClusterChange();

      } else if (OHazelcastDistributedMap.isDatabaseStatus(key)) {
        String values = OHazelcastDistributedMap.getDatabaseStatusKeyValues(key);
        ODistributedServerLog.debug(
            this,
            nodeName,
            getNodeName(iEvent.getMember(), true),
            ODistributedServerLog.DIRECTION.IN,
            "Received removed status %s=%s",
            values,
            iEvent.getValue());

        // CALL DATABASE EVENT
        final String nodeName = values.substring(0, values.indexOf("."));
        final String databaseName = values.substring(values.indexOf(".") + 1);

        distributedPlugin.onDatabaseEvent(
            nodeName, databaseName, (ODistributedServerManager.DB_STATUS) iEvent.getValue());
      }
    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      OLogManager.instance().error(this, "Hazelcast is not running", e);
    }
  }

  @Override
  public void entryEvicted(final EntryEvent<String, Object> iEvent) {}

  @Override
  public void mapEvicted(final MapEvent iEvent) {}

  @Override
  public void mapCleared(final MapEvent event) {}

  /** Removes the node map entry. */
  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    new Thread(
            () -> {
              try {
                updateLastClusterChange();

                if (iEvent.getMember() == null) return;

                final String nodeLeftName = getNodeName(iEvent.getMember(), true);
                if (nodeLeftName == null) return;

                distributedPlugin.removeServer(nodeLeftName, true);

              } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
                OLogManager.instance().error(this, "Hazelcast is not running", e);
              } catch (Exception e) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error on removing the server '%s'",
                        e,
                        getNodeName(iEvent.getMember(), true));
              }
            })
        .start();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    new Thread(
            () -> {
              if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
                return;

              try {
                updateLastClusterChange();
                final String addedNodeName = getNodeName(iEvent.getMember(), true);
                ODistributedServerLog.info(
                    this,
                    nodeName,
                    null,
                    ODistributedServerLog.DIRECTION.NONE,
                    "Added new node id=%s name=%s",
                    iEvent.getMember(),
                    addedNodeName);

                registerNode(iEvent.getMember(), addedNodeName);

                // REMOVE THE NODE FROM AUTO REMOVAL
                autoRemovalOfServers.remove(addedNodeName);

              } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
                OLogManager.instance().error(this, "Hazelcast is not running", e);
              }
            })
        .start();
  }

  @Override
  public void stateChanged(final LifecycleEvent event) {
    final LifecycleEvent.LifecycleState state = event.getState();
    if (state == LifecycleEvent.LifecycleState.MERGING)
      setNodeStatus(ODistributedServerManager.NODE_STATUS.MERGING);
    else if (state == LifecycleEvent.LifecycleState.MERGED) {
      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Server merged the existent cluster, merging databases...");

      configurationMap.clearLocalCache();

      // UPDATE THE UUID
      final String oldUuid = nodeUuid;
      nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Replacing old UUID %s with the new %s",
          oldUuid,
          nodeUuid);

      activeNodesNamesByUuid.remove(oldUuid);
      configurationMap.removeNode(oldUuid);

      activeNodes.put(nodeName, hazelcastInstance.getCluster().getLocalMember());
      activeNodesNamesByUuid.put(nodeUuid, nodeName);
      activeNodesUuidByName.put(nodeName, nodeUuid);

      publishLocalNodeConfiguration();
      setNodeStatus(ODistributedServerManager.NODE_STATUS.ONLINE);

      // TEMPORARY PATCH TO FIX HAZELCAST'S BEHAVIOUR THAT ENQUEUES THE MERGING ITEM EVENT WITH
      // THIS
      // AND ACTIVE NODES MAP COULD BE STILL NOT FILLED
      Thread t =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {

                    for (final String databaseName :
                        distributedPlugin.getMessageService().getDatabases()) {
                      executeInDistributedDatabaseLock(
                          databaseName,
                          20000,
                          null,
                          new OCallable<Object, OModifiableDistributedConfiguration>() {
                            @Override
                            public Object call(final OModifiableDistributedConfiguration cfg) {
                              ODistributedServerLog.debug(
                                  this,
                                  nodeName,
                                  null,
                                  ODistributedServerLog.DIRECTION.NONE,
                                  "Replacing local database '%s' configuration with the most recent from the joined cluster...",
                                  databaseName);

                              cfg.override(configurationMap.getDatabaseConfiguration(databaseName));

                              return null;
                            }
                          });
                    }
                  } finally {
                    ODistributedServerLog.warn(
                        this,
                        nodeName,
                        null,
                        ODistributedServerLog.DIRECTION.NONE,
                        "Network merged ...");
                    setNodeStatus(ODistributedServerManager.NODE_STATUS.ONLINE);
                  }
                }
              });
      t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      t.start();
    }
  }

  public void removeDbFromClusterMetadata(final ODatabaseInternal iDatabase) {
    final String dbName = iDatabase.getName();

    if (configurationMap != null) {
      configurationMap.removeDatabaseStatus(nodeName, dbName);
    }
  }

  public void dropDatabaseConfiguration(final String dbName) {
    // LAST NODE HOLDING THE DATABASE, DELETE DISTRIBUTED CFG TOO
    configurationMap.removeDatabaseConfiguration(dbName);
    configurationMap.remove(OSyncDatabaseTask.DEPLOYDB + dbName);
    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Dropped last copy of database '%s', removing it from the cluster",
        dbName);
  }

  // Remove the given distributed db from the current server's configuration and
  // return the name of all other servers that also have the db.
  public Set<String> dropDbFromConfiguration(final String dbName) {
    final ODistributedConfiguration dCfg = getDatabaseConfiguration(dbName);

    final Set<String> servers = dCfg.getAllConfiguredServers();
    final long start = System.currentTimeMillis();

    // WAIT ALL THE SERVERS BECOME ONLINE
    boolean allServersAreOnline = false;
    while (!allServersAreOnline && System.currentTimeMillis() - start < 5000) {
      allServersAreOnline = true;
      for (String s : servers) {
        final ODistributedServerManager.DB_STATUS st = getDatabaseStatus(s, dbName);
        if (st == ODistributedServerManager.DB_STATUS.NOT_AVAILABLE
            || st == ODistributedServerManager.DB_STATUS.SYNCHRONIZING
            || st == ODistributedServerManager.DB_STATUS.BACKUP) {
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
    return servers;
  }

  public ODocument getNodeConfigurationByUuid(final String iNodeId, final boolean useCache) {
    if (configurationMap == null)
      // NOT YET STARTED
      return null;

    final ODocument doc;
    if (useCache) {
      doc = configurationMap.getLocalCachedNodeConfig(iNodeId);
    } else {
      doc = configurationMap.getNodeConfig(iNodeId);
    }

    if (doc == null)
      ODistributedServerLog.debug(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.OUT,
          "Cannot find node with id '%s'",
          iNodeId);

    return doc;
  }

  public ODocument getNodeConfigurationByName(final String nodeName, final boolean useCache) {
    String uuid = getNodeUuidByName(nodeName);
    return getNodeConfigurationByUuid(uuid, useCache);
  }

  public ODistributedServerManager.DB_STATUS getDatabaseStatus(
      final String iNode, final String iDatabaseName) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(iDatabaseName)) {
      // CHECK THE SERVER STATUS
      if (getActiveServers().contains(iNode)) {
        return ODistributedServerManager.DB_STATUS.ONLINE;
      } else {
        return ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
      }
    }

    final ODistributedServerManager.DB_STATUS status =
        configurationMap.getCachedDatabaseStatus(iNode, iDatabaseName);
    return status != null ? status : ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  public void setDatabaseStatus(
      final String iNode,
      final String iDatabaseName,
      final ODistributedServerManager.DB_STATUS iStatus) {

    final ODistributedServerManager.DB_STATUS currStatus =
        configurationMap.getDatabaseStatus(iNode, iDatabaseName);

    if (currStatus == null || currStatus != iStatus) {
      configurationMap.setDatabaseStatus(iNode, iDatabaseName, iStatus);
      distributedPlugin.invokeOnDatabaseStatusChange(iNode, iDatabaseName, iStatus);
    }
  }

  // Returns name of distributed databases in the cluster.
  public Set<String> getDatabases() {
    return configurationMap.getDatabases();
  }

  public void reloadRegisteredNodes() {
    final ODocument registeredNodesFromCluster = configurationMap.getRegisteredNodes();

    if (registeredNodesFromCluster.hasProperty("ids")
        && registeredNodesFromCluster.hasProperty("names")) {
      registeredNodeById.clear();
      registeredNodeById.addAll(registeredNodesFromCluster.field("ids", OType.EMBEDDEDLIST));
      registeredNodeByName.clear();
      registeredNodeByName.putAll(registeredNodesFromCluster.field("names", OType.EMBEDDEDMAP));
    } else
      throw new ODistributedException("Cannot find distributed 'registeredNodes' configuration");
  }

  private List<String> getRegisteredNodes() {
    return configurationMap.getNodes();
  }

  public void removeNodeFromConfiguration(
      final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Removing server '%s' from all the databases (removeOnlyDynamicServers=%s)...",
        nodeLeftName,
        removeOnlyDynamicServers);

    for (String dbName : distributedPlugin.getManagedDatabases()) {
      removeNodeFromConfiguration(nodeLeftName, dbName, removeOnlyDynamicServers, false);
    }
  }

  public boolean removeNodeFromConfiguration(
      final String nodeLeftName,
      final String databaseName,
      final boolean removeOnlyDynamicServers,
      final boolean statusOffline) {
    ODistributedServerLog.debug(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Removing server '%s' from database configuration '%s' (removeOnlyDynamicServers=%s)...",
        nodeLeftName,
        databaseName,
        removeOnlyDynamicServers);

    final OModifiableDistributedConfiguration cfg = getDatabaseConfiguration(databaseName).modify();

    if (removeOnlyDynamicServers) {
      // CHECK THE SERVER IS NOT REGISTERED STATICALLY
      final String dc = cfg.getDataCenterOfServer(nodeLeftName);
      if (dc != null) {
        ODistributedServerLog.info(
            this,
            nodeName,
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in data center '%s' configuration for database '%s'",
            nodeLeftName,
            dc,
            databaseName);
        return false;
      }

      // CHECK THE SERVER IS NOT REGISTERED IN SERVERS
      final Set<String> registeredServers = cfg.getRegisteredServers();
      if (registeredServers.contains(nodeLeftName)) {
        ODistributedServerLog.info(
            this,
            nodeName,
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in 'servers' of the distributed configuration for database '%s'",
            nodeLeftName,
            databaseName);
        return false;
      }
    }

    final boolean found =
        executeInDistributedDatabaseLock(
            databaseName,
            20000,
            cfg,
            new OCallable<Boolean, OModifiableDistributedConfiguration>() {
              @Override
              public Boolean call(OModifiableDistributedConfiguration cfg) {
                return cfg.removeServer(nodeLeftName) != null;
              }
            });

    final ODistributedServerManager.DB_STATUS nodeLeftStatus =
        getDatabaseStatus(nodeLeftName, databaseName);
    if (statusOffline && nodeLeftStatus != ODistributedServerManager.DB_STATUS.OFFLINE)
      setDatabaseStatus(nodeLeftName, databaseName, ODistributedServerManager.DB_STATUS.OFFLINE);
    else if (!statusOffline && nodeLeftStatus != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE)
      setDatabaseStatus(
          nodeLeftName, databaseName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    return found;
  }

  public Member removeFromLocalActiveServerList(String nodeLeftName) {
    final Member member = activeNodes.remove(nodeLeftName);
    if (member == null) return null;
    if (member.getUuid() != null) activeNodesNamesByUuid.remove(member.getUuid());
    activeNodesUuidByName.remove(nodeLeftName);
    return member;
  }

  public void removeServerFromCluster(
      final Member member, final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning()) return;

    final long autoRemoveOffLineServer =
        OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS.getValueAsLong();
    if (autoRemoveOffLineServer == 0)
      // REMOVE THE NODE RIGHT NOW
      removeNodeFromConfiguration(nodeLeftName, removeOnlyDynamicServers);
    else if (autoRemoveOffLineServer > 0) {
      // SCHEDULE AUTO REMOVAL IN A WHILE
      autoRemovalOfServers.put(nodeLeftName, System.currentTimeMillis());
      Orient.instance()
          .scheduleTask(
              () -> {
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
              },
              autoRemoveOffLineServer,
              0);
    }

    for (String databaseName : distributedPlugin.getManagedDatabases()) {
      final ODistributedServerManager.DB_STATUS nodeLeftStatus =
          getDatabaseStatus(nodeLeftName, databaseName);
      if (nodeLeftStatus != ODistributedServerManager.DB_STATUS.OFFLINE
          && nodeLeftStatus != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE)
        configurationMap.setDatabaseStatus(
            nodeLeftName, databaseName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    }

    ODistributedServerLog.warn(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Node removed id=%s name=%s",
        member,
        nodeLeftName);

    if (nodeLeftName.startsWith("ext:")) {
      final List<String> registeredNodes = getRegisteredNodes();

      ODistributedServerLog.error(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Removed node id=%s name=%s has not being recognized. Remove the node manually (registeredNodes=%s)",
          member,
          nodeLeftName,
          registeredNodes);
    }
  }

  public Set<String> getActiveServers() {
    return activeNodes.keySet();
  }

  protected void registerNode(final Member member, final String joinedNodeName) {
    if (activeNodes.containsKey(joinedNodeName))
      // ALREADY REGISTERED: SKIP IT
      return;

    if (joinedNodeName.startsWith("ext:"))
      // NODE HAS NOT IS YET
      return;

    if (activeNodes.putIfAbsent(joinedNodeName, member) == null) {
      if (!distributedPlugin.onNodeJoining(joinedNodeName)) {
        // DENY JOIN
        ODistributedServerLog.info(
            this,
            nodeName,
            getNodeName(member, true),
            ODistributedServerLog.DIRECTION.IN,
            "Denied node to join the cluster id=%s name=%s",
            member,
            getNodeName(member, true));

        activeNodes.remove(joinedNodeName);
        return;
      }

      activeNodesNamesByUuid.put(member.getUuid(), joinedNodeName);
      activeNodesUuidByName.put(joinedNodeName, member.getUuid());

      distributedPlugin.onNodeJoined(joinedNodeName, member);
    }
  }

  public String getNodeName(final Member iMember, final boolean useCache) {
    if (iMember == null || iMember.getUuid() == null) return "?";

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    final String name = activeNodesNamesByUuid.get(iMember.getUuid());
    if (name != null) return name;

    final ODocument cfg = getNodeConfigurationByUuid(iMember.getUuid(), useCache);
    if (cfg != null) return cfg.field("name");

    return "ext:" + iMember.getUuid();
  }

  public ODocument getClusterConfiguration() {

    final ODocument cluster = new ODocument();

    cluster.field("localName", distributedPlugin.getName());
    cluster.field("localId", nodeUuid);

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    for (Member member : activeNodes.values()) {
      ODocument memberConfig = getNodeConfigurationByUuid(member.getUuid(), true);
      if (memberConfig == null) {
        continue;
      }
      memberConfig = memberConfig.copy();

      members.add(memberConfig);

      final String nodeName = getNodeName(member, true);
      final Map<String, String> dbStatus = new HashMap<>();
      memberConfig.field("databasesStatus", dbStatus, OType.EMBEDDEDMAP);
      // Member DB status
      for (String db : distributedPlugin.getManagedDatabases()) {
        final DB_STATUS nodeDbState = getDatabaseStatus(nodeName, db);
        dbStatus.put(db, nodeDbState.toString());
      }
    }

    return cluster;
  }

  public String tryGetNodeNameById(final int id) {
    if (id < 0) throw new IllegalArgumentException("Node id " + id + " is invalid");

    synchronized (registeredNodeById) {
      if (id < registeredNodeById.size()) return registeredNodeById.get(id);
    }
    return null;
  }

  public int tryGetNodeIdByName(final String name) {
    final Integer val = registeredNodeByName.get(name);
    if (val == null) return -1;
    return val.intValue();
  }

  public String getNodeUuidByName(final String name) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Node name " + name + " is invalid");

    return activeNodesUuidByName.get(name);
  }

  public int getAvailableNodes(final String iDatabaseName) {
    int availableNodes = 0;
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName)) availableNodes++;
    }
    return availableNodes;
  }

  public List<String> getOnlineNodes(final String iDatabaseName) {
    final List<String> onlineNodes = new ArrayList<String>(activeNodes.size());
    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeOnline(entry.getKey(), iDatabaseName)) onlineNodes.add(entry.getKey());
    }
    return onlineNodes;
  }

  public Set<String> getAvailableNodeNames(final String iDatabaseName) {
    final Set<String> nodes = new HashSet<String>();

    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName)) nodes.add(entry.getKey());
    }
    return nodes;
  }

  /**
   * Returns the available nodes (not offline) and clears the node list by removing the offline
   * nodes.
   */
  public int getAvailableNodes(final Collection<String> iNodes, final String databaseName) {
    for (Iterator<String> it = iNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();

      if (!isNodeAvailable(node, databaseName)) it.remove();
    }
    return iNodes.size();
  }

  /**
   * Executes an operation protected by a distributed lock (one per database).
   *
   * @param <T> Return type
   * @param databaseName Database name
   * @param iCallback Operation @return The operation's result of type T
   */
  public <T> T executeInDistributedDatabaseLock(
      final String databaseName,
      final long timeoutLocking,
      OModifiableDistributedConfiguration lastCfg,
      final OCallable<T, OModifiableDistributedConfiguration> iCallback) {

    boolean updated;
    T result;
    getLockManagerExecutor().acquireExclusiveLock(databaseName, nodeName, timeoutLocking);
    try {

      if (lastCfg == null)
        // ACQUIRE CFG INSIDE THE LOCK
        lastCfg = getDatabaseConfiguration(databaseName).modify();

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(
            this,
            nodeName,
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Current distributed configuration for database '%s': %s",
            databaseName,
            lastCfg.getDocument().toJSON());

      try {

        result = iCallback.call(lastCfg);

      } finally {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(
              this,
              nodeName,
              null,
              ODistributedServerLog.DIRECTION.NONE,
              "New distributed configuration for database '%s': %s",
              databaseName,
              lastCfg.getDocument().toJSON());

        // CONFIGURATION CHANGED, UPDATE IT ON THE CLUSTER AND DISK
        updated = updateCachedDatabaseConfiguration(databaseName, lastCfg);
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);

    } finally {
      getLockManagerRequester().releaseExclusiveLock(databaseName, nodeName);
    }
    if (updated) {
      // SEND NEW CFG TO ALL THE CONNECTED CLIENTS
      distributedPlugin.notifyClients(databaseName);
      serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());
    }
    return result;
  }

  public ODocument getOnlineDatabaseConfiguration(final String iDatabaseName) {
    return configurationMap.getDatabaseConfiguration(iDatabaseName);
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName) {
    return getDatabaseConfiguration(iDatabaseName, true);
  }

  public ODistributedConfiguration getDatabaseConfiguration(
      final String iDatabaseName, final boolean createIfNotPresent) {
    ODistributedDatabaseImpl local =
        distributedPlugin.getMessageService().getDatabase(iDatabaseName);
    if (local == null) {
      return null;
    }

    return local.getDistributedConfiguration();
  }

  public void setNodeStatus(final ODistributedServerManager.NODE_STATUS iStatus) {
    if (status.equals(iStatus))
      // NO CHANGE
      return;

    status = iStatus;

    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Updated node status to '%s'",
        status);
  }

  public ODistributedServerManager.NODE_STATUS getNodeStatus() {
    return status;
  }

  public boolean checkNodeStatus(final ODistributedServerManager.NODE_STATUS iStatus2Check) {
    return status.equals(iStatus2Check);
  }

  public boolean isNodeOnline(final String iNodeName, final String iDatabaseName) {
    return getDatabaseStatus(iNodeName, iDatabaseName)
        == ODistributedServerManager.DB_STATUS.ONLINE;
  }

  public void updateLastClusterChange() {
    lastClusterChangeOn = System.currentTimeMillis();
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
  }

  public int getLocalNodeId() {
    return nodeId;
  }

  public String getLocalNodeUuid() {
    return nodeUuid;
  }

  public String getLocalNodeName() {
    return nodeName;
  }
}
