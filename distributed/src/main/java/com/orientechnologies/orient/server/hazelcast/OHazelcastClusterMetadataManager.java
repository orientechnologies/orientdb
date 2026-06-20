package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OCallableNoParamNoReturn;
import com.orientechnologies.common.util.OCallableUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.NODE_STATUS;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.ODistributedStartupException;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OHazelcastClusterMetadataManager
    implements MembershipListener, EntryListener<String, Object>, LifecycleListener {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OHazelcastClusterMetadataManager.class);

  public static final String CONFIG_NODE_PREFIX = "node.";

  protected String hazelcastConfigFile = "hazelcast.xml";
  protected Config hazelcastConfig;
  protected String membershipListenerRegistration;
  protected String membershipListenerMapRegistration;
  protected volatile HazelcastInstance hazelcastInstance;

  // THIS MAP IS BACKED BY HAZELCAST EVENTS. IN THIS WAY WE AVOID TO USE HZ MAP DIRECTLY
  protected OHazelcastDistributedMap configurationMap;

  protected ConcurrentMap<String, Member> activeNodes = new ConcurrentHashMap<>();
  protected ConcurrentMap<String, String> activeNodesNamesByUuid = new ConcurrentHashMap<>();
  protected ConcurrentMap<String, String> activeNodesUuidByName = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, Integer> registeredNodeByName = new ConcurrentHashMap<>();

  protected OCancellableTimer publishLocalNodeConfigurationTask = null;

  protected volatile NODE_STATUS status = NODE_STATUS.OFFLINE;

  protected long lastClusterChangeOn;
  private String nodeUuid;

  private String nodeName = null;
  private OServer serverInstance;

  private final ODistributedPlugin distributedPlugin;

  public OHazelcastClusterMetadataManager(ODistributedPlugin distributedPlugin) {
    this.distributedPlugin = distributedPlugin;
  }

  public void configHazelcastPlugin(
      OServer server, OServerParameterConfiguration[] params, String nodeName)
      throws FileNotFoundException {
    this.nodeName = nodeName;
    this.serverInstance = server;
    for (OServerParameterConfiguration param : params) {
      if (param.name.equalsIgnoreCase("configuration.hazelcast")) {
        hazelcastConfigFile = OSystemVariableResolver.resolveSystemVariables(param.value);
        hazelcastConfigFile = OFileUtils.getPath(hazelcastConfigFile);
        // If hazelcastConfig is null, use the file system XML config.
        if (hazelcastConfig == null) {
          hazelcastConfig = new FileSystemXmlConfig(hazelcastConfigFile);
          hazelcastConfig.setClassLoader(this.getClass().getClassLoader());
        }

        // Disabled the shudown hook of hazelcast, shutdown is managed by orient hook
        hazelcastConfig.setProperty("hazelcast.shutdownhook.enabled", "false");
      }
    }
  }

  public Config getHazelcastConfig() {
    return hazelcastConfig;
  }

  public void startupHazelcastPlugin() throws IOException, InterruptedException {
    status = NODE_STATUS.STARTING;

    final String localNodeName = nodeName;

    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    hazelcastInstance = configureHazelcast();

    nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

    final LifecycleService lifecycleService = hazelcastInstance.getLifecycleService();
    lifecycleService.addLifecycleListener(this);

    logger.info("Starting distributed server '%s' (hzID=%s)...", localNodeName, nodeUuid);

    activeNodes.put(localNodeName, hazelcastInstance.getCluster().getLocalMember());
    activeNodesNamesByUuid.put(nodeUuid, localNodeName);
    activeNodesUuidByName.put(localNodeName, nodeUuid);

    configurationMap = new OHazelcastDistributedMap(hazelcastInstance);

    // PUBLISH CURRENT NODE NAME
    final ONodeConfig nodeCfg = new ONodeConfig();

    // REMOVE ANY PREVIOUS REGISTERED SERVER WITH THE SAME NODE NAME
    final Set<String> node2Remove = new HashSet<String>();

    for (String nodeUUid : configurationMap.getNodes()) {
      final ONodeConfig nCfg = configurationMap.getNodeConfig(nodeUUid);
      if (nodeName.equals(nCfg.getName())) {
        // SAME NODE NAME: REMOVE IT
        node2Remove.add(nodeUUid);
      }
    }

    for (String n : node2Remove) configurationMap.removeNode(n);

    nodeCfg.setUuid(nodeUuid);
    nodeCfg.setName(nodeName);
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
          logger.warnNode(
              localNodeName, "Cannot find configuration for member: %s, uuid", m, m.getUuid());
      }
    }

    logger.infoNode(localNodeName, "Servers in cluster: %s", activeNodes.keySet());

    publishLocalNodeConfiguration();

    if (!configurationMap.existsNode(nodeUuid)) {
      // NODE NOT REGISTERED, FORCING SHUTTING DOWN
      logger.errorNode(localNodeName, "Error on registering local node on cluster");
      throw new ODistributedStartupException("Error on registering local node on cluster");
    }

    // CONNECTS TO ALL THE AVAILABLE NODES
    distributedPlugin.connectToAllNodes(activeNodes.keySet());

    publishLocalNodeConfiguration();
    membershipListenerMapRegistration =
        configurationMap.getHazelcastMap().addEntryListener(this, true);
    membershipListenerRegistration = hazelcastInstance.getCluster().addMembershipListener(this);
    OrientDBInternal ctx = serverInstance.getDatabases();

    publishLocalNodeConfiguration();

    final long delay = OGlobalConfiguration.DISTRIBUTED_PUBLISH_NODE_STATUS_EVERY.getValueAsLong();
    if (delay > 0) {
      publishLocalNodeConfigurationTask =
          ctx.periodicExecute(this::publishLocalNodeConfiguration, delay);
    }
  }

  protected void publishLocalNodeConfiguration() {
    try {
      final ONodeConfig cfg = distributedPlugin.getLocalNodeConfiguration();
      configurationMap.putNodeConfig(nodeUuid, cfg);
    } catch (Exception e) {
      logger.errorNode(nodeName, "Error on publishing local server configuration", e);
    }
  }

  public void prepareHazelcastPluginShutdown() {
    if (publishLocalNodeConfigurationTask != null) publishLocalNodeConfigurationTask.cancel();
  }

  public void hazelcastPluginShutdown() {
    activeNodes.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();

    if (membershipListenerRegistration != null && hazelcastInstance != null) {
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
        logger.error("Error on shutting down Hazelcast instance", e);
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

    return Hazelcast.newHazelcastInstance(hazelcastConfig);
  }

  public OHazelcastDistributedMap getConfigurationMap() {
    return configurationMap;
  }

  @Override
  public void memberAttributeChanged(final MemberAttributeEvent memberAttributeEvent) {}

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
            logger.errorIn(
                joinedNodeName,
                eventNodeName,
                "Found a new node (%s) with the same name as current: '%s'. "
                    + "The node has been excluded. Change the name in its"
                    + " config/orientdb-dserver-config.xml file",
                iEvent.getMember(),
                joinedNodeName);

            throw new ODistributedException(
                "Found a new node ("
                    + iEvent.getMember().toString()
                    + ") with the same name as current: '"
                    + joinedNodeName
                    + "'. The node has been excluded. Change the name in its"
                    + " config/orientdb-dserver-config.xml file");
          }

          registerNode(iEvent.getMember(), joinedNodeName);
        }
      }
    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      logger.error("Hazelcast is not running", e);
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

        final ODocument cfg = (ODocument) iEvent.getValue();

        String name = cfg.field("name");
        if (!activeNodes.containsKey(name)) updateLastClusterChange();

        activeNodes.put(name, iEvent.getMember());
        if (iEvent.getMember().getUuid() != null) {
          activeNodesNamesByUuid.put(iEvent.getMember().getUuid(), name);
          activeNodesUuidByName.put(name, iEvent.getMember().getUuid());
        }
        distributedPlugin.dumpServersStatus();
      }

    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      logger.error("Hazelcast is not running", e);
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
          activeNodes.remove(eventNodeName);
          activeNodesNamesByUuid.remove(iEvent.getMember().getUuid());
          activeNodesUuidByName.remove(eventNodeName);
          distributedPlugin.onServerRemoved(eventNodeName);
        }

        updateLastClusterChange();

        distributedPlugin.dumpServersStatus();
      }
    } catch (HazelcastInstanceNotActiveException | RetryableHazelcastException e) {
      logger.error("Hazelcast is not running", e);
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
    updateLastClusterChange();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    OrientDBInternal ctx = serverInstance.getDatabases();
    ctx.execute(
        () -> {
          if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning())
            return;

          updateLastClusterChange();
          final String addedNodeName = getNodeName(iEvent.getMember(), true);
          logger.infoNode(
              nodeName, "Added new node id=%s name=%s", iEvent.getMember(), addedNodeName);

          registerNode(iEvent.getMember(), addedNodeName);
        });
  }

  @Override
  public void stateChanged(final LifecycleEvent event) {
    final LifecycleEvent.LifecycleState state = event.getState();
    if (state == LifecycleEvent.LifecycleState.MERGED) {
      logger.infoNode(nodeName, "Server merged the existent cluster, merging databases...");

      configurationMap.clearLocalCache();

      // UPDATE THE UUID
      final String oldUuid = nodeUuid;
      nodeUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();

      logger.infoNode(nodeName, "Replacing old UUID %s with the new %s", oldUuid, nodeUuid);

      activeNodesNamesByUuid.remove(oldUuid);
      configurationMap.removeNode(oldUuid);

      activeNodes.put(nodeName, hazelcastInstance.getCluster().getLocalMember());
      activeNodesNamesByUuid.put(nodeUuid, nodeName);
      activeNodesUuidByName.put(nodeName, nodeUuid);

      publishLocalNodeConfiguration();
    }
  }

  public ONodeConfig getNodeConfigurationByUuid(final String iNodeId, final boolean useCache) {
    if (configurationMap == null)
      // NOT YET STARTED
      return null;

    final ONodeConfig doc;
    if (useCache) {
      doc = configurationMap.getLocalCachedNodeConfig(iNodeId);
    } else {
      doc = configurationMap.getNodeConfig(iNodeId);
    }

    return doc;
  }

  public ONodeConfig getNodeConfigurationByName(final String nodeName, final boolean useCache) {
    String uuid = getNodeUuidByName(nodeName);
    return getNodeConfigurationByUuid(uuid, useCache);
  }

  public Member removeFromLocalActiveServerList(String nodeLeftName) {
    final Member member = activeNodes.remove(nodeLeftName);
    if (member == null) return null;
    if (member.getUuid() != null) activeNodesNamesByUuid.remove(member.getUuid());
    activeNodesUuidByName.remove(nodeLeftName);
    return member;
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
      String url = null;
      for (int retry = 0; retry < 20; ++retry) {
        ONodeConfig cfg = getNodeConfigurationByUuid(member.getUuid(), false);
        if (cfg == null || cfg.getListeners() == null) {
          try {
            Thread.sleep(100);
            continue;

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw OException.wrapException(
                new ODistributedException("Cannot find node '" + joinedNodeName + "'"), e);
          }
        }

        url = ODistributedPlugin.getListeningBinaryAddress(cfg);

        if (url != null) {
          break;
        }

        // RETRY TO GET USR+PASSWORD IN A WHILE
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException.wrapException(
              new OInterruptedException("Cannot connect to remote server " + joinedNodeName), e);
        }
      }
      if (url == null) {
        return;
      }

      activeNodesNamesByUuid.put(member.getUuid(), joinedNodeName);
      activeNodesUuidByName.put(joinedNodeName, member.getUuid());

      distributedPlugin.onNodeJoined(joinedNodeName, url, member);
    }
  }

  public String getNodeName(final Member iMember, final boolean useCache) {
    if (iMember == null || iMember.getUuid() == null) return "?";

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    final String name = activeNodesNamesByUuid.get(iMember.getUuid());
    if (name != null) return name;

    final ONodeConfig cfg = getNodeConfigurationByUuid(iMember.getUuid(), useCache);
    if (cfg != null) return cfg.getName();

    return "ext:" + iMember.getUuid();
  }

  public OClusterConfiguration getClusterConfiguration() {

    OClusterConfiguration clusterConfig = new OClusterConfiguration();
    OrientDBDistributed context = (OrientDBDistributed) serverInstance.getDatabases();

    clusterConfig.setLocalName(distributedPlugin.getName());
    clusterConfig.setLocalId(nodeUuid);

    var networkTopology = context.getNodeState().getOps().getNetworkTopology();
    var databaseTopology = context.getNodeState().getOps().getDatabaseTopology();
    // INSERT MEMBERS
    for (var member : networkTopology.getMembers()) {
      ONodeConfig nodeConfig = getNodeConfigurationByName(member.getNode(), true);
      if (nodeConfig == null) {
        continue;
      }
      final String nodeName = member.getNode();
      final Map<String, String> dbStatus = new HashMap<>();
      for (var db : databaseTopology.getDatabases()) {
        var dbName = databaseTopology.getDatabaseName(db);
        final DB_STATUS nodeDbState = context.getDatabaseStatus(nodeName, dbName);
        dbStatus.put(dbName, nodeDbState.toString());
      }
      nodeConfig.setDatabasesStatus(dbStatus);
      clusterConfig.addMember(nodeConfig);
    }

    return clusterConfig;
  }

  public String getNodeUuidByName(final String name) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Node name " + name + " is invalid");

    return activeNodesUuidByName.get(name);
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName) {
    return getDatabaseConfiguration(iDatabaseName, true);
  }

  public ODistributedConfiguration getDatabaseConfiguration(
      final String iDatabaseName, final boolean createIfNotPresent) {
    return ((OrientDBDistributed) serverInstance.getDatabases())
        .getDistributedConfiguration(iDatabaseName);
  }

  public void updateLastClusterChange() {
    lastClusterChangeOn = System.currentTimeMillis();
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
  }
}
