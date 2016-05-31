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
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.sql.OCommandExecutorSQLHASyncCluster;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.task.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * Abstract plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class ODistributedAbstractPlugin extends OServerPluginAbstract
    implements ODistributedServerManager, ODatabaseLifecycleListener, OCommandOutputListener {
  public static final String                                     REPLICATOR_USER                   = "_CrossServerTempUser";
  protected static final String                                  PAR_DEF_DISTRIB_DB_CONFIG         = "configuration.db.default";
  protected static final String                                  NODE_NAME_ENV                     = "ORIENTDB_NODE_NAME";

  protected OServer                                              serverInstance;
  protected boolean                                              enabled                           = true;
  protected String                                               nodeName                          = null;
  protected int                                                  nodeId                            = -1;
  protected File                                                 defaultDatabaseConfigFile;
  protected ConcurrentHashMap<String, ODistributedStorage>       storages                          = new ConcurrentHashMap<String, ODistributedStorage>();
  protected volatile NODE_STATUS                                 status                            = NODE_STATUS.OFFLINE;
  protected long                                                 lastClusterChangeOn;
  protected List<ODistributedLifecycleListener>                  listeners                         = new ArrayList<ODistributedLifecycleListener>();
  protected final ConcurrentMap<String, ORemoteServerController> remoteServers                     = new ConcurrentHashMap<String, ORemoteServerController>();
  protected TimerTask                                            publishLocalNodeConfigurationTask = null;

  // LOCAL MSG COUNTER
  protected AtomicLong                                           localMessageIdCounter             = new AtomicLong();
  protected OClusterOwnershipAssignmentStrategy                  clusterAssignmentStrategy         = new ODefaultClusterOwnershipAssignmentStrategy(
      this);

  protected Map<String, Long>                                    lastLSNWriting                    = new HashMap<String, Long>();
  protected static final int                                     DEPLOY_DB_MAX_RETRIES             = 10;
  protected Map<String, Member>                                  activeNodes                       = new ConcurrentHashMap<String, Member>();
  protected Map<String, String>                                  activeNodesNamesByMemberId        = new ConcurrentHashMap<String, String>();
  protected List<String>                                         registeredNodeById;
  protected Map<String, Integer>                                 registeredNodeByName;
  protected ODistributedMessageServiceImpl                       messageService;
  protected Date                                                 startedOn                         = new Date();
  protected ORemoteTaskFactory                                   taskFactory                       = new ODefaultRemoteTaskFactory();
  protected String                                               nodeUuid;

  private volatile int                                           hashLastServerDump                = 0;

  protected abstract ODistributedConfiguration getLastDatabaseConfiguration(String databaseName);

  public void waitUntilNodeOnline() throws InterruptedException {
    while (!status.equals(NODE_STATUS.ONLINE))
      Thread.sleep(100);
  }

  public void waitUntilNodeOnline(final String nodeName, final String databaseName) throws InterruptedException {
    while (messageService == null || messageService.getDatabase(databaseName) == null || !isNodeOnline(nodeName, databaseName))
      Thread.sleep(100);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    serverInstance = oServer;
    oServer.setVariable("ODistributedAbstractPlugin", this);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(OSystemVariableResolver.resolveSystemVariables(param.value))) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("nodeName")) {
        nodeName = param.value;
        if (nodeName.contains("."))
          throw new OConfigurationException("Illegal node name '" + nodeName + "'. '.' is not allowed in node name");
      } else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        setDefaultDatabaseConfigFile(param.value);
      }
    }

    if (serverInstance.getUser("replicator") == null)
      // DROP THE REPLICATOR USER. THIS USER WAS NEEDED BEFORE 2.2, BUT IT'S NOT REQUIRED ANYMORE
      OLogManager.instance().config(this,
          "Found 'replicator' user. Starting from OrientDB v2.2 this internal user is no needed anymore. Removing it...");
    try {
      serverInstance.dropUser("replicator");
    } catch (IOException e) {
      throw OException.wrapException(new OConfigurationException("Error on deleting 'replicator' user"), e);
    }
  }

  public void setDefaultDatabaseConfigFile(final String iFile) {
    defaultDatabaseConfigFile = new File(OSystemVariableResolver.resolveSystemVariables(iFile));
    if (!defaultDatabaseConfigFile.exists())
      throw new OConfigurationException("Cannot find distributed database config file: " + defaultDatabaseConfigFile);
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    Orient.instance().addDbLifecycleListener(this);
  }

  @Override
  public ODistributedAbstractPlugin registerLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.add(iListener);
    return this;
  }

  @Override
  public ODistributedAbstractPlugin unregisterLifecycleListener(final ODistributedLifecycleListener iListener) {
    listeners.remove(iListener);
    return this;
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    // CLOSE ALL CONNECTIONS TO THE SERVERS
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();

    if (publishLocalNodeConfigurationTask != null)
      publishLocalNodeConfigurationTask.cancel();

    if (messageService != null)
      messageService.shutdown();

    activeNodes.clear();
    activeNodesNamesByMemberId.clear();

    setNodeStatus(NODE_STATUS.OFFLINE);

    Orient.instance().removeDbLifecycleListener(this);

    // CLOSE AND FREE ALL THE STORAGES
    for (ODistributedStorage s : storages.values())
      try {
        s.shutdownAsynchronousWorker();
        s.close();
      } catch (Exception e) {
      }
    storages.clear();
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
      final String dbName = iDatabase.getName();

      if (getMessageService().getDatabase(dbName) == null) {
        // CHECK TO PUBLISH IT TO THE CLUSTER
        final ODistributedDatabaseImpl distribDatabase = messageService.registerDatabase(dbName);
        distribDatabase.setOnline();
      }

      final ODistributedConfiguration cfg = getDatabaseConfiguration(dbName);
      if (cfg == null)
        return;

      if (!(iDatabase.getStorage() instanceof ODistributedStorage)
          || ((ODistributedStorage) iDatabase.getStorage()).getDistributedManager().isOffline()) {

        final ODistributedStorage storage = getStorage(dbName);

        // INIT IT
        storage.wrap((OAbstractPaginatedStorage) iDatabase.getStorage().getUnderlying());

        iDatabase.replaceStorage(storage);

        if (isNodeOnline(nodeName, dbName))
          installDbClustersLocalStrategy(iDatabase);
      }
    } catch (HazelcastException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

    } catch (HazelcastInstanceNotActiveException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

    } finally {
      // RESTORE ORIGINAL DATABASE INSTANCE IN TL
      ODatabaseRecordThreadLocal.INSTANCE.set(currDb);
    }
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    synchronized (storages) {
      storages.remove(iDatabase.getURL());
    }

    final ODistributedMessageService msgService = getMessageService();
    if (msgService != null) {
      msgService.unregisterDatabase(iDatabase.getName());
    }
  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {
  }

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  public String getNodeName(final Member iMember) {
    if (iMember == null)
      return "?";

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    final String name = activeNodesNamesByMemberId.get(iMember.getUuid());
    if (name != null)
      return name;

    final ODocument cfg = getNodeConfigurationByUuid(iMember.getUuid());
    if (cfg != null)
      return cfg.field("name");

    return "ext:" + iMember.getUuid();
  }

  public boolean updateCachedDatabaseConfiguration(final String iDatabaseName, final ODocument cfg, final boolean iSaveToDisk) {
    final ODistributedStorage stg = storages.get(iDatabaseName);
    if (stg == null)
      return false;

    final ODistributedConfiguration dCfg = stg.getDistributedConfiguration();

    ODocument oldCfg = dCfg != null ? dCfg.getDocument() : null;
    Integer oldVersion = oldCfg != null ? (Integer) oldCfg.field("version") : null;
    if (oldVersion == null)
      oldVersion = 0;

    Integer currVersion = (Integer) cfg.field("version");
    if (currVersion == null)
      currVersion = 1;

    final boolean modified = currVersion >= oldVersion;

    if (oldCfg != null && oldVersion > currVersion) {
      // NO CHANGE, SKIP IT
      OLogManager.instance().debug(this,
          "Skip saving of distributed configuration file for database '%s' because is unchanged (version %d)", iDatabaseName,
          (Integer) cfg.field("version"));
      return false;
    }

    // SAVE IN NODE'S LOCAL RAM
    final ODistributedConfiguration newCfg = new ODistributedConfiguration(cfg);
    stg.setDistributedConfiguration(newCfg);

    // PRINT THE NEW CONFIGURATION
    // TODO: AVOID DUMPING IT EVERY TIME
    final String cfgOutput = ODistributedOutput.formatClusterTable(this, iDatabaseName, newCfg, getAvailableNodes(iDatabaseName));

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "New distributed configuration for database: %s (version=%d)%s\n", iDatabaseName, cfg.field("version"), cfgOutput);

    if (iSaveToDisk) {
      // SAVE THE CONFIGURATION TO DISK
      FileOutputStream f = null;
      try {
        File file = getDistributedConfigFile(iDatabaseName);

        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Saving distributed configuration file for database '%s' to: %s", iDatabaseName, file);

        if (!file.exists()) {
          file.getParentFile().mkdirs();
          file.createNewFile();
        }

        f = new FileOutputStream(file);
        f.write(cfg.toJSON().getBytes());
        f.flush();
      } catch (Exception e) {
        ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Error on saving distributed configuration file", e);

      } finally {
        if (f != null)
          try {
            f.close();
          } catch (IOException e) {
          }
      }
    }

    if (modified) {
      serverInstance.getClientConnectionManager().pushDistribCfg2Clients(getClusterConfiguration());
      dumpServersStatus();
    }

    return modified;
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName) {
    ODistributedConfiguration dCfg = null;

    final ODistributedStorage stg = getStorage(iDatabaseName);
    dCfg = stg.getDistributedConfiguration();
    if (dCfg != null)
      return dCfg;

    // LOAD FILE IN DATABASE DIRECTORY IF ANY
    final File specificDatabaseConfiguration = getDistributedConfigFile(iDatabaseName);
    ODocument cfg = loadDatabaseConfiguration(iDatabaseName, specificDatabaseConfiguration, false);

    if (cfg == null) {
      // FIRST TIME RUNNING: GET DEFAULT CFG
      cfg = loadDatabaseConfiguration(iDatabaseName, defaultDatabaseConfigFile, true);
      if (cfg == null)
        throw new OConfigurationException("Cannot load default distributed database config file: " + defaultDatabaseConfigFile);
    }

    return stg.getDistributedConfiguration();
  }

  public File getDistributedConfigFile(final String iDatabaseName) {
    return new File(serverInstance.getDatabaseDirectory() + iDatabaseName + "/" + FILE_DISTRIBUTED_DB_CONFIG);
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  protected ODocument loadDatabaseConfiguration(final String iDatabaseName, final File file, final boolean writeCfgToDisk) {
    if (!file.exists() || file.length() == 0)
      return null;

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "loaded database configuration from disk: %s", file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 1);
      updateCachedDatabaseConfiguration(iDatabaseName, doc, writeCfgToDisk);
      return doc;

    } catch (Exception e) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Error on loading distributed configuration file in: %s", e, file.getAbsolutePath());
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  @Override
  public ODocument getClusterConfiguration() {
    if (!enabled)
      return null;

    final ODocument cluster = new ODocument();

    cluster.field("localName", getName());
    cluster.field("localId", nodeUuid);

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    // members.add(getLocalNodeConfiguration());
    for (Member member : activeNodes.values()) {
      members.add(getNodeConfigurationByUuid(member.getUuid()));
    }

    return cluster;
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

    final List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
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
  public ODistributedResponse sendRequest(final String iDatabaseName, final Collection<String> iClusterNames,
      final Collection<String> iTargetNodes, final ORemoteTask iTask, final long reqId,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback) {

    final ODistributedRequest req = new ODistributedRequest(taskFactory, nodeId, reqId, iDatabaseName, iTask, iExecutionMode);

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
      final ODatabaseDocumentInternal database) {
    if (database != null && !(database.getStorage() instanceof ODistributedStorage))
      throw new ODistributedException("Distributed storage was not installed for database '" + database.getName()
          + "'. Implementation found: " + database.getStorage().getClass().getName());

    final ODistributedAbstractPlugin manager = this;

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

      ODocument cfg = getNodeConfigurationByUuid(member.getUuid());
      while (cfg == null || cfg.field("listeners") == null ) {
        try {
          Thread.sleep(100);
          cfg = getNodeConfigurationByUuid(member.getUuid());

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ODistributedException("Cannot find node '" + rNodeName + "'");
        }
      }

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
  public void onLocalNodeConfigurationRequest(final ODocument iConfiguration) {
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed())
      return;

    // RUN ONLY IN NON-DISTRIBUTED MODE
    if (!isRelatedToLocalServer(iDatabase))
      return;

    final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    installClustersOfClass(iDatabase, iClass);
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
      final Integer val = registeredNodeByName.get(name);
      if (val == null)
        return -1;
      return val.intValue();
    }
  }

  @Override
  public void updateLastClusterChange() {
    lastClusterChangeOn = System.currentTimeMillis();
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
    return executeInDistributedDatabaseLock(databaseName, new OCallable<Boolean, ODistributedConfiguration>() {
      @Override
      public Boolean call(final ODistributedConfiguration cfg) {
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
    });
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
      return executeInDistributedDatabaseLock(databaseName, new OCallable<Boolean, ODistributedConfiguration>() {
        @Override
        public Boolean call(final ODistributedConfiguration cfg) {
          return rebalanceClusterOwnership(iNode, database, cfg, clustersWithNotAvailableOwner, rebalance);
        }
      });
    } finally {
      database.close();
    }
  }

  protected String getCoordinatorServer(final ODistributedConfiguration cfg) {
    final List<String> servers = cfg.getServers("*");
    if (servers == null || servers.isEmpty())
      return null;

    String next = servers.get(0);
    if (ODistributedConfiguration.NEW_NODE_TAG.equals(next)) {
      // GET THE NEXT ONE
      next = servers.size() > 1 ? servers.get(1) : null;
    }

    return next;
  }

  /**
   * Returns the next eligible server coordinator (2nd in server list).
   */
  private String getNextEligibleServerCoordinator(final ODistributedConfiguration cfg) {
    final List<String> servers = cfg.getServers("*");
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
  public boolean isNodeAvailable(final String iNodeName, final String iDatabaseName) {
    return getDatabaseStatus(iNodeName, iDatabaseName) != DB_STATUS.OFFLINE;
  }

  @Override
  public boolean isNodeAvailable(final String iNodeName) {
    if (iNodeName == null)
      return false;
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


  @Override
  public String toString() {
    return nodeName;
  }

  @Override
  public ODistributedMessageServiceImpl getMessageService() {
    return messageService;
  }

  public long getLastClusterChangeOn() {
    return lastClusterChangeOn;
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

  public boolean installDatabase(final boolean iStartup, final String databaseName, final ODocument config) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

    ODistributedServerLog.info(this, nodeName, null, DIRECTION.NONE, "Current node started as %s for database '%s'",
        cfg.getServerRole(nodeName), databaseName);

    final Set<String> configuredDatabases = serverInstance.getAvailableStorageNames().keySet();
    if (!iStartup && configuredDatabases.contains(databaseName))
      return false;

    final ODistributedDatabaseImpl distrDatabase = messageService.registerDatabase(databaseName);

    final Boolean autoDeploy = config.field("autoDeploy");
    if (autoDeploy == null || !autoDeploy) {
      // NO AUTO DEPLOY
      setDatabaseStatus(nodeName, databaseName, DB_STATUS.ONLINE);
      return false;
    }

    boolean databaseInstalled;

    // CREATE THE DISTRIBUTED QUEUE
    if (!distrDatabase.exists() || distrDatabase.getSyncConfiguration().isEmpty()) {

      // FIRST TIME, ASK FOR FULL REPLICA
      databaseInstalled = requestFullDatabase(distrDatabase, databaseName, iStartup);

    } else {
      try {

        // TRY WITH DELTA SYNC
        databaseInstalled = requestDatabaseDelta(distrDatabase, databaseName);

      } catch (ODistributedDatabaseDeltaSyncException e) {
        // FALL BACK TO FULL BACKUP
        databaseInstalled = requestFullDatabase(distrDatabase, databaseName, iStartup);
      }
    }

    return databaseInstalled;
  }

  protected boolean requestFullDatabase(ODistributedDatabaseImpl distrDatabase, String databaseName, boolean backupDatabase) {
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

    getAvailableNodes(nodes, databaseName);

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
      final List<String> targetNodes = new ArrayList<String>(1);
      targetNodes.add(entry.getKey());

      ODistributedServerLog.info(this, nodeName, entry.getKey(), DIRECTION.OUT, "Requesting database delta sync for '%s' LSN=%s...",
          databaseName, entry.getValue());

      final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, targetNodes, deployTask,
          getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null).getPayload();

      ODistributedServerLog.info(this, nodeName, entry.getKey(), DIRECTION.IN, "Receiving delta sync for '%s'...", databaseName);

      ODistributedServerLog.debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Database delta sync returned: %s",
          results);

      final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

      // EXTRACT THE REAL RESULT
      for (Map.Entry<String, Object> r : results.entrySet()) {
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
            OCommandExecutorSQLHASyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
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

    if (nodes.isEmpty()) {
      ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE,
          "Cannot request full deploy of database '%s' because there are no nodes available with such database", databaseName);
      return false;
    }

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
        "Requesting deploy of database '%s' on local server...", databaseName);

    final OAbstractReplicatedTask deployTask = new OSyncDatabaseTask();

    final Map<String, Object> results = (Map<String, Object>) sendRequest(databaseName, null, selectedNodes, deployTask,
        getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null).getPayload();

    ODistributedServerLog.debug(this, nodeName, selectedNodes.toString(), DIRECTION.OUT, "Deploy returned: %s", results);

    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    // EXTRACT THE REAL RESULT
    for (Map.Entry<String, Object> r : results.entrySet()) {
      final Object value = r.getValue();

      if (value instanceof Boolean)
        continue;
      else if (value instanceof Throwable) {
        ODistributedServerLog.error(this, nodeName, r.getKey(), DIRECTION.IN, "Error on installing database '%s' in %s",
            (Exception) value, databaseName, dbPath);
      } else if (value instanceof ODistributedDatabaseChunk) {
        if (backupDatabase)
          backupCurrentDatabase(databaseName);

        final Set<String> toSyncClusters = installDatabaseFromNetwork(dbPath, databaseName, distrDatabase, r.getKey(),
            (ODistributedDatabaseChunk) value, false);

        // SYNC ALL THE CLUSTERS
        for (String cl : toSyncClusters) {
          // FILTER CLUSTER CHECKING IF ANY NODE IS ACTIVE
          OCommandExecutorSQLHASyncCluster.replaceCluster(this, serverInstance, databaseName, cl);
        }

        return true;

      } else
        throw new IllegalArgumentException("Type " + value + " not supported");
    }

    throw new ODistributedException("No response received from remote nodes for auto-deploy of database '" + databaseName + "'");
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
                    getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

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
      try {
        executeInDistributedDatabaseLock(databaseName, new OCallable<Void, ODistributedConfiguration>() {
          @Override
          public Void call(final ODistributedConfiguration cfg) {
            if (db.isClosed())
              getServerInstance().openDatabase(db);

            db.reload();
            db.getMetadata().reload();

            rebalanceClusterOwnership(nodeName, db, cfg, new HashSet<String>(), true);

            distrDatabase.setOnline();
            return null;
          }
        });
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }
    }

    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);

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

  @Override
  public ORemoteTaskFactory getTaskFactory() {
    return taskFactory;
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
  public synchronized boolean installClustersOfClass(final ODatabaseInternal iDatabase, final OClass iClass) {

    final String databaseName = iDatabase.getName();

    if (!(iClass.getClusterSelection() instanceof OLocalClusterStrategy))
      // INJECT LOCAL CLUSTER STRATEGY
      ((OClassImpl) iClass).setClusterSelectionInternal(new OLocalClusterStrategy(this, databaseName, iClass));

    if (iClass.isAbstract())
      return false;

    final int[] clusterIds = iClass.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int clusterId : clusterIds)
      clusterNames.add(iDatabase.getClusterNameById(clusterId));

    return executeInDistributedDatabaseLock(databaseName, new OCallable<Boolean, ODistributedConfiguration>() {
      @Override
      public Boolean call(final ODistributedConfiguration lastCfg) {
        final Set<String> availableNodes = getAvailableNodeNames(iDatabase.getName());

        return clusterAssignmentStrategy.assignClusterOwnershipOfClass(iDatabase, lastCfg, iClass, availableNodes,
            new HashSet<String>(), true);
      }
    });
  }

  /**
   * Executes an operation protected by a distributed lock (one per database).
   *
   * @param databaseName
   *          Database name
   * @param iCallback
   *          Operation
   * @param <T>
   *          Return type
   * @return The operation's result of type T
   */
  public <T> T executeInDistributedDatabaseLock(final String databaseName,
      final OCallable<T, ODistributedConfiguration> iCallback) {
    if (OScenarioThreadLocal.INSTANCE.isInDatabaseLock()) {
      // ALREADY IN LOCK
      final ODistributedConfiguration lastCfg = getDatabaseConfiguration(databaseName);
      return (T) iCallback.call(lastCfg);
    }

    final Lock lock = getLock(databaseName + ".cfg");
    lock.lock();
    try {

      OScenarioThreadLocal.INSTANCE.setInDatabaseLock(true);

      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration lastCfg = getLastDatabaseConfiguration(databaseName);
      final int cfgVersion = lastCfg.getVersion();

      try {

        return (T) iCallback.call(lastCfg);

      } finally {
        if (lastCfg.getVersion() > cfgVersion)
          // CONFIGURATION CHANGED, UPDATE IT ON THE CLUSTER AND DISK
          updateCachedDatabaseConfiguration(databaseName, lastCfg.getDocument(), true, true);

        OScenarioThreadLocal.INSTANCE.setInDatabaseLock(false);
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);

    } finally {
      lock.unlock();
    }
  }

  protected void onDatabaseEvent(final ODocument config, final String databaseName) {
    if (messageService.getDatabase(databaseName) != null) {
      executeInDistributedDatabaseLock(databaseName, new OCallable<Object, ODistributedConfiguration>() {
        @Override
        public Object call(ODistributedConfiguration iArgument) {
          // DATABASE IS CONFIGURED ON LOCAL NODE, UPDATE THE CFG ON DISK ONLY
          updateCachedDatabaseConfiguration(databaseName, config, true, false);
          return null;
        }
      });
    }

    installDatabase(false, databaseName, config);
    dumpServersStatus();
  }

  protected void onDatabaseEvent(final String nodeName, final String databaseName, final DB_STATUS status) {
    updateLastClusterChange();
    dumpServersStatus();
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

      try {
        final ODistributedAbstractPlugin me = this;
        executeInDistributedDatabaseLock(databaseName, new OCallable<Void, ODistributedConfiguration>() {
          @Override
          public Void call(final ODistributedConfiguration cfg) {
            try {
              if (delta) {

                new OIncrementalServerSync().importDelta(serverInstance, db, in, iNode);

              } else {

                // IMPORT FULL DATABASE (LISTENER ONLY FOR DEBUG PURPOSE)
                db.restore(in, null, null, ODistributedServerLog.isDebugEnabled() ? me : null);

              }
              return null;
            } catch (IOException e) {
              throw OException.wrapException(new OIOException("Error on distributed sync of database"), e);
            }
          }
        });
      } finally {
        in.close();
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
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n"))
      iText = iText.substring(2);
    else if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  public void stopNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of stopping node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(taskFactory, nodeId, getNextMessageIdCounter(), null,
        new OStopServerTask(), ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);

    getRemoteServer(iNode).sendRequest(request, iNode);
  }

  public void restartNode(final String iNode) throws IOException {
    ODistributedServerLog.warn(this, nodeName, null, DIRECTION.NONE, "Sending request of restarting node '%s'...", iNode);

    final ODistributedRequest request = new ODistributedRequest(taskFactory, nodeId, getNextMessageIdCounter(), null,
        new ORestartServerTask(), ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);

    getRemoteServer(iNode).sendRequest(request, iNode);
  }

  public Set<String> getAvailableNodeNames(final String iDatabaseName) {
    final Set<String> nodes = new HashSet<String>();

    for (Map.Entry<String, Member> entry : activeNodes.entrySet()) {
      if (isNodeAvailable(entry.getKey(), iDatabaseName))
        nodes.add(entry.getKey());
    }
    return nodes;
  }

  public long getNextMessageIdCounter() {
    final long v = localMessageIdCounter.getAndIncrement();
    return v;
  }

  protected void closeRemoteServer(final String node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  protected boolean isRelatedToLocalServer(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    // Check for the system database.
    if (iDatabase.getName().equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME)) return false;

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

  /**
   * Avoids to dump the same configuration twice if it's unchanged since the last time.
   */
  protected void dumpServersStatus() {
    final String dump = ODistributedOutput.formatServerStatus(this, getClusterConfiguration());

    final int hashServerDump = dump.hashCode();

    if (hashServerDump != hashLastServerDump) {
      hashLastServerDump = hashServerDump;
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Distributed servers status:\n%s", dump);
    }
  }

  protected ODistributedStorage getStorage(final String dbName) {
    ODistributedStorage storage = storages.get(dbName);
    if (storage == null) {
      storage = new ODistributedStorage(serverInstance);

      final ODistributedStorage oldStorage = storages.putIfAbsent(dbName, storage);
      if (oldStorage != null)
        storage = oldStorage;
    }
    return storage;
  }
}
