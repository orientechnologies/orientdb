/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.ONodeListenerConfig;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStartupException;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.hazelcast.OHazelcastClusterMetadataManager;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import sun.misc.Signal;

/**
 * Plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedPlugin implements OServerPlugin, ODistributedServerManager {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODistributedPlugin.class);

  protected static final String PAR_DEF_DISTRIB_DB_CONFIG = "configuration.db.default";
  protected static final String NODE_NAME_ENV = "ORIENTDB_NODE_NAME";

  protected boolean enabled = true;
  private OServer serverInstance;
  private String nodeName = null;
  protected File defaultDatabaseConfigFile;

  protected static final int DEPLOY_DB_MAX_RETRIES = 10;
  protected Set<String> installingDatabases =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  private volatile String lastServerDump = "";

  private OCancellableTimer haStatsTask = null;
  protected OSignalHandler.OSignalListener signalListener;

  private final OHazelcastClusterMetadataManager clusterManager;

  public ODistributedPlugin() {
    clusterManager = new OHazelcastClusterMetadataManager(this);
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    serverInstance = oServer;
    oServer.setVariable("ODistributedAbstractPlugin", this);

    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(
            OSystemVariableResolver.resolveSystemVariables(param.getValue()))) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.getName().equalsIgnoreCase("nodeName")) {
        nodeName = param.getValue();
        if (nodeName.contains("."))
          throw new OConfigurationException(
              "Illegal node name '" + nodeName + "'. '.' is not allowed in node name");
      } else if (param.getName().startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        setDefaultDatabaseConfigFile(param.getValue());
      }
    }

    try {
      clusterManager.configHazelcastPlugin(oServer, iParams, nodeName);
    } catch (FileNotFoundException e) {
      throw OException.wrapException(
          new ODatabaseException("Error loading hazelcast configuration"), e);
    }
    String contextName = clusterManager.getHazelcastConfig().getGroupConfig().getName();
    String contextPassword = clusterManager.getHazelcastConfig().getGroupConfig().getName();
    if (nodeName == null) assignNodeName();
    ((OrientDBDistributed) serverInstance.getDatabases())
        .initDistributed(nodeName, contextName, 1, contextPassword);
  }

  public File getDefaultDatabaseConfigFile() {
    return defaultDatabaseConfigFile;
  }

  public void setDefaultDatabaseConfigFile(final String iFile) {
    defaultDatabaseConfigFile = new File(OSystemVariableResolver.resolveSystemVariables(iFile));
    if (!defaultDatabaseConfigFile.exists())
      throw new OConfigurationException(
          "Cannot find distributed database config file: " + defaultDatabaseConfigFile);
  }

  @Override
  public void startup() {
    if (!enabled) return;
    OrientDBInternal databases = serverInstance.getDatabases();
    if (databases instanceof OrientDBDistributed) ((OrientDBDistributed) databases).setPlugin(this);

    // REGISTER TEMPORARY USER FOR REPLICATION PURPOSE
    try {
      clusterManager.startupHazelcastPlugin();

      OContextConfiguration ctx = serverInstance.getContextConfiguration();
      final long statsDelay = ctx.distributedDumpStatsEvery();
      if (statsDelay > 0) {
        haStatsTask = databases.periodicExecute(this::dumpStats, statsDelay);
      }

      signalListener =
          new OSignalHandler.OSignalListener() {
            @Override
            public void onSignal(final Signal signal) {
              if (signal.toString().trim().equalsIgnoreCase("SIGTRAP")) dumpStats();
            }
          };
      Orient.instance().getSignalHandler().registerListener(signalListener);
    } catch (Exception e) {
      logger.errorNode(nodeName, "Error on starting distributed plugin", e);
      throw OException.wrapException(
          new ODistributedStartupException("Error on starting distributed plugin"), e);
    }

    dumpServersStatus();
  }

  @Override
  public void shutdown() {
    if (!enabled) return;
    OSignalHandler signalHandler = Orient.instance().getSignalHandler();
    if (signalHandler != null) signalHandler.unregisterListener(signalListener);

    logger.warnNode(nodeName, "Shutting down node '%s'...", nodeName);

    clusterManager.prepareHazelcastPluginShutdown();
    if (haStatsTask != null) haStatsTask.cancel();

    clusterManager.hazelcastPluginShutdown();
  }

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public void sendShutdown() {
    shutdown();
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public String toString() {
    return nodeName;
  }

  protected void assignNodeName() {
    // ORIENTDB_NODE_NAME ENV VARIABLE OR JVM SETTING
    nodeName = OSystemVariableResolver.resolveVariable(NODE_NAME_ENV);

    if (nodeName != null) {
      nodeName = nodeName.trim();
      if (nodeName.isEmpty()) nodeName = null;
    }

    if (nodeName == null) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |         WARNING: FIRST DISTRIBUTED RUN CONFIGURATION          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | This is the first time that the server is running as          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | distributed. Please type the name you want to assign to the   |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | current server node.                                          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |                                                               |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | setting ORIENTDB_NODE_NAME to the server node name to use.    |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.print(OAnsiCode.format("\n$ANSI{yellow Node name [BLANK=auto generate it]: }"));

      OConsoleReader reader = new ODefaultConsoleReader();
      try {
        nodeName = reader.readLine();
      } catch (IOException e) {
      }
      if (nodeName != null) {
        nodeName = nodeName.trim();
        if (nodeName.isEmpty()) nodeName = null;
      }
    }

    if (nodeName == null)
      // GENERATE NODE NAME
      this.nodeName = "node" + System.currentTimeMillis();

    logger.warnNode("Assigning distributed node name: %s", this.nodeName);

    // SALVE THE NODE NAME IN CONFIGURATION
    boolean found = false;
    final OServerConfiguration cfg = serverInstance.getConfiguration();
    for (OServerHandlerConfiguration h : cfg.getHandlers()) {
      if (h.getClazz().equals(getClass().getName())) {
        for (OServerParameterConfiguration p : h.getParameters()) {
          if (p.getName().equals("nodeName")) {
            found = true;
            p.setValue(this.nodeName);
            break;
          }
        }

        if (!found) {
          h.setParameters(OArrays.copyOf(h.getParameters(), h.getParameters().length + 1));
          h.getParameters()[h.getParameters().length - 1] =
              new OServerParameterConfiguration("nodeName", this.nodeName);
        }

        try {
          serverInstance.saveConfiguration();
        } catch (IOException e) {
          throw OException.wrapException(
              new OConfigurationException("Cannot save server configuration"), e);
        }
        break;
      }
    }
  }

  public void closeRemoteServer(final ONodeId node) {
    ((OrientDBDistributed) this.serverInstance.getDatabases()).closeRemoteServer(node);
  }

  /** Avoids to dump the same configuration twice if it's unchanged since the last time. */
  public void dumpServersStatus() {
    final OClusterConfiguration cfg = getClusterConfiguration();

    final String compactStatus =
        ODistributedOutput.getCompactServerStatus(
            (OrientDBDistributed) this.getServerInstance().getDatabases(), cfg);

    if (!lastServerDump.equals(compactStatus)) {
      lastServerDump = compactStatus;

      //      logger.infoNode(
      //          getLocalNodeName(),
      //          "Distributed servers status (*=current):\n%s",
      //          ODistributedOutput.formatServerStatus(this, cfg));
    }
  }

  public static String getListeningBinaryAddress(final ONodeConfig cfg) {
    if (cfg == null) return null;

    final Collection<ONodeListenerConfig> listeners = cfg.getListeners();
    if (listeners == null) return null;
    String listenUrl = null;
    for (ONodeListenerConfig listener : listeners) {
      if (listener.getProtocol().equals("ONetworkProtocolBinary")) {
        listenUrl = listener.getListen();
        break;
      }
    }
    return listenUrl;
  }

  protected void dumpStats() {
    try {
      final OClusterConfiguration clusterCfg = getClusterConfiguration();

      ODatabasesTopology databaseTopology =
          ((OrientDBDistributed) serverInstance.getDatabases())
              .getNodeState()
              .getDatabaseTopology();
      var dbIds = databaseTopology.getDatabases();
      final List<String> dbs =
          new ArrayList<>(
              dbIds.stream().map((dbId) -> databaseTopology.getDatabaseName(dbId)).toList());
      Collections.sort(dbs);
      final StringBuilder buffer = new StringBuilder(8192);

      buffer.append(ODistributedOutput.formatLatency(this.getLocalNodeName(), clusterCfg));
      buffer.append(ODistributedOutput.formatMessages(this.getLocalNodeName(), clusterCfg));

      OLogManager.instance().flush();
      for (String db : dbs) {
        buffer.append(getDatabase(db).dump());
      }

      // DUMP HA STATS
      System.out.println(buffer);

    } catch (Exception e) {
      logger.errorNode(nodeName, "Error on printing HA stats", e);
    }
  }

  public void onNodeJoined(ONodeId joinedNodeId, String url) {
    ((OrientDBDistributed) serverInstance.getDatabases()).connected(joinedNodeId, url);

    // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN ONLY IF AUTO-DEPLOY IS SET
    dumpServersStatus();
  }

  // Called to notify this server, that a node has been removed from the cluster
  public void onServerRemoved(ONodeId nodeName) {
    closeRemoteServer(nodeName);
  }

  @Override
  public OClusterConfiguration getClusterConfiguration() {
    if (!enabled) return null;

    return ((OrientDBDistributed) serverInstance.getDatabases()).getClusterConfiguration();
  }

  @Override
  public ODistributedDatabaseImpl getDatabase(String name) {
    return ((OrientDBDistributed) getServerInstance().getDatabases()).getDatabase(name);
  }
}
