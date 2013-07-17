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
package com.orientechnologies.orient.server.distributed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

/**
 * Abstract plugin to manage the distributed environment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODistributedAbstractPlugin extends OServerHandlerAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener {
  public static final String                              REPLICATOR_USER            = "replicator";
  protected static final String                           MASTER_AUTO                = "$auto";

  protected static final String                           PAR_DEF_DISTRIB_DB_CONFIG  = "configuration.db.default";
  protected static final String                           FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";

  protected OServer                                       serverInstance;
  protected Map<String, OStorageSynchronizer>             synchronizers              = new HashMap<String, OStorageSynchronizer>();
  protected Map<String, ODocument>                        databaseConfiguration      = new HashMap<String, ODocument>();

  protected boolean                                       enabled                    = true;
  protected String                                        alias                      = null;
  protected Class<? extends OReplicationConflictResolver> confictResolverClass;
  protected boolean                                       alignmentStartup;
  protected int                                           alignmentTimer;
  protected Map<String, OReplicationStrategy>             strategies                 = new HashMap<String, OReplicationStrategy>();

  @SuppressWarnings("unchecked")
  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    serverInstance = oServer;
    oServer.setVariable("ODistributedAbstractPlugin", this);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value)) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("alias"))
        alias = param.value;
      else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        if (loadDatabaseConfiguration("*", OSystemVariableResolver.resolveSystemVariables(param.value)) == null)
          throw new OConfigurationException("Error on loading distributed database configuration");
      } else if (param.name.equalsIgnoreCase("conflict.resolver.impl"))
        try {
          confictResolverClass = (Class<? extends OReplicationConflictResolver>) Class.forName(param.value);
        } catch (ClassNotFoundException e) {
          OLogManager.instance().error(this, "Cannot find the conflict resolver implementation '%s'", e, param.value);
        }
      else if (param.name.equalsIgnoreCase("alignment.startup"))
        alignmentStartup = Boolean.parseBoolean(param.value);
      else if (param.name.equalsIgnoreCase("alignment.timer"))
        alignmentTimer = Integer.parseInt(param.value);
      else if (param.name.startsWith("replication.strategy.")) {
        try {
          strategies.put(param.name.substring("replication.strategy.".length()), (OReplicationStrategy) Class.forName(param.value)
              .newInstance());
        } catch (Exception e) {
          OLogManager.instance().error(this, "Cannot create replication strategy instance '%s'", e, param.value);

          e.printStackTrace();
        }
      }
    }

    // CHECK THE CONFIGURATION
    synchronized (databaseConfiguration) {
      if (!databaseConfiguration.containsKey("*"))
        throw new OConfigurationException("Invalid cluster configuration: cannot find settings '" + PAR_DEF_DISTRIB_DB_CONFIG
            + "' for the default database");
    }

    if (serverInstance.getUser(REPLICATOR_USER) == null)
      // CREATE THE REPLICATOR USER
      try {
        serverInstance.addUser(REPLICATOR_USER, null, "database.passthrough");
        serverInstance.saveConfiguration();
      } catch (IOException e) {
        throw new OConfigurationException("Error on creating 'replicator' user", e);
      }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    Orient.instance().addDbLifecycleListener(this);
    super.startup();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    Orient.instance().removeDbLifecycleListener(this);
    super.shutdown();
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabase iDatabase) {
    final String dbDirectory = serverInstance.getDatabaseDirectory();
    if (!iDatabase.getURL().substring(iDatabase.getURL().indexOf(":") + 1).startsWith(dbDirectory))
      // NOT OWN DB, SKIPT IT
      return;

    synchronized (databaseConfiguration) {
      final ODocument cfg = getDatabaseConfiguration(iDatabase.getName());
      if (cfg == null)
        return;

      final Boolean synch = (Boolean) cfg.field("synchronization");
      if (synch == null || synch) {
        final OStorageSynchronizer dbSynchronizer = getDatabaseSynchronizer(iDatabase.getName());

        if (iDatabase instanceof ODatabaseComplex<?> && !(iDatabase.getStorage() instanceof ODistributedStorage))
          ((ODatabaseComplex<?>) iDatabase).replaceStorage(new ODistributedStorage(serverInstance, dbSynchronizer,
              (OStorageEmbedded) ((ODatabaseComplex<?>) iDatabase).getStorage()));
      }
    }
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabase iDatabase) {
  }

  @Override
  public void sendShutdown() {
    super.sendShutdown();
  }

  @Override
  public String getName() {
    return "cluster";
  }

  public String getLocalNodeId() {
    return alias;
  }

  public OReplicationStrategy getReplicationStrategy(String iStrategy) {
    if (iStrategy.startsWith("$"))
      iStrategy = iStrategy.substring(1);

    final OReplicationStrategy strategy = strategies.get(iStrategy);
    if (strategy == null)
      throw new ODistributedException("Configured strategy '" + iStrategy + "' is not configured");

    return strategy;

  }

  public ODocument getDatabaseConfiguration(final String iDatabaseName) {
    // NOT FOUND: GET BY CONFIGURATION ON LOCAL NODE
    synchronized (databaseConfiguration) {
      ODocument cfg = databaseConfiguration.get(iDatabaseName);
      if (cfg == null) {
        // TRY LOADING THE DATABASE CONFIG FILE
        cfg = loadDatabaseConfiguration(iDatabaseName, serverInstance.getDatabaseDirectory() + iDatabaseName + "/"
            + FILE_DISTRIBUTED_DB_CONFIG);

        if (cfg == null) {
          // NOT FOUND: GET THE DEFAULT ONE
          cfg = databaseConfiguration.get("*");
          saveDatabaseConfiguration(iDatabaseName, cfg);
        }
      }
      return cfg;
    }
  }

  public void setDefaultDatabaseConfiguration(final String iDatabaseName, final ODocument iConfiguration) {
    synchronized (databaseConfiguration) {
      databaseConfiguration.put(iDatabaseName, iConfiguration);
    }
  }

  public OStorageSynchronizer getDatabaseSynchronizer(final String iDatabaseName) {
    synchronized (synchronizers) {
      OStorageSynchronizer sync = synchronizers.get(iDatabaseName);
      if (sync == null) {
        try {
          sync = new OStorageSynchronizer(serverInstance, this, iDatabaseName);
          synchronizers.put(iDatabaseName, sync);
          sync.recoverUncommited(this, iDatabaseName);
        } catch (IllegalArgumentException e) {
          synchronizers.remove(iDatabaseName);
          return null;
        } catch (IOException e) {
          synchronizers.remove(iDatabaseName);
          throw new ODistributedException("Cannot get the storage synchronizer for database " + iDatabaseName, e);
        }
      }
      return sync;
    }
  }

  public Collection<String> getSynchronousReplicaNodes(final String iDatabaseName, final String iClusterName, final Object iKey) {
    return getReplicaNodes("synch-replicas", iDatabaseName, iClusterName, iKey);
  }

  public Collection<String> getAsynchronousReplicaNodes(final String iDatabaseName, final String iClusterName, final Object iKey) {
    return getReplicaNodes("asynch-replicas", iDatabaseName, iClusterName, iKey);
  }

  @SuppressWarnings("unchecked")
  protected Collection<String> getReplicaNodes(final String iMode, final String iDatabaseName, final String iClusterName,
      final Object iKey) {
    Object replicas = getDatabaseClusterConfiguration(iDatabaseName, iClusterName).field(iMode);
    if (replicas == null)
      // GET THE DEFAULT ONE
      replicas = getDatabaseClusterConfiguration(iDatabaseName, "*").field(iMode);

    if (replicas == null)
      // NO REPLICAS CONFIGURED
      return Collections.emptyList();

    final Set<String> remoteNodes = getRemoteNodeIds();

    final List<String> result = new ArrayList<String>();

    if (replicas instanceof String || replicas instanceof Integer) {
      // DYNAMIC NODES
      int tot = 0;

      if (replicas instanceof String) {
        final String replicasAsText = (String) replicas;
        if (replicasAsText.charAt(replicasAsText.length() - 1) == '%') {
          // PERCENTAGE
          final int perc = Integer.parseInt(replicasAsText.substring(0, replicasAsText.length() - 1));
          tot = Math.round(perc * remoteNodes.size() / 100);
        } else
          // PUNCTUAL
          tot = Integer.parseInt(replicasAsText);
      } else
        tot = (Integer) replicas;

      for (String nodeId : remoteNodes) {
        if (result.size() > tot)
          break;
        result.add(nodeId);
      }
    } else if (replicas instanceof Collection) {
      // STATIC NODE LIST
      final Collection<String> nodeCollection = (Collection<String>) replicas;
      for (String nodeId : nodeCollection) {
        if (remoteNodes.contains(nodeId))
          result.add(nodeId);
      }
    }

    return result;
  }

  protected ODocument getDatabaseClusterConfiguration(final String iDbName, final String iClusterName) {
    synchronized (databaseConfiguration) {
      final ODocument clusters = getDatabaseConfiguration(iDbName).field("clusters");

      if (clusters == null)
        throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

      ODocument cfg = clusters.field(iClusterName);
      if (cfg == null)
        cfg = clusters.field("*");

      return cfg;
    }
  }

  protected ODocument loadDatabaseConfiguration(final String iDatabaseName, final String filePath) {
    File file = new File(filePath);
    if (!file.exists() || file.length() == 0)
      return null;

    OLogManager.instance().config(this, "Loading distributed configuration for database '%s'", iDatabaseName);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      synchronized (databaseConfiguration) {
        final ODocument doc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
        databaseConfiguration.put(iDatabaseName, doc);
        return doc;
      }

    } catch (Exception e) {
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  protected void saveDatabaseConfiguration(final String iDatabaseName, final ODocument cfg) {
    synchronized (databaseConfiguration) {
      final ODocument oldCfg = databaseConfiguration.get(iDatabaseName);
      if (oldCfg != null && Arrays.equals(oldCfg.toStream(), cfg.toStream()))
        // NO CHANGE, SKIP IT
        return;

      databaseConfiguration.put(iDatabaseName, cfg);

      OLogManager.instance().config(this, "Saving distributed configuration for database '%s'", iDatabaseName);

      FileOutputStream f = null;
      try {
        File file = new File(serverInstance.getDatabaseDirectory() + iDatabaseName + "/" + FILE_DISTRIBUTED_DB_CONFIG);
        f = new FileOutputStream(file);
        f.write(cfg.toJSON().getBytes());
      } catch (Exception e) {
      } finally {
        if (f != null)
          try {
            f.close();
          } catch (IOException e) {
          }
      }
    }
  }
}
