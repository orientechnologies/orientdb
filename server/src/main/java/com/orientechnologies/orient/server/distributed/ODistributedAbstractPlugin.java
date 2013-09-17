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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Abstract plugin to manage the distributed environment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODistributedAbstractPlugin extends OServerPluginAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener {
  public static final String                              REPLICATOR_USER            = "replicator";
  protected static final String                           MASTER_AUTO                = "$auto";

  protected static final String                           PAR_DEF_DISTRIB_DB_CONFIG  = "configuration.db.default";
  protected static final String                           FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";

  protected OServer                                       serverInstance;
  protected Map<String, OStorageSynchronizer>             synchronizers              = new HashMap<String, OStorageSynchronizer>();
  protected Map<String, ODocument>                        databaseConfiguration      = new HashMap<String, ODocument>();

  protected boolean                                       enabled                    = true;
  protected String                                        nodeName                   = null;
  protected Class<? extends OReplicationConflictResolver> confictResolverClass;
  protected boolean                                       alignmentStartup;
  protected int                                           alignmentTimer;
  protected Map<String, ODistributedPartitioningStrategy> strategies                 = new HashMap<String, ODistributedPartitioningStrategy>();

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
      } else if (param.name.equalsIgnoreCase("nodeName"))
        nodeName = param.value;
      else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG)) {
        final String path = OSystemVariableResolver.resolveSystemVariables(param.value);
        if (loadDatabaseConfiguration("*", path) == null)
          throw new OConfigurationException("Error on loading distributed database configuration from " + path);
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
          strategies.put(param.name.substring("replication.strategy.".length()),
              (ODistributedPartitioningStrategy) Class.forName(param.value).newInstance());
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

  @Override
  public void onCreate(final ODatabase iDatabase) {
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

        if (dbSynchronizer != null && iDatabase instanceof ODatabaseComplex<?>
            && !(iDatabase.getStorage() instanceof ODistributedStorage))
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
    return nodeName;
  }

  public ODistributedPartitioningStrategy getReplicationStrategy(String iStrategy) {
    if (iStrategy.startsWith("$"))
      iStrategy = iStrategy.substring(1);

    final ODistributedPartitioningStrategy strategy = strategies.get(iStrategy);
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

  public ODistributedPartitioningStrategy getStrategy(final String iStrategyName) {
    return strategies.get(iStrategyName);
  }

  public OStorageSynchronizer getDatabaseSynchronizer(final String iDatabaseName) {
    synchronized (synchronizers) {
      OStorageSynchronizer sync = synchronizers.get(iDatabaseName);
      if (sync == null) {
        try {
          sync = new OStorageSynchronizer(serverInstance, this, iDatabaseName);
          synchronizers.put(iDatabaseName, sync);
          sync.config();
          sync.recoverUncommited(this, iDatabaseName);
        } catch (Exception e) {
          synchronizers.remove(iDatabaseName);
          throw new ODistributedException("Cannot get the storage synchronizer for database " + iDatabaseName, e);
        }
      }

      return sync;
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
