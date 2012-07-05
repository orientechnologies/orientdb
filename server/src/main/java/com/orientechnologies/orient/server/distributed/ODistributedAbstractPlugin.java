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
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

/**
 * Abstract plugin to manage the distributed environment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODistributedAbstractPlugin extends OServerHandlerAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener {
  private static final String                 PAR_DEF_DISTRIB_DB_CONFIG  = "default.distributed.db";
  private static final String                 FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";
  public static final String                  REPLICATOR_USER            = "replicator";
  protected OServer                           serverInstance;
  protected boolean                           enabled                    = true;
  protected String                            alias                      = null;
  protected long                              offlineBuffer              = -1;
  protected Map<String, OStorageSynchronizer> synchronizers              = new HashMap<String, OStorageSynchronizer>();
  protected Map<String, ODocument>            databaseConfiguration      = new HashMap<String, ODocument>();

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
      else if (param.name.startsWith(PAR_DEF_DISTRIB_DB_CONFIG))
        if (loadDatabaseConfiguration("*", OSystemVariableResolver.resolveSystemVariables(param.value)) == null)
          throw new OConfigurationException("Error on loading distributed database configuration");
    }

    // CHECK THE CONFIGURATION
    if (!databaseConfiguration.containsKey("*"))
      throw new OConfigurationException("Invalid cluster configuration: cannot find settings '" + PAR_DEF_DISTRIB_DB_CONFIG
          + "' for the default database");

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

    super.startup();
    Orient.instance().addDbLifecycleListener(this);
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
    final ODocument cfg = getDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    final Boolean synch = (Boolean) cfg.field("synchronization");
    if (synch == null || synch) {
      getDatabaseSynchronizer(iDatabase.getName());

      if (iDatabase instanceof ODatabaseComplex<?>)
        ((ODatabaseComplex<?>) iDatabase).replaceStorage(new ODistributedStorage(this,
            (OStorageEmbedded) ((ODatabaseComplex<?>) iDatabase).getStorage()));
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

  public ODocument getDatabaseConfiguration(final String iDatabaseName) {
    // NOT FOUND: GET BY CONFIGURATION ON LOCAL NODE
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

  public void setDefaultDatabaseConfiguration(final String iDatabaseName, final ODocument iConfiguration) {
    databaseConfiguration.put(iDatabaseName, iConfiguration);
  }

  public long getOfflineBuffer() {
    return offlineBuffer;
  }

  public void setOfflineBuffer(long offlineBuffer) {
    this.offlineBuffer = offlineBuffer;
  }

  public OStorageSynchronizer getDatabaseSynchronizer(final String iDatabaseName) {
    synchronized (synchronizers) {
      OStorageSynchronizer sync = synchronizers.get(iDatabaseName);
      if (sync == null) {
        try {
          sync = new OStorageSynchronizer(this, iDatabaseName);
        } catch (IOException e) {
          throw new ODistributedException("Cannot get the storage synchronizer for database " + iDatabaseName, e);
        }
        synchronizers.put(iDatabaseName, sync);
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

      ODocument doc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
      databaseConfiguration.put(iDatabaseName, doc);
      return doc;

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
    File file = new File(serverInstance.getDatabaseDirectory() + iDatabaseName + "/" + FILE_DISTRIBUTED_DB_CONFIG);

    databaseConfiguration.put(iDatabaseName, cfg);

    OLogManager.instance().config(this, "Saving distributed configuration for database '%s'", iDatabaseName);

    FileOutputStream f = null;
    try {
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
