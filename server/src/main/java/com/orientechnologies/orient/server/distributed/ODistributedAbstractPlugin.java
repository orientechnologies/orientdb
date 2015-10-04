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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract plugin to manage the distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class ODistributedAbstractPlugin extends OServerPluginAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener {
  public static final String                               REPLICATOR_USER             = "replicator";
  protected static final String                            MASTER_AUTO                 = "$auto";

  protected static final String                            PAR_DEF_DISTRIB_DB_CONFIG   = "configuration.db.default";
  protected static final String                            FILE_DISTRIBUTED_DB_CONFIG  = "distributed-config.json";

  protected OServer                                        serverInstance;
  protected Map<String, ODocument>                         cachedDatabaseConfiguration = new HashMap<String, ODocument>();

  protected boolean                                        enabled                     = true;
  protected String                                         nodeName                    = null;
  protected File                                           defaultDatabaseConfigFile;
  protected ConcurrentHashMap<String, ODistributedStorage> storages                    = new ConcurrentHashMap<String, ODistributedStorage>();

  public static Object runInDistributedMode(final Callable iCall) throws Exception {
    final OScenarioThreadLocal.RUN_MODE currentRunningMode = OScenarioThreadLocal.INSTANCE.get();
    if (currentRunningMode != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
      OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

    try {
      return iCall.call();
    } finally {

      if (currentRunningMode != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
        OScenarioThreadLocal.INSTANCE.set(currentRunningMode);
    }
  }

  public static Object runInDefaultMode(final Callable iCall) throws Exception {
    final OScenarioThreadLocal.RUN_MODE currentRunningMode = OScenarioThreadLocal.INSTANCE.get();
    if (currentRunningMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
      OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);

    try {
      return iCall.call();
    } finally {

      if (currentRunningMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
        OScenarioThreadLocal.INSTANCE.set(currentRunningMode);
    }
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

    if (serverInstance.getUser(REPLICATOR_USER) == null)
      // CREATE THE REPLICATOR USER
      try {
        serverInstance.addUser(REPLICATOR_USER, null, "database.passthrough");
        serverInstance.saveConfiguration();
      } catch (IOException e) {
        throw new OConfigurationException("Error on creating 'replicator' user", e);
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
  public void shutdown() {
    if (!enabled)
      return;

    // CLOSE AND FREE ALL THE STORAGES
    for (ODistributedStorage s : storages.values())
      try {
        s.shutdownAsynchronousWorker();
        s.close();
      } catch (Exception e) {
      }
    storages.clear();

    Orient.instance().removeDbLifecycleListener(this);
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    if (dbUrl.startsWith("plocal:")) {
      // CHECK SPECIAL CASE WITH MULTIPLE SERVER INSTANCES ON THE SAME JVM
      final String dbDirectory = serverInstance.getDatabaseDirectory();
      if (!dbUrl.substring("plocal:".length()).startsWith(dbDirectory))
        // SKIP IT: THIS HAPPENS ONLY ON MULTIPLE SERVER INSTANCES ON THE SAME JVM
        return;
    }

    synchronized (cachedDatabaseConfiguration) {
      final ODistributedConfiguration cfg = getDatabaseConfiguration(iDatabase.getName());
      if (cfg == null)
        return;

      final OStorage dbStorage = iDatabase.getStorage();

      if (iDatabase instanceof ODatabase<?> && dbStorage instanceof OAbstractPaginatedStorage) {
        ODistributedStorage storage = storages.get(iDatabase.getURL());
        if (storage == null) {
          storage = new ODistributedStorage(serverInstance, (OAbstractPaginatedStorage) dbStorage);
          final ODistributedStorage oldStorage = storages.putIfAbsent(iDatabase.getURL(), storage);
          if (oldStorage != null)
            storage = oldStorage;
        }

        iDatabase.replaceStorage(storage);
      }
    }
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    synchronized (cachedDatabaseConfiguration) {
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

  public boolean updateCachedDatabaseConfiguration(final String iDatabaseName, final ODocument cfg, final boolean iSaveToDisk) {
    synchronized (cachedDatabaseConfiguration) {
      ODocument oldCfg = cachedDatabaseConfiguration.get(iDatabaseName);
      Integer oldVersion = oldCfg != null ? (Integer) oldCfg.field("version") : null;
      if (oldVersion == null)
        oldVersion = 0;

      Integer currVersion = (Integer) cfg.field("version");
      if (currVersion == null)
        currVersion = 0;

      final boolean modified = currVersion >= oldVersion;

      if (oldCfg != null && oldVersion > currVersion) {
        // NO CHANGE, SKIP IT
        OLogManager.instance().debug(this,
            "Skip saving of distributed configuration file for database '%s' because is unchanged (version %d)", iDatabaseName,
            (Integer) cfg.field("version"));
        return false;
      }

      // SAVE IN NODE'S LOCAL RAM
      cachedDatabaseConfiguration.put(iDatabaseName, cfg);

      // PRINT THE NEW CONFIGURATION
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
          "updated distributed configuration for database: %s:\n----------\n%s\n----------", iDatabaseName,
          cfg.toJSON("prettyPrint"));

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
      return modified;
    }
  }

  public ODistributedConfiguration getDatabaseConfiguration(final String iDatabaseName) {
    synchronized (cachedDatabaseConfiguration) {
      ODocument cfg = cachedDatabaseConfiguration.get(iDatabaseName);
      if (cfg == null) {

        // LOAD FILE IN DATABASE DIRECTORY IF ANY
        final File specificDatabaseConfiguration = getDistributedConfigFile(iDatabaseName);
        cfg = loadDatabaseConfiguration(iDatabaseName, specificDatabaseConfiguration);

        if (cfg == null) {
          // FIRST TIME RUNNING: GET DEFAULT CFG
          cfg = loadDatabaseConfiguration(iDatabaseName, defaultDatabaseConfigFile);
          if (cfg == null)
            throw new OConfigurationException("Cannot load default distributed database config file: " + defaultDatabaseConfigFile);
        }

        cachedDatabaseConfiguration.put(iDatabaseName, cfg);
      }

      return new ODistributedConfiguration(cfg);
    }
  }

  public File getDistributedConfigFile(final String iDatabaseName) {
    return new File(serverInstance.getDatabaseDirectory() + iDatabaseName + "/" + FILE_DISTRIBUTED_DB_CONFIG);
  }

  public OServer getServerInstance() {
    return serverInstance;
  }

  protected ODocument loadDatabaseConfiguration(final String iDatabaseName, final File file) {
    if (!file.exists() || file.length() == 0)
      return null;

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "loaded database configuration from disk: %s", file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 0);
      updateCachedDatabaseConfiguration(iDatabaseName, doc, false);
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
}
