package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ODistributedConfigurationManager {

  private final OrientDBDistributed context;
  private final ODistributedServerManager distributedManager;
  private volatile ODistributedConfiguration distributedConfiguration;
  private final String databaseName;

  public ODistributedConfigurationManager(
      OrientDBDistributed context, ODistributedServerManager distributedManager, String name) {
    this.context = context;
    this.distributedManager = distributedManager;
    this.databaseName = name;
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    return getDistributedConfiguration(null);
  }

  public ODistributedConfiguration getExisingDistributedConfiguration() {
    return distributedConfiguration;
  }

  public ODistributedConfiguration getDistributedConfiguration(ODatabaseSession session) {
    if (distributedConfiguration == null) {
      loadDistributedConfiguration(session);
    }
    return distributedConfiguration;
  }

  private void loadDistributedConfiguration(ODatabaseSession session) {
    ODocument doc = distributedManager.getOnlineDatabaseConfiguration(databaseName);
    if (doc != null) {
      // DISTRIBUTED CFG AVAILABLE: COPY IT TO THE LOCAL DIRECTORY
      ODistributedServerLog.info(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Downloaded configuration for database '%s' from the cluster",
          databaseName);
      setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
    } else if (!isMemory() && distributedConfigFileExists()) {
      doc = loadConfigurationFromFile(getDistributedConfigFile());
      if (doc == null) {
        doc = loadConfigurationFromFile(distributedManager.getDefaultDatabaseConfigFile());
      }
      if (doc == null) {
        throw new OConfigurationException(
            "Cannot load default distributed for database '"
                + databaseName
                + "' config file: "
                + distributedManager.getDefaultDatabaseConfigFile());
      }

      // SAVE THE GENERIC FILE AS DATABASE FILE
      setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
      // JUST LOAD THE FILE IN MEMORY
      distributedConfiguration = new ODistributedConfiguration(doc);

      // LOADED FILE, PUBLISH IT IN THE CLUSTER
      distributedManager.publishDistributedConfiguration(databaseName, distributedConfiguration);

    } else {
      doc = readDistributedConfiguration(session);
      if (doc == null) {
        doc = loadConfigurationFromFile(distributedManager.getDefaultDatabaseConfigFile());
      }
      if (doc == null) {
        throw new OConfigurationException(
            "Cannot load default distributed for database '"
                + databaseName
                + "' config file: "
                + distributedManager.getDefaultDatabaseConfigFile());
      }
      // JUST LOAD THE FILE IN MEMORY
      distributedConfiguration = new ODistributedConfiguration(doc);

      // LOADED FILE, PUBLISH IT IN THE CLUSTER
      distributedManager.publishDistributedConfiguration(databaseName, distributedConfiguration);
    }
  }

  public void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration) {
    if (this.distributedConfiguration == null
        || distributedConfiguration.getVersion() > this.distributedConfiguration.getVersion()) {
      this.distributedConfiguration =
          new ODistributedConfiguration(distributedConfiguration.getDocument().copy());

      ODistributedServerLog.info(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Setting new distributed configuration for database: %s (version=%d)\n",
          databaseName,
          distributedConfiguration.getVersion());

      saveDatabaseConfiguration();
    }
  }

  public boolean tryUpdatingDatabaseConfigurationLocally(
      final String iDatabaseName, final OModifiableDistributedConfiguration cfg) {

    final ODistributedConfiguration dCfg = getDistributedConfiguration();

    Integer oldVersion = dCfg != null ? dCfg.getVersion() : null;
    if (oldVersion == null) oldVersion = 0;

    int currVersion = cfg.getVersion();

    final boolean modified = currVersion > oldVersion;

    if (dCfg != null && !modified) {
      // NO CHANGE, SKIP IT
      return false;
    }

    // SAVE IN NODE'S LOCAL RAM
    setDistributedConfiguration(cfg);

    return modified;
  }

  public ODocument readDistributedConfiguration(ODatabaseSession session) {

    ODocument config = null;
    try {
      if (session != null) {
        return readConfig(session);
      } else {
        config =
            context
                .executeNoAuthorization(databaseName, ODistributedConfigurationManager::readConfig)
                .get();
      }
    } catch (InterruptedException | ExecutionException e) {
      ODistributedServerLog.error(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on loading distributed configuration",
          e);
    }

    return config;
  }

  private static ODocument readConfig(ODatabaseSession session) {
    OSharedContextEmbedded value =
        (OSharedContextEmbedded) ((ODatabaseDocumentInternal) session).getSharedContext();
    ODocument doc = value.loadDistributedConfig(session);
    return doc;
  }

  public void saveDatabaseConfiguration() {
    context.executeNoAuthorization(
        databaseName,
        (session) -> {
          ODocument doc = distributedConfiguration.getDocument();
          OSharedContextEmbedded value =
              (OSharedContextEmbedded) ((ODatabaseDocumentInternal) session).getSharedContext();
          value.saveConfig(session, "ditributedConfig", doc);
          return null;
        });
    if (!isMemory()) {
      saveDatabaseConfigurationToFile();
    }
  }

  private boolean isMemory() {
    OAbstractPaginatedStorage storage = context.getStorage(databaseName);
    return storage != null && storage.isMemory();
  }

  public ODocument loadConfigurationFromFile(final File file) {
    if (!file.exists() || file.length() == 0) return null;

    ODistributedServerLog.info(
        this,
        distributedManager.getLocalNodeName(),
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Loaded configuration for database '%s' from disk: %s",
        databaseName,
        file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 1);
      return doc;

    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on loading distributed configuration file in: %s",
          e,
          file.getAbsolutePath());
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  public void saveDatabaseConfigurationToFile() {
    // SAVE THE CONFIGURATION TO DISK
    FileOutputStream f = null;
    try {
      File file = getDistributedConfigFile();

      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }

      f = new FileOutputStream(file);
      f.write(distributedConfiguration.getDocument().toJSON().getBytes());
      f.flush();
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on saving distributed configuration file",
          e);

    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
  }

  private boolean distributedConfigFileExists() {
    return getDistributedConfigFile().exists();
  }

  protected File getDistributedConfigFile() {
    return new File(
        distributedManager.getServerInstance().getDatabaseDirectory()
            + databaseName
            + "/"
            + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);
  }
}
