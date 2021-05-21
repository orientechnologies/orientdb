package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.impl.ODistributedOutput;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class ODistributedConfigurationManager {

  private final ODistributedServerManager distributedManager;
  private volatile ODistributedConfiguration distributedConfiguration;
  private String databaseName;

  public ODistributedConfigurationManager(
      ODistributedServerManager distributedManager, String name) {
    this.distributedManager = distributedManager;
    this.databaseName = name;
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    if (distributedConfiguration == null) {
      final Map<String, Object> map = distributedManager.getConfigurationMap();
      if (map == null) return null;

      ODocument doc = (ODocument) map.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName);
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
      } else {
        doc = loadDatabaseConfiguration(getDistributedConfigFile());
        if (doc == null) {
          // LOOK FOR THE STD FILE
          doc = loadDatabaseConfiguration(distributedManager.getDefaultDatabaseConfigFile());
          if (doc == null)
            throw new OConfigurationException(
                "Cannot load default distributed for database '"
                    + databaseName
                    + "' config file: "
                    + distributedManager.getDefaultDatabaseConfigFile());

          // SAVE THE GENERIC FILE AS DATABASE FILE
          setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
        } else
          // JUST LOAD THE FILE IN MEMORY
          distributedConfiguration = new ODistributedConfiguration(doc);

        // LOADED FILE, PUBLISH IT IN THE CLUSTER
        distributedManager.updateCachedDatabaseConfiguration(
            databaseName, new OModifiableDistributedConfiguration(doc), true);
      }
    }
    return distributedConfiguration;
  }

  public void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration) {
    if (this.distributedConfiguration == null
        || distributedConfiguration.getVersion() > this.distributedConfiguration.getVersion()) {
      this.distributedConfiguration =
          new ODistributedConfiguration(distributedConfiguration.getDocument().copy());

      // PRINT THE NEW CONFIGURATION
      final String cfgOutput =
          ODistributedOutput.formatClusterTable(
              distributedManager,
              databaseName,
              distributedConfiguration,
              distributedManager.getTotalNodes(databaseName));

      ODistributedServerLog.info(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Setting new distributed configuration for database: %s (version=%d)%s\n",
          databaseName,
          distributedConfiguration.getVersion(),
          cfgOutput);

      saveDatabaseConfiguration();
    }
  }

  public ODocument loadDatabaseConfiguration(final File file) {
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

  public void saveDatabaseConfiguration() {
    // SAVE THE CONFIGURATION TO DISK
    FileOutputStream f = null;
    try {
      File file = getDistributedConfigFile();

      ODistributedServerLog.debug(
          this,
          distributedManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Saving distributed configuration file for database '%s' to: %s",
          databaseName,
          file);

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

  protected File getDistributedConfigFile() {
    return new File(
        distributedManager.getServerInstance().getDatabaseDirectory()
            + databaseName
            + "/"
            + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);
  }
}
