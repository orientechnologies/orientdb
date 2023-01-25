package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ODistributedConfigurationManager {

  private final ODistributedServerManager distributedManager;
  private volatile ODistributedConfiguration distributedConfiguration;
  private final String databaseName;

  public ODistributedConfigurationManager(
      ODistributedServerManager distributedManager, String name) {
    this.distributedManager = distributedManager;
    this.databaseName = name;
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    return getDistributedConfiguration(null);
  }

  public ODistributedConfiguration getDistributedConfiguration(ODatabaseSession session) {
    if (distributedConfiguration == null) {
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
      } else {
        doc = loadDatabaseConfiguration(session);
        if (doc == null) {
          // LOOK FOR THE STD FILE
          doc = loadConfiguration();
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
            databaseName, new OModifiableDistributedConfiguration(doc));
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

  public ODocument loadDatabaseConfiguration(ODatabaseSession session) {

    ODocument config = null;
    try {
      if (session != null) {
        return readConfig(session);
      } else {
        OrientDBInternal context = distributedManager.getServerInstance().getDatabases();
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
    OrientDBInternal context = distributedManager.getServerInstance().getDatabases();
    context.executeNoAuthorization(
        databaseName,
        (session) -> {
          ODocument doc = distributedConfiguration.getDocument();
          OSharedContextEmbedded value =
              (OSharedContextEmbedded) ((ODatabaseDocumentInternal) session).getSharedContext();
          value.saveConfig(session, "ditributedConfig", doc);
          return null;
        });
  }

  public ODocument loadConfiguration() {
    final File file = distributedManager.getDefaultDatabaseConfigFile();
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
}
