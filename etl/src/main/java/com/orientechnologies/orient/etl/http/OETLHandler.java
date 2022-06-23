package com.orientechnologies.orient.etl.http;

import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.util.OMigrationConfigManager;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;

/** Created by gabriele on 27/02/17. */
public class OETLHandler {

  private ExecutorService pool = OThreadPoolExecutors.newSingleThreadPool("OETLHandler");

  private OETLJob currentJob = null;
  private OETLListener listener;

  public OETLHandler(OETLListener listener) {
    this.listener = listener;
  }

  public OETLHandler() {}

  /** Executes import with configuration; */
  public void executeImport(ODocument cfg, OServer server) {

    OETLJob job =
        new OETLJob(
            cfg,
            server,
            new OETLListener() {
              @Override
              public void onEnd(OETLJob etlJob) {
                currentJob = null;
                if (OETLHandler.this.listener != null) {
                  OETLHandler.this.listener.onEnd(etlJob);
                }
              }
            });

    job.validate();

    currentJob = job;
    pool.execute(job);
  }

  /** Checks If the connection with given parameters is alive */
  public void checkConnection(ODocument args) throws Exception {}

  /**
   * Status of the Running Jobs
   *
   * @return ODocument
   */
  public ODocument status() {

    ODocument status = new ODocument();

    Collection<ODocument> jobs = new ArrayList<ODocument>();
    if (currentJob != null) {
      jobs.add(currentJob.status());
    }
    status.field("jobs", jobs);
    return status;
  }

  public void saveConfiguration(ODocument args, OServer server) throws Exception {

    final String outDBName = args.field("outDBName");
    final ODocument migrationConfig =
        new ODocument().fromJSON((String) args.field("migrationConfig"), "noMap");
    final String configName = args.field("configName");
    final String protocol = args.field("protocol");

    if (outDBName == null) {
      throw new IllegalArgumentException("target database name is null.");
    }

    // fetching the server orientdb home and updating coherently the target database URL if we have
    // an orientdb loader
    String outOrientGraphUri = server.getDatabaseDirectory() + outDBName;
    String dbURL = protocol + ":" + outOrientGraphUri;
    ODocument loader = migrationConfig.field("loader");
    if (loader.field("orientdb") != null) {
      ((ODocument) loader.field("orientdb")).field("dbURL", dbURL);
    }

    if (migrationConfig == null) {
      throw new IllegalArgumentException("Migration config is null.");
    }

    OMigrationConfigManager.writeConfigurationInTargetDB(
        migrationConfig, outOrientGraphUri, configName);
  }

  public ODocument listConfigurations(OServer server) throws Exception {

    ODocument database2Configs = new ODocument();

    File serverDBHome = new File(server.getDatabaseDirectory());
    File[] dbDirectories = serverDBHome.listFiles();

    for (int i = 0; i < dbDirectories.length; i++) {

      String dbName = dbDirectories[i].getName();
      File currentETLConfigDir = new File(dbDirectories[i].getAbsolutePath() + "/etl-config/");

      if (currentETLConfigDir.exists()) {
        List<ODocument> configs = new LinkedList<ODocument>();
        File[] currentDBConfigs = currentETLConfigDir.listFiles();

        for (int k = 0; k < currentDBConfigs.length; k++) {

          String currentConfigName = currentDBConfigs[k].getName();

          if (currentConfigName.endsWith(
              ".json")) { // checking if the current file is a json configuration
            ODocument currentConfig = new ODocument();
            currentConfig.field("configName", currentConfigName);

            // reading the config as a text
            Scanner scanner = null;
            String configText = null;
            try {
              scanner = new Scanner(currentDBConfigs[k]);
              configText = scanner.useDelimiter("\\A").next();
            } catch (Exception e) {

            } finally {
              scanner.close();
            }

            currentConfig.field("config", configText);
            configs.add(currentConfig);
          }
        }
        database2Configs.field(dbName, configs);
      }
    }
    return database2Configs;
  }
}
