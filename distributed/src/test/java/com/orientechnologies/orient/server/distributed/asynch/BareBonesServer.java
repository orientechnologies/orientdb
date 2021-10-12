package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.server.OServer;
import java.io.File;

public class BareBonesServer {

  private OServer server;

  public void createDB(String databaseName) {
    OLogManager.instance().info(this, "creating the database:" + databaseName);
    if (!server.getContext().exists(databaseName)) {
      server
          .getContext()
          .execute(
              "create database ? plocal users(admin identified by 'admin' role admin)",
              databaseName);
    }
    ODatabaseSession graph = server.getContext().open(databaseName, "admin", "admin");

    OSchema schema = graph.getMetadata().getSchema();
    if (!schema.existsClass("edgetype")) {
      schema.createClass("edgetype", schema.getClass("E"));
    }
    if (!schema.existsClass("vertextype")) {
      schema.createClass("vertextype", schema.getClass("V"));
    }

    graph.close();
  }

  public void start(String configFileDir, String configFileName) {
    OLogManager.instance().info(this, "starting the database based on: " + configFileName);
    try {
      server = new OServer(false);
      server.startup(new File(configFileDir, configFileName));
      server.activate();
      if (server.getDistributedManager() != null)
        server.getDistributedManager().waitUntilNodeOnline();
    } catch (Exception e) {
      OLogManager.instance().error(this, "start", e);
    }
  }

  public void stop() {
    OLogManager.instance().info(this, "stopping the database");
    server.shutdown();
  }

  public void deleteRecursively(final File iRootFile) {
    OLogManager.instance().info(this, "deleting recursively: " + iRootFile.getAbsolutePath());
    OFileUtils.deleteRecursively(iRootFile);
  }

  public OServer getServer() {
    return server;
  }
}
