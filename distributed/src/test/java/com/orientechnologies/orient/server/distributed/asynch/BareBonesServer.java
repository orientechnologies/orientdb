package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;

public class BareBonesServer {

  private OServer server;

  public void createDB(String orientUrl) {
    OLogManager.instance().info(this, "creating the database:" + orientUrl);
    ODatabaseDocumentTx graph = new ODatabaseDocumentTx(orientUrl);
    if(!graph.exists()){
      graph.create();
    }else {
      graph.open("admin", "admin");
    }

    OSchema schema = graph.getMetadata().getSchema();
    if(!schema.existsClass("edgetype")){
      schema.createClass("edgetype", schema.getClass("E"));
    }
    if(!schema.existsClass("vertextype")){
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
      if (server.getPluginByClass(OHazelcastPlugin.class) != null)
        server.getPluginByClass(OHazelcastPlugin.class).waitUntilNodeOnline();
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
}
