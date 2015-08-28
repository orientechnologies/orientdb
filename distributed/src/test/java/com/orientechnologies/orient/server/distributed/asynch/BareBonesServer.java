package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import java.io.File;

public class BareBonesServer {

  private OServer server;

  public void createDB(String orientUrl) {
    OLogManager.instance().info(this, "creating the database:" + orientUrl);
    OrientGraphFactory factory = new OrientGraphFactory(orientUrl);
    OrientBaseGraph graph = factory.getTx();
    graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
      @Override
      public Object call(OrientBaseGraph g) {
        g.createEdgeType("edgetype");
        g.createVertexType("vertextype");
        return null;
      }
    });

    graph.shutdown();
    factory.close();
  }

  public void start(String configFileDir, String configFileName) {
    OLogManager.instance().info(this, "starting the database based on: " + configFileName);
    try {
      server = OServerMain.create();
      server.startup(new File(configFileDir, configFileName));
      server.activate();
      if (server.getPluginByClass(OHazelcastPlugin.class) != null)
        server.getPluginByClass(OHazelcastPlugin.class).waitUntilOnline();
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
