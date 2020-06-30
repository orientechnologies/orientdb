package com.orientechnologies.orient.test.util;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalSetup implements TestSetup {
  private Map<String, OServer> servers = new HashMap<>();
  private TestConfig           config;

  public LocalSetup(TestConfig config) {
    this.config = config;
  }

  @Override
  public void startServer(String serverId) throws OTestSetupException {
    OServer server;
    try {
      server = OServer.startFromClasspathConfig(config.getLocalConfigFile(serverId));
    } catch (ClassNotFoundException | InstantiationException | IOException | IllegalAccessException e) {
      throw new OTestSetupException(e);
    }
    servers.put(serverId, server);
  }

  @Override public void start() {
    for(String serverId: config.getServerIds()) {
      startServer(serverId);
    }
  }

  @Override
  public void shutdownServer(String serverId) {
    OServer server = servers.get(serverId);
    if (server != null) {
      server.shutdown();
    }
  }

  @Override
  public void teardown() {
    System.out.println("shutdown");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("test");
    remote.close();
    for (OServer server : servers.values()) server.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

  @Override
  public String getRemoteAddress() {
    return "remote:localhost";
  }

  @Override
  public TestConfig getSetupConfig() {
    return config;
  }
}
