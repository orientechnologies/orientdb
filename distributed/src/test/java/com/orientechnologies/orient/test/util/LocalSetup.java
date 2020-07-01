package com.orientechnologies.orient.test.util;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalSetup implements TestSetup {
  private Map<String, OServer> servers = new HashMap<>();
  private Map<String, String> httpRemotes = new HashMap<>();
  private Map<String, String> binaryRemotes = new HashMap<>();
  private TestConfig config;

  public LocalSetup(TestConfig config) {
    this.config = config;
  }

  @Override
  public void startServer(String serverId) throws OTestSetupException {
    OServer server;
    try {
      server = OServer.startFromClasspathConfig(config.getLocalConfigFile(serverId));
    } catch (ClassNotFoundException
        | InstantiationException
        | IOException
        | IllegalAccessException e) {
      throw new OTestSetupException(e);
    }
    servers.put(serverId, server);
    server
        .getNetworkListeners()
        .forEach(
            listener -> {
              String address = String.format("localhost:%d", listener.getInboundAddr().getPort());
              if (listener.getProtocolType().equals(ONetworkProtocolBinary.class)) {
                binaryRemotes.put(serverId, address);
              } else if (listener.getProtocolType().equals(ONetworkProtocolHttpDb.class)) {
                httpRemotes.put(serverId, address);
              }
            });
  }

  @Override
  public void start() {
    for (String serverId : config.getServerIds()) {
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
    // todo: pass custom teardown
    String address = "remote:" + getAddress(config.getServerIds().get(0), PortType.BINARY);
    OrientDB remote = new OrientDB(address, "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("test");
    remote.close();
    for (OServer server : servers.values()) server.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

  @Override
  public String getAddress(String serverId, PortType port) {
    switch (port) {
      case HTTP:
        return httpRemotes.get(serverId);
      case BINARY:
        return binaryRemotes.get(serverId);
    }
    return null;
  }

  @Override
  public TestConfig getSetupConfig() {
    return config;
  }
}
