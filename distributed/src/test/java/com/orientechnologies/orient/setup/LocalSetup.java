package com.orientechnologies.orient.setup;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalSetup implements TestSetup {
  private final Map<String, OServer> servers = new HashMap<>();
  private final Map<String, String> httpRemotes = new HashMap<>();
  private final Map<String, String> binaryRemotes = new HashMap<>();
  private final TestConfig config;

  public LocalSetup(TestConfig config) {
    this.config = config;
  }

  @Override
  public void startServer(String serverId) throws TestSetupException {
    OServer server;
    try {
      server = OServer.startFromClasspathConfig(config.getLocalConfigFile(serverId));
    } catch (ClassNotFoundException
        | InstantiationException
        | IOException
        | IllegalAccessException e) {
      throw new TestSetupException(e);
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
    for (OServer server : servers.values()) server.shutdown();
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
  public OrientDB createRemote(String serverId, OrientDBConfig config) {
    return createRemote(serverId, null, null, config);
  }

  @Override
  public OrientDB createRemote(
      String serverId, String serverUser, String serverPassword, OrientDBConfig config) {
    return new OrientDB(
        "remote:" + getAddress(serverId, PortType.BINARY), serverUser, serverPassword, config);
  }
}
