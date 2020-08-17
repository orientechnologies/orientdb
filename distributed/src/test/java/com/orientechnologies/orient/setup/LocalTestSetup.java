package com.orientechnologies.orient.setup;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LocalTestSetup implements TestSetup {
  private final Map<String, ServerRun> servers = new HashMap<>();
  private final Map<String, String> httpRemotes = new HashMap<>();
  private final Map<String, String> binaryRemotes = new HashMap<>();
  private String rootDirectory = "target/servers/";
  private final SetupConfig config;

  public LocalTestSetup(SetupConfig config) {
    this.config = config;
    for (String serverId : config.getServerIds()) {
      servers.put(serverId, new ServerRun(rootDirectory, serverId));
    }
  }

  @Override
  public void startServer(String serverId) throws TestSetupException {
    ServerRun server = servers.get(serverId);
    if (server == null) {
      throw new TestSetupException("server '" + serverId + "' not found.");
    }
    try {
      server.startServer(config.getLocalConfigFile(serverId));
    } catch (Exception e) {
      throw new TestSetupException(e);
    }
    server
        .getServerInstance()
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
  public void startServers() {
    for (String serverId : config.getServerIds()) {
      startServer(serverId);
    }
  }

  @Override
  public void shutdownServer(String serverId) {
    ServerRun server = servers.get(serverId);
    if (server != null) {
      server.getServerInstance().shutdown();
    }
  }

  @Override
  public void teardown() {
    for (ServerRun server : servers.values()) server.getServerInstance().shutdown();
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

  public Collection<ServerRun> getServers() {
    return servers.values();
  }

  public ServerRun getServer(String serverId) {
    return servers.get(serverId);
  }
}
