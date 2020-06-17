package com.orientechnologies.orient.server.distributed;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ORemoteServerManager {

  private final ConcurrentMap<String, ORemoteServerController> remoteServers =
      new ConcurrentHashMap<String, ORemoteServerController>();
  private final String localNodeName;
  private final ORemoteServerAvailabilityCheck check;

  public ORemoteServerManager(String localNodeName, ORemoteServerAvailabilityCheck check) {
    this.localNodeName = localNodeName;
    this.check = check;
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ORemoteServerController connectRemoteServer(
      final String rNodeName, String host, String user, String password) throws IOException {
    // OK
    ORemoteServerController remoteServer =
        new ORemoteServerController(check, localNodeName, rNodeName, host, user, password);
    final ORemoteServerController old = remoteServers.putIfAbsent(rNodeName, remoteServer);
    if (old != null) {
      remoteServer.close();
      remoteServer = old;
    }
    return remoteServer;
  }

  public void closeRemoteServer(final String node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null) c.close();
  }

  public void closeAll() {
    for (ORemoteServerController server : remoteServers.values()) server.close();
    remoteServers.clear();
  }
}
