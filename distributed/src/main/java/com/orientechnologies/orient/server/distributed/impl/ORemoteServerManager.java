package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
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
    final ORemoteServerController remoteServer =
        remoteServers.computeIfAbsent(
            rNodeName,
            (node) -> {
              return new ORemoteServerController(check, localNodeName, node, host, user, password);
            });
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
