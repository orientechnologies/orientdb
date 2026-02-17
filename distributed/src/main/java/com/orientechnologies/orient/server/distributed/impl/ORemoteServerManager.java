package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ORemoteServerManager {

  private final ConcurrentMap<ONodeId, ORemoteServerController> remoteServers =
      new ConcurrentHashMap<>();
  private final ONodeId local;
  private final ORemoteServerAvailabilityCheck check;

  public ORemoteServerManager(ONodeId local, ORemoteServerAvailabilityCheck check) {
    this.local = local;
    this.check = check;
  }

  public ORemoteServerController getRemoteServer(final ONodeId rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ORemoteServerController connectRemoteServer(
      final ONodeId rNodeName, String host, String user, String password) throws IOException {
    // OK
    final ORemoteServerController remoteServer =
        remoteServers.computeIfAbsent(
            rNodeName,
            (node) -> {
              return new ORemoteServerController(check, local, node, host, user, password);
            });
    return remoteServer;
  }

  public void closeRemoteServer(final ONodeId node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null) c.close();
  }

  public void closeAll() {
    for (ORemoteServerController server : remoteServers.values()) server.close();
    remoteServers.clear();
  }
}
