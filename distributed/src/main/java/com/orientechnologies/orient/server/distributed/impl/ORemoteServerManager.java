package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public class ORemoteServerManager {

  private final ConcurrentMap<ONodeId, ORemoteServerController> remoteServers =
      new ConcurrentHashMap<>();
  private final ONodeId local;
  private final ORemoteServerAvailabilityCheck check;
  private final ExecutorService executor;

  public ORemoteServerManager(
      ONodeId local, ORemoteServerAvailabilityCheck check, ExecutorService executor) {
    this.local = local;
    this.check = check;
    this.executor = executor;
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
              return new ORemoteServerController(
                  check, local, node, host, user, password, executor);
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
