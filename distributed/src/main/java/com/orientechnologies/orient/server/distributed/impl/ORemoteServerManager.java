package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeInfoListener;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactoryManager;
import com.orientechnologies.orient.server.distributed.impl.ORemoteAddress.OBinaryAddress;
import com.orientechnologies.orient.server.distributed.impl.task.ORemoteTaskFactoryManagerImpl;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public class ORemoteServerManager {

  private final ConcurrentMap<ONodeId, ORemoteServerController> remoteServers =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<ONodeId, ORemoteAddress> remoteAddresses = new ConcurrentHashMap<>();
  protected ORemoteTaskFactoryManager taskFactoryManager = new ORemoteTaskFactoryManagerImpl(this);

  private final ONodeId local;
  private final ORemoteServerAvailabilityCheck check;
  private final ExecutorService executor;
  private final String user;
  private final String password;

  public ORemoteServerManager(
      ONodeId local,
      ORemoteServerAvailabilityCheck check,
      ExecutorService executor,
      String user,
      String password) {
    this.local = local;
    this.check = check;
    this.executor = executor;
    this.user = user;
    this.password = password;
  }

  public ORemoteServerController getRemoteServer(final ONodeId rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ORemoteServerController connectRemoteServer(final ONodeId rNodeName, String host)
      throws IOException {
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

  public void registerRemoteAddresses(ONodeId nodeId, List<ONodeInfoListener> listeners) {
    remoteAddresses.computeIfAbsent(nodeId, k -> new ORemoteAddress()).addAddresses(listeners);
  }

  public List<OBinaryAddress> getRemoteAddresses(ONodeId node) {
    return Optional.ofNullable(remoteAddresses.get(node))
        .map(ORemoteAddress::getAddresses)
        .orElseGet(Collections::emptyList);
  }

  public ORemoteTaskFactoryManager getTaskFactoryManager() {
    return taskFactoryManager;
  }
}
