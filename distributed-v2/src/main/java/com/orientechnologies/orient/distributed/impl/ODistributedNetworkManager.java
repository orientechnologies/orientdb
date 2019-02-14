package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedChannelBinaryProtocol;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ODistributedNetworkManager implements ODiscoveryListener {

  private final ConcurrentMap<ONodeIdentity, ORemoteServerController> remoteServers = new ConcurrentHashMap<ONodeIdentity, ORemoteServerController>();
  private final OrientDBDistributed                                   orientDB;
  private final ONodeConfiguration                                    config;
  private final ONodeInternalConfiguration                            internalConfiguration;
  private       OUDPMulticastNodeManager                              discoveryManager;

  public ODistributedNetworkManager(OrientDBDistributed orientDB, ONodeConfiguration config,
      ONodeInternalConfiguration internalConfiguration) {
    this.orientDB = orientDB;
    this.config = config;
    this.internalConfiguration = internalConfiguration;

  }

  public ORemoteServerController getRemoteServer(final ONodeIdentity rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ORemoteServerController connectRemoteServer(final ONodeIdentity nodeIdentity, String host, String user, String password)
      throws IOException {
    // OK
    ORemoteServerController remoteServer = new ORemoteServerController(new ORemoteServerAvailabilityCheck() {
      @Override
      public boolean isNodeAvailable(String node) {
        return true;
      }

      @Override
      public void nodeDisconnected(String node) {
        //TODO: Integrate with the discovery manager.
        ODistributedNetworkManager.this.orientDB.nodeDisconnected(nodeIdentity);
      }
    }, config.getNodeIdentity().getName(), nodeIdentity.getName(), host, user, password);
    final ORemoteServerController old = remoteServers.putIfAbsent(nodeIdentity, remoteServer);
    if (old != null) {
      remoteServer.close();
      remoteServer = old;
    }
    return remoteServer;
  }

  public void closeRemoteServer(final ONodeIdentity node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  public void closeAll() {
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();
  }

  public void startup() {
    //TODO different strategies for different infrastructures, eg. AWS

    discoveryManager = new OUDPMulticastNodeManager(config, internalConfiguration, this, orientDB);
    discoveryManager.start();
  }

  public void shutdown() {
    discoveryManager.stop();
    closeAll();
    //TODO
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    return null;//TODO
  }

  @Override
  public void nodeConnected(NodeData data) {
    if (data.getNodeIdentity().equals(config.getNodeIdentity()))
      return;
    ORemoteServerController remote = getRemoteServer(data.getNodeIdentity());
    if (remote == null) {
      //TODO: use some authentication data to do a binary connection.
      try {
        remote = connectRemoteServer(data.getNodeIdentity(), data.address + ":" + data.port, data.connectionUsername,
            data.connectionPassword);
        orientDB.nodeConnected(data.getNodeIdentity(), new ODistributedChannelBinaryProtocol(config.getNodeIdentity(), remote));
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on establish connection to a new joined node", e);
      }
    } else {
      orientDB.nodeConnected(data.getNodeIdentity(), new ODistributedChannelBinaryProtocol(config.getNodeIdentity(), remote));
    }
  }

  public void nodeDisconnected(NodeData data) {
    orientDB.nodeDisconnected(data.getNodeIdentity());
    //TODO: Disconnect binary sockets
  }

  @Override
  public void leaderElected(NodeData data) {
    orientDB.setCoordinator(data.getNodeIdentity());
  }
}
