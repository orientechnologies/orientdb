package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedExecutable;
import com.orientechnologies.orient.distributed.impl.network.binary.OBinaryDistributedMessage;
import com.orientechnologies.orient.distributed.impl.network.binary.ODistributedChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ODistributedNetworkManager implements ODiscoveryListener {

  private final ConcurrentMap<ONodeIdentity, ODistributedChannelBinaryProtocol> remoteServers = new ConcurrentHashMap<>();
  private final OrientDBDistributed                                             orientDB;
  private final ONodeConfiguration                                              config;
  private final ONodeInternalConfiguration                                      internalConfiguration;
  private       ONodeManager                                                    discoveryManager;
  private       OCoordinatedExecutorMessageHandler                              requestHandler;

  public ODistributedNetworkManager(OrientDBDistributed orientDB, ONodeConfiguration config,
      ONodeInternalConfiguration internalConfiguration) {
    this.orientDB = orientDB;
    this.config = config;
    this.internalConfiguration = internalConfiguration;
    this.requestHandler = new OCoordinatedExecutorMessageHandler(orientDB);
  }

  public ODistributedChannelBinaryProtocol getRemoteServer(final ONodeIdentity rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ODistributedChannelBinaryProtocol connectRemoteServer(final ONodeIdentity nodeIdentity, String host, String user,
      String password) throws IOException {
    // OK
    ORemoteServerController remoteServer = new ORemoteServerController(new ORemoteServerAvailabilityCheck() {
      @Override
      public boolean isNodeAvailable(String nodeIdToString) {
        return true;
      }

      @Override
      public void nodeDisconnected(String nodeIdToString) {
        //TODO: Integrate with the discovery manager.
        ODistributedNetworkManager.this.orientDB.nodeDisconnected(nodeIdentity);
      }
    }, internalConfiguration.getNodeIdentity().toString(), nodeIdentity.toString(), host, user, password);
    ODistributedChannelBinaryProtocol channel = new ODistributedChannelBinaryProtocol(internalConfiguration.getNodeIdentity(),
        remoteServer);
    final ODistributedChannelBinaryProtocol old = remoteServers.putIfAbsent(nodeIdentity, channel);
    if (old != null) {
      channel.close();
      channel = old;
    }
    return channel;
  }

  public void closeRemoteServer(final ONodeIdentity node) {
    final ODistributedChannelBinaryProtocol c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  public void closeAll() {
    for (ODistributedChannelBinaryProtocol server : remoteServers.values())
      server.close();
    remoteServers.clear();
  }

  public void startup() {
    //TODO different strategies for different infrastructures, eg. AWS

    discoveryManager = new OUDPMulticastNodeManager(config, internalConfiguration, this, orientDB,
        orientDB.getStructuralDistributedContext().getOpLog());
    discoveryManager.start();
  }

  public void shutdown() {
    discoveryManager.stop();
    closeAll();
    //TODO
  }

  @Override
  public void nodeConnected(NodeData data) {
    if (data.getNodeIdentity().equals(internalConfiguration.getNodeIdentity()))
      return;
    ODistributedChannelBinaryProtocol channel = getRemoteServer(data.getNodeIdentity());
    if (channel == null) {
      try {
        channel = connectRemoteServer(data.getNodeIdentity(), data.address + ":" + data.port, data.connectionUsername,
            data.connectionPassword);
        orientDB.nodeConnected(data.getNodeIdentity(), channel);
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on establish connection to a new joined node", e);
      }
    } else {
      orientDB.nodeConnected(data.getNodeIdentity(), channel);
    }
  }

  public void nodeDisconnected(NodeData data) {
    orientDB.nodeDisconnected(data.getNodeIdentity());
    //TODO: Disconnect binary sockets
  }

  @Override
  public void leaderElected(NodeData data) {
    //TODO: Come from a term
    OLogId lastValid = null;
    requestHandler.setLeader(data.getNodeIdentity());
    orientDB.setLeader(data.getNodeIdentity(), lastValid);
  }

  public ODistributedChannel getChannel(ONodeIdentity identity) {
    return remoteServers.get(identity);
  }

  public Set<ONodeIdentity> getRemoteServers() {
    return remoteServers.keySet();
  }

  public void coordinatedRequest(OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    OBinaryRequest<OBinaryResponse> request = new OBinaryDistributedMessage();
    try {
      request.read(channel, 0, null);
    } catch (IOException e) {
      //impossible to read request ... probably need to notify this back.
      throw e;
    }
    ODistributedExecutable executable = (ODistributedExecutable) request;
    executable.executeDistributed(requestHandler);
  }

}
