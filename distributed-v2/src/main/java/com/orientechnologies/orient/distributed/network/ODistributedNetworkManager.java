package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OCoordinatedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedExecutable;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage;
import com.orientechnologies.orient.distributed.network.binary.ODistributedChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ODistributedNetworkManager implements ODiscoveryListener, ODistributedNetwork {

  private final ConcurrentMap<ONodeIdentity, ODistributedChannelBinaryProtocol> remoteServers =
      new ConcurrentHashMap<>();
  private final ONodeConfiguration config;
  private final ONodeInternalConfiguration internalConfiguration;
  private final OSchedulerInternal scheduler;
  private ONodeManager discoveryManager;
  private OCoordinatedExecutor requestHandler;

  public ODistributedNetworkManager(
      OCoordinatedExecutor requestHandler,
      ONodeConfiguration config,
      ONodeInternalConfiguration internalConfiguration,
      OSchedulerInternal scheduler) {
    this.config = config;
    this.internalConfiguration = internalConfiguration;
    this.requestHandler = requestHandler;
    this.scheduler = scheduler;
  }

  private ODistributedChannelBinaryProtocol getRemoteServer(final ONodeIdentity rNodeName) {
    return remoteServers.get(rNodeName);
  }

  private ODistributedChannelBinaryProtocol connectRemoteServer(
      final ONodeIdentity nodeIdentity, String host, String user, String password)
      throws IOException {
    // OK
    ORemoteServerController remoteServer =
        new ORemoteServerController(
            new ORemoteServerAvailabilityCheck() {
              @Override
              public boolean isNodeAvailable(String nodeIdToString) {
                return true;
              }

              @Override
              public void nodeDisconnected(String nodeIdToString) {
                // TODO: Integrate with the discovery manager.
                ODistributedNetworkManager.this.requestHandler.nodeDisconnected(nodeIdentity);
              }
            },
            internalConfiguration.getNodeIdentity().toString(),
            nodeIdentity.toString(),
            host,
            user,
            password);
    ODistributedChannelBinaryProtocol channel =
        new ODistributedChannelBinaryProtocol(
            internalConfiguration.getNodeIdentity(), remoteServer);
    final ODistributedChannelBinaryProtocol old = remoteServers.putIfAbsent(nodeIdentity, channel);
    if (old != null) {
      channel.close();
      channel = old;
    }
    return channel;
  }

  public void closeRemoteServer(final ONodeIdentity node) {
    final ODistributedChannelBinaryProtocol c = remoteServers.remove(node);
    if (c != null) c.close();
  }

  private void closeAll() {
    for (ODistributedChannelBinaryProtocol server : remoteServers.values()) server.close();
    remoteServers.clear();
  }

  public void startup(OOperationLog structuralLog) {
    // TODO different strategies for different infrastructures, eg. AWS
    discoveryManager =
        new OUDPMulticastNodeManager(config, internalConfiguration, this, scheduler, structuralLog);
    discoveryManager.start();
  }

  public void shutdown() {
    discoveryManager.stop();
    closeAll();
    // TODO
  }

  @Override
  public void nodeConnected(NodeData data) {
    if (data.getNodeIdentity().equals(internalConfiguration.getNodeIdentity())) return;
    ODistributedChannelBinaryProtocol channel = getRemoteServer(data.getNodeIdentity());
    if (channel == null) {
      try {
        connectRemoteServer(
            data.getNodeIdentity(),
            data.address + ":" + data.port,
            data.connectionUsername,
            data.connectionPassword);

      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on establish connection to a new joined node", e);
      }
    }
    requestHandler.nodeConnected(data.getNodeIdentity());
  }

  public void nodeDisconnected(NodeData data) {
    requestHandler.nodeDisconnected(data.getNodeIdentity());
    // TODO: Disconnect binary sockets
  }

  @Override
  public void leaderElected(NodeData data) {
    // TODO: Come from a term
    OLogId lastValid = null;
    requestHandler.setLeader(data.getNodeIdentity(), lastValid);
  }

  public ODistributedChannel getChannel(ONodeIdentity identity) {
    return remoteServers.get(identity);
  }

  public Set<ONodeIdentity> getRemoteServers() {
    return remoteServers.keySet();
  }

  public void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    OBinaryRequest<OBinaryResponse> request = new OBinaryDistributedMessage();
    try {
      request.read(channel, 0, null);
    } catch (IOException e) {
      // impossible to read request ... probably need to notify this back.
      throw e;
    }
    ODistributedExecutable executable = (ODistributedExecutable) request;
    executable.executeDistributed(requestHandler);
  }

  public void propagate(Collection<ONodeIdentity> members, OLogId id, ORaftOperation operation) {
    for (ONodeIdentity member : members) {
      assert !isSelf(member);
      getChannel(member).propagate(id, operation);
    }
  }

  public void confirm(Collection<ONodeIdentity> members, OLogId id) {
    for (ONodeIdentity member : members) {
      assert !isSelf(member);
      getChannel(member).confirm(id);
    }
  }

  public void ack(ONodeIdentity member, OLogId logId) {
    assert !isSelf(member);
    getChannel(member).ack(logId);
  }

  public void submit(
      ONodeIdentity leader, OSessionOperationId operationId, OStructuralSubmitRequest request) {
    if (isSelf(leader)) {
      this.requestHandler.executeStructuralSubmitRequest(leader, operationId, request);
    } else {
      getChannel(leader).submit(operationId, request);
    }
  }

  public void reply(
      ONodeIdentity identity, OSessionOperationId operationId, OStructuralSubmitResponse response) {
    if (isSelf(identity)) {
      this.requestHandler.executeStructuralSubmitResponse(identity, operationId, response);
    } else {
      getChannel(identity).reply(operationId, response);
    }
  }

  public void submit(
      ONodeIdentity leader,
      String database,
      OSessionOperationId operationId,
      OSubmitRequest request) {
    if (isSelf(leader)) {
      this.requestHandler.executeSubmitRequest(leader, database, operationId, request);
    } else {
      getChannel(leader).submit(database, operationId, request);
    }
  }

  private boolean isSelf(ONodeIdentity leader) {
    return this.internalConfiguration.getNodeIdentity().equals(leader);
  }

  public void replay(
      ONodeIdentity to,
      String database,
      OSessionOperationId operationId,
      OSubmitResponse response) {
    if (isSelf(to)) {
      this.requestHandler.executeSubmitResponse(to, database, operationId, response);
    } else {
      getChannel(to).reply(database, operationId, response);
    }
  }

  public void sendResponse(
      ONodeIdentity member, String database, OLogId opId, ONodeResponse response) {
    if (isSelf(member)) {
      this.requestHandler.executeOperationResponse(member, database, opId, response);
    } else {
      getChannel(member).sendResponse(database, opId, response);
    }
  }

  public void sendRequest(
      Collection<ONodeIdentity> members, String database, OLogId id, ONodeRequest nodeRequest) {
    for (ONodeIdentity member : members) {
      if (isSelf(member)) {
        this.requestHandler.executeOperationRequest(member, database, id, nodeRequest);
      } else {
        getChannel(member).sendRequest(database, id, nodeRequest);
      }
    }
  }

  public void send(ONodeIdentity identity, OOperation operation) {
    assert !isSelf(identity);
    getChannel(identity).send(operation);
  }

  @Override
  public void sendAll(Collection<ONodeIdentity> members, OOperation operation) {
    for (ONodeIdentity member : members) {
      if (isSelf(member)) {
        this.requestHandler.executeOperation(member, operation);
      } else {
        getChannel(member).send(operation);
      }
    }
  }

  @Override
  public void notifyLastDbOperation(ONodeIdentity leader, String database, OLogId leaderLastValid) {
    discoveryManager.notifyLastDbOperation(database, leaderLastValid);
  }

  @Override
  public void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid) {
    // TODO
  }

  @Override
  public void lastDbOperation(ONodeIdentity leader, String database, OLogId logId) {
    requestHandler.notifyLastDatabaseOperation(leader, database, logId);
  }
}
