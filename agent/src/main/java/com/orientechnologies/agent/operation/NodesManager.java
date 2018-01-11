package com.orientechnologies.agent.operation;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationTask;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.util.*;

public class NodesManager {

  private ODistributedServerManager manager;

  public NodesManager(ODistributedServerManager manager) {
    this.manager = manager;
  }

  public List<OperationResponseFromNode> sendAll(NodeOperation task) {
    Set<String> servers = manager.getActiveServers();
    long requestId = manager.getNextMessageIdCounter();
    OperationResponseManager responseManager = new OperationResponseManager(servers);
    ODistributedRequest req = new ODistributedRequest(manager, manager.getLocalNodeId(), requestId, null,
        new NodeOperationTask(task));
    for (String server : servers) {
      try {
        manager.getRemoteServer(server).sendRequest(req);
        manager.getMessageService().registerRequest(requestId, responseManager);
      } catch (IOException e) {
        responseManager.removeServerBecauseUnreachable(server);
      }
    }
    try {
      responseManager.waitForSynchronousResponses();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
    return responseManager.getResponses();
  }

  public OperationResponseFromNode send(String nodeName, NodeOperation task) {
    Set<String> nodes = new HashSet<>();
    nodes.add(nodeName);
    OperationResponseManager responseManager = new OperationResponseManager(nodes);
    try {
      long requestId = manager.getNextMessageIdCounter();
      ODistributedRequest req = new ODistributedRequest(manager, manager.getLocalNodeId(), requestId, null,
          new NodeOperationTask(task));
      manager.getRemoteServer(nodeName).sendRequest(req);

      manager.getMessageService().registerRequest(requestId, responseManager);
    } catch (IOException e) {
      responseManager.removeServerBecauseUnreachable(nodeName);
    }
    return responseManager.getResponses().get(0);
  }

}
