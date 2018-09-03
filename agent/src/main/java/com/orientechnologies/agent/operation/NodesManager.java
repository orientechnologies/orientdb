package com.orientechnologies.agent.operation;

import com.orientechnologies.agent.cloud.processor.tasks.*;
import com.orientechnologies.agent.cloud.processor.tasks.backup.*;
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

    initCommands();
  }

  private void initCommands() {
    NodeOperationTask.register(1, () -> new EnterpriseStatsTask(), () -> new EnterpriseStatsResponse());
    NodeOperationTask.register(2, () -> new NewEnterpriseStatsTask(), () -> new EnterpriseStatsResponse());
    NodeOperationTask.register(10, () -> new AddBackupTask(), () -> new AddBackupTaskResponse());
    NodeOperationTask.register(11, () -> new ListBackupTask(), () -> new ListBackupTaskResponse());
    NodeOperationTask.register(12, () -> new RemoveBackupTask(), () -> new OkEmptyResponse());
    NodeOperationTask.register(13, () -> new ListBackupLogsTask(), () -> new ListBackupLogsResponse());
    NodeOperationTask.register(14, () -> new ChangeBackupTask(), () -> new ChangeBackupTaskResponse());
    NodeOperationTask.register(15, () -> new RestoreBackupTask(), () -> new OkEmptyResponse());

    NodeOperationTask.register(20, () -> new ListConnectionsTask(), () -> new ListConnectionsTaskResponse());
    NodeOperationTask.register(21, () -> new ThreadDumpTask(), () -> new ThreadDumpTaskResponse());
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
    try {
      responseManager.waitForSynchronousResponses();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
    return responseManager.getResponses().size() > 0 ? responseManager.getResponses().get(0) : null;
  }

}
