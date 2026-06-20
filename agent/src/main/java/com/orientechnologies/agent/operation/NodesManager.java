package com.orientechnologies.agent.operation;

import com.orientechnologies.agent.cloud.processor.tasks.request.NewEnterpriseStatsTask;
import com.orientechnologies.agent.cloud.processor.tasks.response.EnterpriseStatsResponse;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationTask;
import java.util.List;
import java.util.Set;

public class NodesManager {

  private OrientDBDistributed context;

  public NodesManager(final OrientDBDistributed manager) {
    this.context = manager;
    initCommands();
  }

  private void initCommands() {
    NodeOperationTask.register(
        2, () -> new NewEnterpriseStatsTask(), () -> new EnterpriseStatsResponse());
  }

  public List<OperationResponseFromNode> sendAll(NodeOperation task) {
    Set<ONodeId> servers = context.getOps().getNetworkTopology().getMembers();
    OperationResponseManager responseManager = new OperationResponseManager(servers);
    ODistributedRequestId requestId = context.nextRequestId();
    ODistributedRequest req =
        new ODistributedRequest(
            context.getTaskFactoryManager(), requestId, null, new NodeOperationTask(task));
    for (ONodeId server : servers) {
      context.getRemoteServer(server).sendRequest(req);
      context.getMessageService().registerRequest(requestId.getMessageId(), responseManager);
    }
    try {
      responseManager.waitForSynchronousResponses();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
    return responseManager.getResponses();
  }

  public OperationResponseFromNode send(final String nodeName, final NodeOperation task) {
    var nodes = Set.of(new ONodeId(nodeName));
    OperationResponseManager responseManager = new OperationResponseManager(nodes);
    long requestId = context.getNextMessageIdCounter();
    ODistributedRequest req =
        new ODistributedRequest(
            context.getTaskFactoryManager(),
            context.nextRequestId(),
            null,
            new NodeOperationTask(task));
    context.getRemoteServer(nodeName).sendRequest(req);

    context.getMessageService().registerRequest(requestId, responseManager);

    try {
      responseManager.waitForSynchronousResponses();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
    return responseManager.getResponses().size() > 0 ? responseManager.getResponses().get(0) : null;
  }
}
