package com.orientechnologies.agent.operation;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationTaskResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OperationResponseManager implements ODistributedResponseManager {

  private final List<OperationResponseFromNode> responses = new ArrayList<>();
  private final Set<String> servers;
  private final CountDownLatch waitingFor;
  private final long sentOn = System.currentTimeMillis();

  public OperationResponseManager(Set<String> servers) {
    this.servers = servers;
    this.waitingFor = new CountDownLatch(servers.size());
  }

  @Override
  public synchronized boolean setLocalResult(String localNodeName, Object localResult) {
    NodeOperationResponse result = (NodeOperationResponse) localResult;
    if (result.isOk()) {
      responses.add(new OperationResponseFromNode(localNodeName, new ResponseOk(result)));
    } else {
      responses.add(new OperationResponseFromNode(localNodeName, new ResponseFailed(result)));
    }
    return false;
  }

  @Override
  public ODistributedResponse getFinalResponse() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void removeServerBecauseUnreachable(String node) {
    responses.add(new OperationResponseFromNode(node, new NodeNotReachable()));
    waitingFor.countDown();
  }

  @Override
  public boolean waitForSynchronousResponses() throws InterruptedException {
    return waitingFor.await(
        OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsInteger(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public long getSynchTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancel() {
    while (waitingFor.getCount() > 0) waitingFor.countDown();
  }

  @Override
  public Set<String> getExpectedNodes() {
    return servers;
  }

  @Override
  public List<String> getRespondingNodes() {
    return responses.stream().map((a) -> a.getSenderNodeName()).collect(Collectors.toList());
  }

  @Override
  public Set<String> getServersWithoutFollowup() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ODistributedRequestId getMessageId() {
    return null;
  }

  @Override
  public ODistributedRequest getRequest() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getQuorum() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean collectResponse(ODistributedResponse response) {
    NodeOperationTaskResponse nodeResponse = (NodeOperationTaskResponse) response.getPayload();
    if (nodeResponse.getResponse().isOk()) {
      responses.add(
          new OperationResponseFromNode(
              response.getExecutorNodeName(), new ResponseOk(nodeResponse.getResponse())));
    } else {
      responses.add(
          new OperationResponseFromNode(
              response.getExecutorNodeName(), new ResponseFailed(nodeResponse.getResponse())));
    }
    waitingFor.countDown();
    return waitingFor.getCount() == 0;
  }

  @Override
  public void timeout() {
    cancel();
  }

  @Override
  public long getSentOn() {
    return sentOn;
  }

  @Override
  public List<String> getMissingNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDatabaseName() {
    return null;
  }

  public List<OperationResponseFromNode> getResponses() {
    return responses;
  }

  @Override
  public boolean isFinished() {
    return waitingFor.getCount() == 0;
  }
}
