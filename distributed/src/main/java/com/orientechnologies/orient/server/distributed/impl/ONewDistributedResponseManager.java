package com.orientechnologies.orient.server.distributed.impl;


import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxStillRunning;

import java.util.*;

public class ONewDistributedResponseManager implements ODistributedResponseManager {

  private final    OTransactionPhase1Task                             iRequest;
  private final    Collection<String>                                 iNodes;
  private final    Set<String>                                        nodesConcurToTheQuorum;
  private final    int                                                availableNodes;
  private final    int                                                expectedResponses;
  private final    int                                                quorum;
  private volatile long                                               timeout;
  private volatile int                                                responseCount;
  private final    List<String>                                       debugNodeReplied   = new ArrayList<>();
  private volatile Map<Integer, List<OTransactionResultPayload>>      resultsByType      = new HashMap<>();
  private volatile IdentityHashMap<OTransactionResultPayload, String> payloadToNode      = new IdentityHashMap<>();
  private volatile boolean                                            finished           = false;
  private volatile boolean                                            quorumReached      = false;
  private volatile Object                                             finalResult;
  private volatile int                                                stillRunning       = 0;
  private volatile int                                                stillRunningWaited = 0;

  public ONewDistributedResponseManager(OTransactionPhase1Task iRequest, Collection<String> iNodes,
      Set<String> nodesConcurToTheQuorum, int availableNodes, int expectedResponses, int quorum) {
    this.iRequest = iRequest;
    this.iNodes = iNodes;
    this.nodesConcurToTheQuorum = nodesConcurToTheQuorum;
    this.availableNodes = availableNodes;
    this.expectedResponses = expectedResponses;
    this.quorum = quorum;
    timeout = iRequest.getSynchronousTimeout(expectedResponses);
  }

  @Override
  public synchronized boolean setLocalResult(String localNodeName, Object localResult) {
    return addResult(localNodeName, (OTransactionResultPayload) localResult);
  }

  @Override
  public ODistributedResponse getFinalResponse() {
    return null;
  }

  @Override
  public Object getGenericFinalResponse() {
    return finalResult;
  }

  @Override
  public synchronized void removeServerBecauseUnreachable(String node) {
    responseCount += 1;
    checkFinished(new ArrayList<>());
  }

  @Override
  public synchronized boolean waitForSynchronousResponses() throws InterruptedException {
    boolean interrupted = false;
    while (!interrupted) {
      try {
        if (!quorumReached) {
          wait(timeout);
        }
        if (quorumReached || finished) {
          return quorumReached;
        } else if (stillRunning - stillRunningWaited > 0) {
          stillRunningWaited++;
          // This put the timeout to just the timeout of one node.
          this.timeout = iRequest.getDistributedTimeout();
        } else {
          return quorumReached;
        }
      } catch (InterruptedException e) {
        //Let the operation finish anyway
        Thread.currentThread().interrupt();
        interrupted = true;
      }
    }
    return quorumReached  ;
  }

  @Override
  public long getSynchTimeout() {
    return timeout;
  }

  @Override
  public void cancel() {
    //This should do nothing we cannot cancel things
  }

  @Override
  public Set<String> getExpectedNodes() {
    return nodesConcurToTheQuorum;
  }

  @Override
  public List<String> getRespondingNodes() {
    return debugNodeReplied;
  }

  @Override
  public Set<String> getServersWithoutFollowup() {
    return null;
  }

  @Override
  public ODistributedRequestId getMessageId() {
    return null;
  }

  @Override
  public ODistributedRequest getRequest() {
    return null;
  }

  public String getNodeNameFromPayload(OTransactionResultPayload payload) {
    return this.payloadToNode.get(payload);
  }

  @Override
  public int getQuorum() {
    return quorum;
  }

  public synchronized boolean collectResponse(OTransactionPhase1TaskResult response, String senderNodeName) {
    if (response.getResultPayload() instanceof OTxStillRunning) {
      stillRunning++;
      return false;
    }
    debugNodeReplied.add(senderNodeName);
    return addResult(senderNodeName, response.getResultPayload());
  }

  private boolean addResult(String senderNodeName, OTransactionResultPayload result) {
    List<OTransactionResultPayload> results = new ArrayList<>();

    if (nodesConcurToTheQuorum.contains(senderNodeName)) {
      results = resultsByType.get(result.getResponseType());
      if (results == null) {
        results = new ArrayList<>();
        results.add(result);
        resultsByType.put(result.getResponseType(), results);
      } else {
        results.add(result);
      }
      payloadToNode.put(result, senderNodeName);
    }
    responseCount += 1;
    checkFinished(results);
    return this.finished;
  }

  private void checkFinished(List<OTransactionResultPayload> results) {
    if (results.size() >= quorum) {
      if (!quorumReached) {
        this.quorumReached = true;
        this.finalResult = results;
        this.notifyAll();
      }
      if (responseCount == expectedResponses) {
        this.finished = true;
      }
    } else if (responseCount == expectedResponses) {
      if (quorumReached) {
        this.finished = true;
      } else {
        this.finished = true;
        finalResult = null;
        this.finalResult = null;
        this.notifyAll();
      }
    }
  }

  public synchronized List<OTransactionResultPayload> getAllResponses() {
    List<OTransactionResultPayload> allResults = new ArrayList<>();
    for (List<OTransactionResultPayload> res : resultsByType.values()) {
      allResults.addAll(res);
    }
    return allResults;
  }

  @Override
  public boolean collectResponse(ODistributedResponse response) {
    if (response.getPayload() instanceof OTransactionPhase1TaskResult) {
      return collectResponse((OTransactionPhase1TaskResult) response.getPayload(), response.getExecutorNodeName());
    } else if (response.getPayload() instanceof RuntimeException) {
      return collectResponse(new OTransactionPhase1TaskResult(new OTxException((RuntimeException) response.getPayload())),
          response.getExecutorNodeName());
    } else {
      return collectResponse(
          new OTransactionPhase1TaskResult(new OTxException(new ODistributedException("unknown payload:" + response.getPayload()))),
          response.getExecutorNodeName());
    }
  }

  public synchronized boolean isQuorumReached() {
    return quorumReached;
  }

  public synchronized boolean isFinished() {
    return finished;
  }

  @Override
  public void timeout() {

  }

  @Override
  public long getSentOn() {
    return 0;
  }

  @Override
  public List<String> getMissingNodes() {
    return null;
  }

  @Override
  public String getDatabaseName() {
    return null;
  }
}
