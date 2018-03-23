package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ONewDistributedResponseManager implements ODistributedResponseManager {

  private final    OTransactionPhase1Task iRequest;
  private final    Collection<String>     iNodes;
  private final    Set<String>            nodesConcurToTheQuorum;
  private final    int                    availableNodes;
  private final    int                    expectedResponses;
  private final    int                    quorum;
  private final    long                   timeout;
  private volatile int                    responseCount;
  private final    List<String>                                  debugNodeReplied = new ArrayList<>();
  private volatile Map<Integer, List<OTransactionResultPayload>> resultsByType    = new HashMap<>();
  private volatile boolean                                       finished         = false;
  private volatile boolean                                       quorumReached    = false;
  private volatile Object finalResult;

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
    return addResult((OTransactionResultPayload) localResult);
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
    if (!quorumReached) {
      wait(timeout);
    }
    return quorumReached;
  }

  @Override
  public long getSynchTimeout() {
    return 0;
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
  public Collection<String> getConflictServers() {
    return null;
  }

  @Override
  public Set<String> getServersWithoutFollowup() {
    return null;
  }

  @Override
  public boolean addFollowupToServer(String node) {
    return false;
  }

  @Override
  public boolean isSynchronousWaiting() {
    return false;
  }

  @Override
  public ODistributedResponse getQuorumResponse() {
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

  @Override
  public Object getResponseFromServer(String server) {
    return null;
  }

  @Override
  public int getQuorum() {
    return 0;
  }

  @Override
  public boolean executeInLock(OCallable<Boolean, ODistributedResponseManager> oCallable) {
    return false;
  }

  public synchronized boolean collectResponse(OTransactionPhase1TaskResult response, String senderNodeName) {
    debugNodeReplied.add(senderNodeName);
    return addResult(response.getResultPayload());
  }

  private boolean addResult(OTransactionResultPayload result) {
    List<OTransactionResultPayload> results = resultsByType.get(result.getResponseType());
    if (results == null) {
      results = new ArrayList<>();
      results.add(result);
      resultsByType.put(result.getResponseType(), results);
    } else {
      results.add(result);
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
      return collectResponse((OTransactionPhase1TaskResult) response.getPayload(), response.getSenderNodeName());
    } else if (response.getPayload() instanceof RuntimeException) {
      return collectResponse(new OTransactionPhase1TaskResult(new OTxException((RuntimeException) response.getPayload())),
          response.getSenderNodeName());
    } else {
      return collectResponse(
          new OTransactionPhase1TaskResult(new OTxException(new ODistributedException("unknown payload:" + response.getPayload()))),
          response.getSenderNodeName());
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
