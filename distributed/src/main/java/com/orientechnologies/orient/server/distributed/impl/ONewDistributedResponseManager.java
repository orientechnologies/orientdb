package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ONewDistributedResponseManager implements ODistributedResponseManager {

  private final    OTransactionPhase1Task iRequest;
  private final    Collection<String>     iNodes;
  private final    Set<String>            nodesConcurToTheQuorum;
  private final    int                    availableNodes;
  private final    int                    expectedResponses;
  private final    int                    quorum;
  private volatile int                    responseCount;
  private volatile Map<Integer, List<OTransactionResultPayload>> resultsByType = new HashMap<>();
  private volatile boolean finished;
  private volatile boolean quorumReached;
  private volatile Object  finalResult;

  public ONewDistributedResponseManager(OTransactionPhase1Task iRequest, Collection<String> iNodes,
      Set<String> nodesConcurToTheQuorum, int availableNodes, int expectedResponses, int quorum) {
    this.iRequest = iRequest;
    this.iNodes = iNodes;
    this.nodesConcurToTheQuorum = nodesConcurToTheQuorum;
    this.availableNodes = availableNodes;
    this.expectedResponses = expectedResponses;
    this.quorum = quorum;
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
  public void removeServerBecauseUnreachable(String node) {
    // ?? probably is more correct handle this case in collect response, to double check
  }

  @Override
  public synchronized boolean waitForSynchronousResponses() throws InterruptedException {
    if (!finished) {
      wait();
    }
    return finished;
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
    return null;
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

  public synchronized boolean collectResponse(OTransactionPhase1TaskResult response) {
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
    if (results.size() >= quorum) {
      this.finished = true;
      this.quorumReached = true;
      this.finalResult = results;
      this.notifyAll();
    } else if (responseCount == expectedResponses) {
      this.quorumReached = false;
      this.finished = true;
      this.finalResult = null;
      this.notifyAll();
    }
    return this.finished;
  }

  @Override
  public boolean collectResponse(ODistributedResponse response) {
    return collectResponse((OTransactionPhase1TaskResult) response.getPayload());
  }

  public synchronized boolean isQuorumReached() {
    return quorumReached;
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
