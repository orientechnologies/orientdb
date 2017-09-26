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

public class ONewDistributedResponseManager implements ODistributedResponseManager {

  private       OTransactionPhase1Task iRequest;
  private final Collection<String>     iNodes;
  private final Set<String>            nodesConcurToTheQuorum;
  private final int                    availableNodes;
  private final int                    expectedResponses;
  private final int                    quorum;
  private Map<Integer, List<OTransactionResultPayload>> resultsByType = new HashMap<>();

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
  public boolean setLocalResult(String localNodeName, Object localResult) {
    List<OTransactionResultPayload> results = addResult((OTransactionResultPayload) localResult);
    return results.size() > quorum;
  }

  @Override
  public ODistributedResponse getFinalResponse() {
    return null;
  }

  @Override
  public void removeServerBecauseUnreachable(String node) {

  }

  @Override
  public boolean waitForSynchronousResponses() throws InterruptedException {
    return true;
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
    return null;
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
    OTransactionResultPayload result = response.getResultPayload();
    List<OTransactionResultPayload> results = addResult(result);
    return results.size() >= quorum;
  }

  public List<OTransactionResultPayload> addResult(OTransactionResultPayload result) {
    List<OTransactionResultPayload> results = resultsByType.get(result.getResponseType());
    if (results == null) {
      results = new ArrayList<>();
      results.add(result);
      resultsByType.put(result.getResponseType(), results);
    } else {
      results.add(result);
    }
    return results;
  }

  @Override
  public boolean collectResponse(ODistributedResponse response) {
    return collectResponse((ODistributedResponse) response.getPayload());
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
