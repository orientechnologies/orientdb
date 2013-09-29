/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask.RESULT_STRATEGY;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * Asynchronous response manager
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedResponseManager {
  private final ODistributedServerManager        dManager;
  private final ODistributedRequest              request;
  private final long                             sentOn;
  private final HashMap<String, Object>          responses                   = new HashMap<String, Object>();
  private final List<List<ODistributedResponse>> responseGroups              = new ArrayList<List<ODistributedResponse>>();
  private final int                              expectedSynchronousResponses;
  private int                                    receivedResponses           = 0;
  private int                                    quorum;
  private boolean                                waitForLocalNode;
  private final long                             synchTimeout;
  private final long                             totalTimeout;
  private boolean                                receivedCurrentNode;
  private final Lock                             lock                        = new ReentrantLock();
  private final Condition                        synchronousResponsesArrived = lock.newCondition();

  private static final String                    NO_RESPONSE                 = "waiting-for-response";

  public ODistributedResponseManager(final ODistributedServerManager iManager, final ODistributedRequest iRequest,
      final Set<String> expectedResponses, final int iExpectedSynchronousResponses, final int iQuorum,
      final boolean iWaitForLocalNode, final long iSynchTimeout, final long iTotalTimeout) {
    this.dManager = iManager;
    this.request = iRequest;
    this.sentOn = System.currentTimeMillis();
    this.expectedSynchronousResponses = iExpectedSynchronousResponses;
    this.quorum = iQuorum;
    this.waitForLocalNode = iWaitForLocalNode;
    this.synchTimeout = iSynchTimeout;
    this.totalTimeout = iTotalTimeout;

    for (String node : expectedResponses)
      responses.put(node, NO_RESPONSE);

    responseGroups.add(new ArrayList<ODistributedResponse>());
  }

  public boolean addResponse(final ODistributedResponse response) {
    final String executorNode = response.getExecutorNodeName();

    if (!responses.containsKey(executorNode)) {
      ODistributedServerLog.warn(this, response.getSenderNodeName(), executorNode, DIRECTION.IN,
          "received response for message %d from unexpected node. Expected are: %s", request.getId(), getExpectedNodes());

      Orient.instance().getProfiler()
          .updateCounter("distributed.replication.unexpectedNodeResponse", "Number of responses from unexpected nodes", +1);

      return false;
    }

    Orient
        .instance()
        .getProfiler()
        .stopChrono("distributed.replication.responseTime", "Response time from replication messages", sentOn,
            "distributed.replication.responseTime");

    Orient
        .instance()
        .getProfiler()
        .stopChrono("distributed.replication." + executorNode + ".responseTime", "Response time from replication messages", sentOn,
            "distributed.replication.*.responseTime");

    responses.put(executorNode, response);
    receivedResponses++;

    if (waitForLocalNode && response.isExecutedOnLocalNode())
      receivedCurrentNode = true;

    boolean foundBucket = false;
    for (int i = 0; i < responseGroups.size(); ++i) {
      final List<ODistributedResponse> sameResponse = responseGroups.get(i);
      if (sameResponse.isEmpty() || sameResponse.get(0).getPayload().equals(response.getPayload())) {
        sameResponse.add(response);
        foundBucket = true;
        break;
      }
    }

    if (!foundBucket) {
      // CREATE A NEW BUCKET
      final ArrayList<ODistributedResponse> newBucket = new ArrayList<ODistributedResponse>();
      responseGroups.add(newBucket);
      newBucket.add(response);
    }

    final boolean completed = getExpectedResponses() == receivedResponses;

    if (receivedResponses >= expectedSynchronousResponses && (!waitForLocalNode || receivedCurrentNode)) {
      if (completed || isMinimumQuorumReached()) {
        // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
        lock.lock();
        try {
          synchronousResponsesArrived.signalAll();
        } finally {
          lock.unlock();
        }
      }
    }

    return completed;
  }

  /**
   * Returns the received response objects.
   */
  public List<ODistributedResponse> getReceivedResponses() {
    final List<ODistributedResponse> parsed = new ArrayList<ODistributedResponse>();
    for (Object r : responses.values())
      if (r != NO_RESPONSE)
        parsed.add((ODistributedResponse) r);
    return parsed;
  }

  public void timeout() {
    manageConflicts();
  }

  public boolean isMinimumQuorumReached() {
    if (quorum == 0)
      return true;

    for (List<ODistributedResponse> group : responseGroups)
      if (group.size() >= quorum)
        return true;

    return false;
  }

  /**
   * Returns the biggest response group.
   * 
   * @return
   */
  public int getBestResponsesGroup() {
    int maxCoherentResponses = 0;
    int bestGroupSoFar = 0;
    for (int i = 0; i < responseGroups.size(); ++i) {
      final int currentGroupSize = responseGroups.get(i).size();
      if (currentGroupSize > maxCoherentResponses) {
        maxCoherentResponses = currentGroupSize;
        bestGroupSoFar = i;
      }
    }
    return bestGroupSoFar;
  }

  protected void manageConflicts() {
    if (quorum == 0)
      // NO QUORUM
      return;

    if (responseGroups.size() == 1)
      // NO CONFLICT
      return;

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);
    final int maxCoherentResponses = bestResponsesGroup.size();

    final int conflicts = getExpectedResponses() - maxCoherentResponses;

    if (maxCoherentResponses >= quorum) {
      // CHECK IF THERE ARE 2 PARTITIONS EQUAL IN SIZE
      for (List<ODistributedResponse> responseGroup : responseGroups) {
        if (responseGroup != bestResponsesGroup && responseGroup.size() == maxCoherentResponses) {
          final List<String> a = new ArrayList<String>();
          for (ODistributedResponse r : bestResponsesGroup)
            a.add(r.getExecutorNodeName());

          final List<String> b = new ArrayList<String>();
          for (ODistributedResponse r : responseGroup)
            b.add(r.getExecutorNodeName());

          ODistributedServerLog
              .error(
                  this,
                  dManager.getLocalNodeName(),
                  null,
                  DIRECTION.NONE,
                  "detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached",
                  a, b, quorum);
          // DON'T FIX RECORDS
          return;
        }
      }

      final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

      // START WITH THE FIXING
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "detected %d conflicts, but the quorum (%d) has been reached. Fixing remote records", conflicts, quorum);

      for (List<ODistributedResponse> responseGroup : responseGroups) {
        if (responseGroup != bestResponsesGroup) {
          for (ODistributedResponse r : responseGroup) {
            ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "fixing response for request=%s in server %s to be: %s", r.getExecutorNodeName(), request, goodResponse);

            final OAbstractRemoteTask fixRequest = ((OAbstractReplicatedTask) request.getPayload()).getFixTask(request,
                goodResponse);

            dManager.sendRequest2Node(request.getDatabaseName(), r.getExecutorNodeName(), fixRequest);
          }
        }
      }

    } else
      ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "detected %d conflicts where the quorum (%d) has not been reached, cannot guarantee coherency against this resources",
          conflicts, quorum);
  }

  public long getMessageId() {
    return request.getId();
  }

  public long getSentOn() {
    return sentOn;
  }

  public int getExpectedResponses() {
    return responses.size();
  }

  public Set<String> getExpectedNodes() {
    return responses.keySet();
  }

  public int getMissingResponses() {
    return getExpectedResponses() - receivedResponses;
  }

  public List<String> getRespondingNodes() {
    final List<String> respondedNodes = new ArrayList<String>();
    for (Map.Entry<String, Object> entry : responses.entrySet())
      if (entry.getValue() != NO_RESPONSE)
        respondedNodes.add(entry.getKey());
    return respondedNodes;
  }

  public List<String> getMissingNodes() {
    final List<String> missingNodes = new ArrayList<String>();
    for (Map.Entry<String, Object> entry : responses.entrySet())
      if (entry.getValue() == NO_RESPONSE)
        missingNodes.add(entry.getKey());
    return missingNodes;
  }

  public int getReceivedResponsesCount() {
    return receivedResponses;
  }

  public long getTotalTimeout() {
    return totalTimeout;
  }

  @SuppressWarnings("unchecked")
  public ODistributedResponse merge(final ODistributedResponse merged) {
    final StringBuilder executor = new StringBuilder();
    HashSet<Object> mergedPayload = new HashSet<Object>();

    for (Map.Entry<String, Object> entry : responses.entrySet()) {
      if (entry.getValue() != NO_RESPONSE) {
        // APPEND THE EXECUTOR
        if (executor.length() > 0)
          executor.append(',');
        executor.append(entry.getKey());

        // MERGE THE RESULTSET
        final ODistributedResponse response = (ODistributedResponse) entry.getValue();
        final Object payload = response.getPayload();
        mergedPayload = (HashSet<Object>) OMultiValue.add(mergedPayload, payload);
      }
    }

    merged.setExecutorNodeName(executor.toString());
    merged.setPayload(mergedPayload);

    return merged;
  }

  public int getExpectedSynchronousResponses() {
    return expectedSynchronousResponses;
  }

  public int getQuorum() {
    return quorum;
  }

  public boolean waitForSynchronousResponses() throws InterruptedException {
    final long beginTime = System.currentTimeMillis();

    lock.lock();
    try {

      if ((waitForLocalNode && !receivedCurrentNode) || receivedResponses < expectedSynchronousResponses) {
        // WAIT FOR THE RESPONSES
        if (!synchronousResponsesArrived.await(synchTimeout, TimeUnit.MILLISECONDS))
          return false;
      }

      return true;
    } finally {
      lock.unlock();

      Orient
          .instance()
          .getProfiler()
          .stopChrono("distributed.replication.synchResponses",
              "Time to collect all the synchronous responses from distributed nodes", beginTime);
    }
  }

  public boolean isWaitForLocalNode() {
    return waitForLocalNode;
  }

  public boolean isReceivedCurrentNode() {
    return receivedCurrentNode;
  }

  public ODistributedResponse getResponse(final RESULT_STRATEGY resultStrategy) {
    manageConflicts();

    if (receivedResponses == 0)
      throw new ODistributedException("No response received from any of nodes " + getExpectedNodes() + " for request " + request);

    if (!isMinimumQuorumReached()) {
      // QUORUM NOT REACHED, UNDO REQUEST
      // TODO: UNDO
      request.undo();
      throw new ODistributedException("Quorum " + getQuorum() + " not reached for request: " + request);
    }

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);

    switch (resultStrategy) {
    case ANY:
      return bestResponsesGroup.get(0);

    case MERGE:
      // return merge( m new OHazelcastDistributedResponse(firstResponse.getRequestId(), null, firstResponse.getSenderNodeName(),
      // firstResponse.getSenderThreadId(), null));
      return bestResponsesGroup.get(0);

    case UNION:
      final Map<String, Object> payloads = new HashMap<String, Object>();
      for (Map.Entry<String, Object> entry : responses.entrySet())
        if (entry.getValue() != NO_RESPONSE)
          payloads.put(entry.getKey(), ((ODistributedResponse) entry.getValue()).getPayload());
      final ODistributedResponse response = bestResponsesGroup.get(0);
      response.setExecutorNodeName(responses.keySet().toString());
      response.setPayload(payloads);
      return response;
    }

    return bestResponsesGroup.get(0);
  }
}
