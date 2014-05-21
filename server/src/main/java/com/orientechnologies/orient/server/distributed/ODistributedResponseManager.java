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
import java.util.Collection;
import java.util.Collections;
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
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * Asynchronous response manager
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedResponseManager {
  private static final String                    NO_RESPONSE                 = "waiting-for-response";
  private final ODistributedServerManager        dManager;
  private final ODistributedRequest              request;
  private final long                             sentOn;
  private final HashMap<String, Object>          responses                   = new HashMap<String, Object>();
  private final boolean                          groupResponsesByResult;
  private final List<List<ODistributedResponse>> responseGroups              = new ArrayList<List<ODistributedResponse>>();
  private final int                              expectedSynchronousResponses;
  private final long                             synchTimeout;
  private final long                             totalTimeout;
  private final Lock                             synchronousResponsesLock    = new ReentrantLock();
  private final Condition                        synchronousResponsesArrived = synchronousResponsesLock.newCondition();
  private int                                    receivedResponses           = 0;
  private int                                    quorum;
  private boolean                                waitForLocalNode;
  private volatile boolean                       receivedCurrentNode;

  public ODistributedResponseManager(final ODistributedServerManager iManager, final ODistributedRequest iRequest,
      final Collection<String> expectedResponses, final int iExpectedSynchronousResponses, final int iQuorum,
      final boolean iWaitForLocalNode, final long iSynchTimeout, final long iTotalTimeout, final boolean iGroupResponsesByResult) {
    this.dManager = iManager;
    this.request = iRequest;
    this.sentOn = System.currentTimeMillis();
    this.expectedSynchronousResponses = iExpectedSynchronousResponses;
    this.quorum = iQuorum;
    this.waitForLocalNode = iWaitForLocalNode;
    this.synchTimeout = iSynchTimeout;
    this.totalTimeout = iTotalTimeout;
    this.groupResponsesByResult = iGroupResponsesByResult;

    for (String node : expectedResponses)
      responses.put(node, NO_RESPONSE);

    if (groupResponsesByResult)
      responseGroups.add(new ArrayList<ODistributedResponse>());
  }

  /**
   * Not synchronized, it's called when a message arrives
   * 
   * @param response
   *          Received response to collect
   * @return True if all the nodes responded, otherwise false
   */
  public boolean collectResponse(final ODistributedResponse response) {
    final String executorNode = response.getExecutorNodeName();

    if (!responses.containsKey(executorNode)) {
      ODistributedServerLog.warn(this, response.getSenderNodeName(), executorNode, DIRECTION.IN,
          "received response for request %s from unexpected node. Expected are: %s", request, getExpectedNodes());

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

    boolean completed = false;
    synchronized (responseGroups) {
      responses.put(executorNode, response);
      receivedResponses++;

      if (waitForLocalNode && response.isExecutedOnLocalNode())
        receivedCurrentNode = true;

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, response.getSenderNodeName(), executorNode, DIRECTION.IN,
            "received response '%s' for request %s (receivedCurrentNode=%s receivedResponses=%d)", response, request,
            receivedCurrentNode, receivedResponses);

      // PUT THE RESPONSE IN THE RIGHT RESPONSE GROUP
      if (groupResponsesByResult) {
        boolean foundBucket = false;
        for (int i = 0; i < responseGroups.size(); ++i) {
          final List<ODistributedResponse> sameResponse = responseGroups.get(i);
          if (sameResponse.isEmpty() || (sameResponse.get(0).getPayload() == null && response.getPayload() == null)
              || sameResponse.get(0).getPayload().equals(response.getPayload())) {
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
      }

      completed = getExpectedResponses() == receivedResponses;

      if (receivedResponses >= expectedSynchronousResponses && (!waitForLocalNode || receivedCurrentNode)) {
        if (completed || isMinimumQuorumReached(false)) {
          // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
          synchronousResponsesLock.lock();
          try {
            synchronousResponsesArrived.signalAll();
          } finally {
            synchronousResponsesLock.unlock();
          }
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

  public boolean isMinimumQuorumReached(final boolean iCheckAvailableNodes) {
    if (isWaitForLocalNode() && !isReceivedCurrentNode()) {
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), dManager.getLocalNodeName(), DIRECTION.IN,
          "no response received from local node about request %s", request);
      return false;
    }

    if (quorum == 0)
      return true;

    if (!groupResponsesByResult)
      return receivedResponses >= quorum;

    synchronized (responseGroups) {
      for (List<ODistributedResponse> group : responseGroups)
        if (group.size() >= quorum)
          return true;

      if (getReceivedResponsesCount() < quorum && iCheckAvailableNodes) {
        final ODistributedConfiguration dbConfig = dManager.getDatabaseConfiguration(getDatabaseName());
        if (!dbConfig.getFailureAvailableNodesLessQuorum("*")) {
          // CHECK IF ANY NODE IS OFFLINE
          int availableNodes = 0;
          for (Map.Entry<String, Object> r : responses.entrySet()) {
            if (dManager.isNodeAvailable(r.getKey(), getDatabaseName()))
              availableNodes++;
          }

          if (availableNodes < quorum) {
            ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "overridden quorum (%d) for request %s because available nodes (%d) are less than quorum, received responses: %s",
                quorum, request, availableNodes, responses);
            return true;
          }
        }
      }
    }

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

  /**
   * Returns all the responses in conflict.
   * 
   * @return
   */
  public List<ODistributedResponse> getConflictResponses() {
    final List<ODistributedResponse> servers = new ArrayList<ODistributedResponse>();
    int bestGroupSoFar = getBestResponsesGroup();
    for (int i = 0; i < responseGroups.size(); ++i) {
      if (i != bestGroupSoFar) {
        for (ODistributedResponse r : responseGroups.get(i))
          servers.add(r);
      }
    }
    return servers;
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

  /**
   * Returns the list of node names that provided a response.
   */
  public List<String> getRespondingNodes() {
    final List<String> respondedNodes = new ArrayList<String>();
    for (Map.Entry<String, Object> entry : responses.entrySet())
      if (entry.getValue() != NO_RESPONSE)
        respondedNodes.add(entry.getKey());
    return respondedNodes;
  }

  /**
   * Returns the list of node names that didn't provide a response.
   */
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

  /**
   * Waits until the minimum responses are collected or timeout occurs. If "waitForLocalNode" wait also for local node.
   * 
   * @return True if the received responses are major or equals then the expected synchronous responses, otherwise false
   * @throws InterruptedException
   */
  public boolean waitForSynchronousResponses() throws InterruptedException {
    final long beginTime = System.currentTimeMillis();

    synchronousResponsesLock.lock();
    try {

      long currentTimeout = synchTimeout;
      while (currentTimeout > 0 && ((waitForLocalNode && !receivedCurrentNode) || receivedResponses < expectedSynchronousResponses)) {
        // WAIT FOR THE RESPONSES
        synchronousResponsesArrived.await(currentTimeout, TimeUnit.MILLISECONDS);

        if ((!waitForLocalNode || receivedCurrentNode) && (receivedResponses >= expectedSynchronousResponses))
          // OK
          break;

        final long now = System.currentTimeMillis();
        final long elapsed = now - beginTime;
        currentTimeout = synchTimeout - elapsed;

        final long lastMemberAddedOn = dManager.getLastClusterChangeOn();
        if (lastMemberAddedOn > 0 && now - lastMemberAddedOn < (synchTimeout * 2)) {
          // NEW NODE DURING WAIT: ENLARGE TIMEOUT
          currentTimeout += synchTimeout;
          ODistributedServerLog.info(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "cluster shape changed during request %s: enlarge timeout +%dms, wait again for %dms", request, synchTimeout,
              currentTimeout);
        }
      }

      return receivedResponses >= expectedSynchronousResponses;

    } finally {
      synchronousResponsesLock.unlock();

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

  public ODistributedResponse getFinalResponse() {
    manageConflicts();

    if (receivedResponses == 0)
      throw new ODistributedException("No response received from any of nodes " + getExpectedNodes() + " for request " + request);

    // MANAGE THE RESULT BASED ON RESULT STRATEGY
    switch (request.getTask().getResultStrategy()) {
    case ANY:
      // DEFAULT: RETURN BEST ANSWER
      break;

    case UNION: {
      // COLLECT ALL THE RESPONSE IN A MAP OF <NODE, RESULT>
      final Map<String, Object> payloads = new HashMap<String, Object>();
      for (Map.Entry<String, Object> entry : responses.entrySet())
        if (entry.getValue() != NO_RESPONSE)
          payloads.put(entry.getKey(), ((ODistributedResponse) entry.getValue()).getPayload());

      final ODistributedResponse response = (ODistributedResponse) responses.values().iterator().next();
      response.setExecutorNodeName(responses.keySet().toString());
      response.setPayload(payloads);
      return response;
    }
    }

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);
    return bestResponsesGroup.get(0);
  }

  public String getDatabaseName() {
    return request.getDatabaseName();
  }

  protected void manageConflicts() {
    if (!groupResponsesByResult || request.getTask().getQuorumType() == OAbstractRemoteTask.QUORUM_TYPE.NONE)
      // NO QUORUM
      return;

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);

    final int maxCoherentResponses = bestResponsesGroup.size();
    final int conflicts = getExpectedResponses() - maxCoherentResponses;

    if (isMinimumQuorumReached(true)) {
      // QUORUM SATISFIED

      if (responseGroups.size() == 1)
        // NO CONFLICT
        return;

      if (checkNoWinnerCase(bestResponsesGroup))
        return;

      // NO FIFTY/FIFTY CASE: FIX THE CONFLICTED NODES BY OVERWRITING THE RECORD WITH THE WINNER'S RESULT
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "detected %d conflicts, but the quorum (%d) has been reached. Fixing remote records. Request: %s", conflicts, quorum,
          request);

      fixNodesInConflict(bestResponsesGroup);

    } else {
      // QUORUM HASN'T BEEN REACHED
      ODistributedServerLog
          .warn(
              this,
              dManager.getLocalNodeName(),
              null,
              DIRECTION.NONE,
              "detected %d node(s) in timeout or in conflict and quorum (%d) has not been reached, rolling back changes for request: %s",
              conflicts, quorum, request);

      undoRequest();

      final StringBuilder msg = new StringBuilder();
      msg.append("Quorum " + getQuorum() + " not reached for request=" + request + ".");
      final List<ODistributedResponse> res = getConflictResponses();
      if (res.isEmpty())
        msg.append(" No server in conflict. ");
      else {
        msg.append(" Servers in timeout/conflict are:");
        for (ODistributedResponse r : res) {
          msg.append("\n - ");
          msg.append(r.getExecutorNodeName());
          msg.append(": ");
          msg.append(r.getPayload());
        }
        msg.append("\n");
      }

      msg.append("Received: ");
      msg.append(responses);

      throw new ODistributedException(msg.toString());
    }
  }

  protected void undoRequest() {
    for (ODistributedResponse r : getReceivedResponses()) {
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "sending undo message for request=%s to server %s", request, r.getExecutorNodeName());

      final OAbstractRemoteTask task = request.getTask();
      if (task instanceof OAbstractReplicatedTask) {
        final OAbstractRemoteTask undoTask = ((OAbstractReplicatedTask) task).getUndoTask(request, r.getPayload());

        if (undoTask != null)
          dManager.sendRequest(request.getDatabaseName(), null, Collections.singleton(r.getExecutorNodeName()), undoTask,
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);
      }
    }
  }

  protected void fixNodesInConflict(List<ODistributedResponse> bestResponsesGroup) {
    final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

    for (List<ODistributedResponse> responseGroup : responseGroups) {
      if (responseGroup != bestResponsesGroup) {
        // CONFLICT GROUP: FIX THEM ONE BY ONE
        for (ODistributedResponse r : responseGroup) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "fixing response for request=%s in server %s to be: %s", request, r.getExecutorNodeName(), goodResponse);

          final OAbstractRemoteTask fixTask = ((OAbstractReplicatedTask) request.getTask()).getFixTask(request, r.getPayload(),
              goodResponse.getPayload());

          if (fixTask != null)
            dManager.sendRequest(request.getDatabaseName(), null, Collections.singleton(r.getExecutorNodeName()), fixTask,
                ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);
        }
      }
    }
  }

  protected boolean checkNoWinnerCase(List<ODistributedResponse> bestResponsesGroup) {
    // CHECK IF THERE ARE 2 PARTITIONS EQUAL IN SIZE
    int maxCoherentResponses = bestResponsesGroup.size();

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
                "detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached. Request: %s",
                a, b, quorum, request);

        // DON'T FIX RECORDS BECAUSE THERE ISN'T A CLEAR WINNER
        return true;
      }
    }
    return false;
  }
}
