/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronous response manager
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedResponseManager {
  public static final int                        ADDITIONAL_TIMEOUT_CLUSTER_SHAPE = 10000;
  private static final OResponseData             NO_RESPONSE                      = new OResponseData("waiting-for-response", 0);
  private final ODistributedServerManager        dManager;
  private final ODistributedRequest              request;
  private final long                             sentOn;
  private final Set<String>                      nodesConcurInQuorum;
  private final HashMap<String, OResponseData>   responses                        = new HashMap<String, OResponseData>();
  private final boolean                          groupResponsesByResult;
  private final List<List<ODistributedResponse>> responseGroups                   = new ArrayList<List<ODistributedResponse>>();
  private int                                    totalExpectedResponses;
  private final long                             synchTimeout;
  private final long                             totalTimeout;
  private final Lock                             synchronousResponsesLock         = new ReentrantLock();
  private final Condition                        synchronousResponsesArrived      = synchronousResponsesLock.newCondition();
  private final int                              quorum;
  private final boolean                          waitForLocalNode;
  private ODistributedResponse                   localResponse;
  private volatile int                           receivedResponses                = 0;
  private volatile boolean                       receivedCurrentNode;

  public static class OResponseData {
    public long   receivedOn;
    public Object response;

    public OResponseData(final Object value, final long receivedOn) {
      this.response = value;
      this.receivedOn = receivedOn;
    }
  }

  public ODistributedResponseManager(final ODistributedServerManager iManager, final ODistributedRequest iRequest,
      final Collection<String> expectedResponses, final Set<String> iNodesConcurInQuorum, final int iTotalExpectedResponses,
      final int iQuorum, final boolean iWaitForLocalNode, final long iSynchTimeout, final long iTotalTimeout,
      final boolean iGroupResponsesByResult) {
    this.dManager = iManager;
    this.request = iRequest;
    this.sentOn = System.nanoTime();
    this.totalExpectedResponses = iTotalExpectedResponses;
    this.quorum = iQuorum;
    this.waitForLocalNode = iWaitForLocalNode;
    this.synchTimeout = iSynchTimeout;
    this.totalTimeout = iTotalTimeout;
    this.groupResponsesByResult = iGroupResponsesByResult;
    this.nodesConcurInQuorum = iNodesConcurInQuorum;

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
    final String senderNode = response.getSenderNodeName();

    synchronousResponsesLock.lock();
    try {
      if (!executorNode.equals(dManager.getLocalNodeName()) && !responses.containsKey(executorNode)) {
        ODistributedServerLog.warn(this, senderNode, executorNode, DIRECTION.IN,
            "Received response for request (%s) from unexpected node. Expected are: %s", request, getExpectedNodes());

        Orient.instance().getProfiler().updateCounter("distributed.node.unexpectedNodeResponse",
            "Number of responses from unexpected nodes", +1);

        return false;
      }

      final long sentOnInMs = getSentOn();
      Orient.instance().getProfiler().stopChrono("distributed.node.latency", "Latency of distributed messages", sentOnInMs,
          "distributed.node.latency");

      Orient.instance().getProfiler().stopChrono("distributed.node." + executorNode + ".latency",
          "Latency of distributed messages per node", sentOnInMs, "distributed.node.*.latency");

      responses.put(executorNode, new OResponseData(response, System.nanoTime()));
      receivedResponses++;

      if (waitForLocalNode && executorNode.equals(senderNode))
        receivedCurrentNode = true;

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, senderNode, executorNode, DIRECTION.IN,
            "Received response '%s' for request (%s) (receivedCurrentNode=%s receivedResponses=%d totalExpectedResponses=%d quorum=%d)",
            response, request, receivedCurrentNode, receivedResponses, totalExpectedResponses, quorum);

      if (groupResponsesByResult) {
        // PUT THE RESPONSE IN THE RIGHT RESPONSE GROUP
        // TODO: AVOID TO KEEP ALL THE RESULT FOR THE SAME RESP GROUP, BUT RATHER THE FIRST ONE + COUNTER
        final Object responsePayload = response.getPayload();

        boolean foundBucket = false;
        for (int i = 0; i < responseGroups.size(); ++i) {
          final List<ODistributedResponse> responseGroup = responseGroups.get(i);

          if (responseGroup.isEmpty())
            // ABSENT
            foundBucket = true;
          else {
            final Object rgPayload = responseGroup.get(0).getPayload();

            if (rgPayload == null && responsePayload == null)
              // BOTH NULL
              foundBucket = true;
            else if (rgPayload != null) {
              if (rgPayload.equals(responsePayload))
                // SAME RESULT
                foundBucket = true;
              else if (rgPayload instanceof Collection && responsePayload instanceof Collection) {
                if (OMultiValue.equals((Collection) rgPayload, (Collection) responsePayload))
                  // COLLECTIONS WITH THE SAME VALUES
                  foundBucket = true;
              }
            }
          }

          if (foundBucket) {
            responseGroup.add(response);
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

      boolean completed = getExpectedResponses() == receivedResponses;

      if (completed || isMinimumQuorumReached()) {
        // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
        notifyWaiters();
      }
      return completed;

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public void notifyWaiters() {
    synchronousResponsesLock.lock();
    try {
      synchronousResponsesArrived.signalAll();
    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public ODistributedRequestId getMessageId() {
    return request.getId();
  }

  /**
   * Return the timestamp the request has been sent converted in ms (internally the time is r.
   */
  public long getSentOn() {
    return sentOn / 1000000;
  }

  @SuppressWarnings("unchecked")
  public ODistributedResponse merge(final ODistributedResponse merged) {
    final StringBuilder executor = new StringBuilder(64);
    HashSet<Object> mergedPayload = new HashSet<Object>();

    for (Map.Entry<String, OResponseData> entry : responses.entrySet()) {
      final OResponseData r = entry.getValue();
      if (r != NO_RESPONSE) {
        // APPEND THE EXECUTOR
        if (executor.length() > 0)
          executor.append(',');
        executor.append(entry.getKey());

        // MERGE THE RESULTSET
        final ODistributedResponse response = (ODistributedResponse) r.response;
        final Object payload = response.getPayload();
        mergedPayload = (HashSet<Object>) OMultiValue.add(mergedPayload, payload);
      }
    }

    merged.setExecutorNodeName(executor.toString());
    merged.setPayload(mergedPayload);

    return merged;
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
      while (currentTimeout > 0 && receivedResponses < totalExpectedResponses && !isMinimumQuorumReached()) {

        // WAIT FOR THE RESPONSES
        synchronousResponsesArrived.await(currentTimeout, TimeUnit.MILLISECONDS);

        if (isMinimumQuorumReached() || receivedResponses >= totalExpectedResponses)
          // OK
          return true;

        if (Thread.currentThread().isInterrupted()) {
          // INTERRUPTED
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "Thread has been interrupted wait for request (%s)", request);
          Thread.currentThread().interrupt();
          break;
        }

        final long now = System.currentTimeMillis();
        final long elapsed = now - beginTime;
        currentTimeout = synchTimeout - elapsed;

        // CHECK IF ANY NODE ARE UNREACHABLE IN THE MEANWHILE
        int synchronizingNodes = 0;
        int missingActiveNodes = 0;

        synchronousResponsesLock.lock();
        try {
          for (Iterator<Map.Entry<String, OResponseData>> iter = responses.entrySet().iterator(); iter.hasNext();) {
            final Map.Entry<String, OResponseData> curr = iter.next();

            if (curr.getValue() == NO_RESPONSE) {
              // ANALYZE THE NODE WITHOUT A RESPONSE
              final ODistributedServerManager.DB_STATUS dbStatus = dManager.getDatabaseStatus(curr.getKey(), getDatabaseName());
              switch (dbStatus) {
              case SYNCHRONIZING:
                synchronizingNodes++;
                missingActiveNodes++;
                break;
              case ONLINE:
                missingActiveNodes++;
                break;
              }
            }
          }

        } finally {
          synchronousResponsesLock.unlock();
        }

        if (missingActiveNodes == 0) {
          // NO MORE ACTIVE NODES TO WAIT
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "No more active nodes to wait for request (%s): anticipate timeout (saved %d ms)", request, currentTimeout);
          break;
        }

        final long lastClusterChange = dManager.getLastClusterChangeOn();
        if (lastClusterChange > 0 && now - lastClusterChange < (synchTimeout + ADDITIONAL_TIMEOUT_CLUSTER_SHAPE)) {
          // CHANGED CLUSTER SHAPE DURING WAIT: ENLARGE TIMEOUT
          currentTimeout = synchTimeout;
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "Cluster shape changed during request (%s): enlarge timeout +%dms, wait again for %dms", request, synchTimeout,
              currentTimeout);
          continue;
        } else if (synchronizingNodes > 0) {
          // SOME NODE IS SYNCHRONIZING: WAIT FOR THEM
          currentTimeout = synchTimeout;
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "%d nodes are in synchronization mode during request (%s): enlarge timeout +%dms, wait again for %dms",
              synchronizingNodes, request, synchTimeout, currentTimeout);
        }
      }

      return isMinimumQuorumReached() || receivedResponses >= totalExpectedResponses;

    } finally {
      synchronousResponsesLock.unlock();

      Orient.instance().getProfiler().stopChrono("distributed.synchResponses",
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
    synchronousResponsesLock.lock();
    try {

      final RuntimeException failure = manageConflicts();
      if (failure != null)
        return new ODistributedResponse(request.getId(), dManager.getLocalNodeName(), dManager.getLocalNodeName(), failure);

      if (receivedResponses == 0) {
        if (quorum > 0 && !request.getTask().isIdempotent())
          throw new ODistributedOperationException(
              "No response received from any of nodes " + getExpectedNodes() + " for request " + request);

        // NO QUORUM, RETURN NULL
        return null;
      }

      // MANAGE THE RESULT BASED ON RESULT STRATEGY
      switch (request.getTask().getResultStrategy()) {
      case ANY:
        // DEFAULT: RETURN BEST ANSWER
        break;

      case UNION: {
        // COLLECT ALL THE RESPONSE IN A MAP OF <NODE, RESULT>
        final Map<String, Object> payloads = new HashMap<String, Object>();
        for (Map.Entry<String, OResponseData> entry : responses.entrySet()) {
          final OResponseData r = entry.getValue();
          if (r != NO_RESPONSE)
            payloads.put(entry.getKey(), ((ODistributedResponse) r.response).getPayload());
        }

        final ODistributedResponse response = getReceivedResponses().iterator().next();
        response.setExecutorNodeName(responses.keySet().toString());
        response.setPayload(payloads);
        return response;
      }
      }

      final int bestResponsesGroupIndex = getBestResponsesGroup();
      final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);
      return bestResponsesGroup.get(0);

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public String getDatabaseName() {
    return request.getDatabaseName();
  }

  public long getSynchTimeout() {
    return synchTimeout;
  }

  public void timeout() {
    synchronousResponsesLock.lock();
    try {
      manageConflicts();
    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  /**
   * Returns the list of node names that didn't provide a response.
   */
  public List<String> getMissingNodes() {
    synchronousResponsesLock.lock();
    try {

      final List<String> missingNodes = new ArrayList<String>();
      for (Map.Entry<String, OResponseData> entry : responses.entrySet())
        if (entry.getValue() == NO_RESPONSE)
          missingNodes.add(entry.getKey());
      return missingNodes;

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public Set<String> getExpectedNodes() {
    synchronousResponsesLock.lock();
    try {
      return new HashSet<String>(responses.keySet());
    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  /**
   * Returns the list of node names that provided a response.
   */
  public List<String> getRespondingNodes() {
    final List<String> respondedNodes = new ArrayList<String>();
    synchronousResponsesLock.lock();
    try {
      for (Map.Entry<String, OResponseData> entry : responses.entrySet())
        if (entry.getValue() != NO_RESPONSE)
          respondedNodes.add(entry.getKey());
    } finally {
      synchronousResponsesLock.unlock();
    }
    return respondedNodes;
  }

  /**
   * Returns all the responses in conflict.
   *
   * @return
   */
  protected List<ODistributedResponse> getConflictResponses() {
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

  protected int getExpectedResponses() {
    return responses.size();
  }

  protected int getMissingResponses() {
    return getExpectedResponses() - receivedResponses;
  }

  protected int getReceivedResponsesCount() {
    return receivedResponses;
  }

  protected long getTotalTimeout() {
    return totalTimeout;
  }

  /**
   * Returns the biggest response group.
   *
   * @return
   */
  protected int getBestResponsesGroup() {
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

  protected boolean isMinimumQuorumReached() {
    if (isWaitForLocalNode() && !isReceivedCurrentNode())
      return false;

    if (quorum == 0)
      return true;

    if (groupResponsesByResult) {
      for (List<ODistributedResponse> group : responseGroups)
        if (group.size() >= quorum) {
          int responsesForQuorum = 0;
          for (ODistributedResponse r : group) {
            if (nodesConcurInQuorum.contains(r.getExecutorNodeName())) {
              final Object payload = r.getPayload();

              if (payload instanceof Throwable) {
                if (payload instanceof ODistributedRecordLockedException)
                  // JUST ONE ODistributedRecordLockedException IS ENOUGH TO FAIL THE OPERATION BECAUSE RESOURCES CANNOT BE LOCKED
                  return false;
              } else if (++responsesForQuorum >= quorum)
                // QUORUM REACHED
                break;
            }
          }

          return responsesForQuorum >= quorum;
        }
    } else {
      if (receivedResponses >= quorum) {
        int responsesForQuorum = 0;
        for (Map.Entry<String, OResponseData> response : responses.entrySet()) {
          if (response.getValue() != NO_RESPONSE && nodesConcurInQuorum.contains(response.getKey()))
            if (++responsesForQuorum >= quorum)
              // QUORUM REACHED
              break;
        }

        return responsesForQuorum >= quorum;
      }

      return receivedResponses >= quorum;
    }

    return false;
  }

  public Map<String, Long> getResponsesLatency() {
    final Map<String, Long> latencies = new HashMap<String, Long>();
    for (Map.Entry<String, OResponseData> entry : responses.entrySet())
      if (entry.getValue() != NO_RESPONSE)
        latencies.put(entry.getKey(), entry.getValue().receivedOn);
    return latencies;
  }

  /**
   * Returns the received response objects.
   */
  protected List<ODistributedResponse> getReceivedResponses() {
    final List<ODistributedResponse> parsed = new ArrayList<ODistributedResponse>();
    for (OResponseData r : responses.values())
      if (r != NO_RESPONSE)
        parsed.add((ODistributedResponse) r.response);
    return parsed;
  }

  protected RuntimeException manageConflicts() {
    if (!groupResponsesByResult || request.getTask().getQuorumType() == OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE)
      // NO QUORUM
      return null;

    if (dManager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // CURRENT NODE OFFLINE: JUST RETURN
      return null;

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);

    final int maxCoherentResponses = bestResponsesGroup.size();
    final int conflicts = getExpectedResponses() - (maxCoherentResponses);

    if (isMinimumQuorumReached()) {
      // QUORUM SATISFIED
      if (responseGroups.size() == 1)
        // NO CONFLICT
        return null;

      if (checkNoWinnerCase(bestResponsesGroup))
        return null;

      if (fixNodesInConflict(bestResponsesGroup, conflicts))
        // FIX SUCCEED
        return null;
    }

    // QUORUM HASN'T BEEN REACHED
    ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
        "Detected %d node(s) in timeout or in conflict and quorum (%d) has not been reached, rolling back changes for request (%s)",
        conflicts, quorum, request);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, composeConflictMessage());

    if (!undoRequest()) {
      // SKIP UNDO
      return null;
    }

    // CHECK IF THERE IS AT LEAST ONE ODistributedRecordLockedException
    for (OResponseData r : responses.values()) {
      if (r.response instanceof ODistributedRecordLockedException)
        throw (ODistributedRecordLockedException) r.response;
    }

    final Object goodResponsePayload = bestResponsesGroup.isEmpty() ? null : bestResponsesGroup.get(0).getPayload();
    if (goodResponsePayload instanceof RuntimeException)
      // RESPONSE IS ALREADY AN EXCEPTION: THROW THIS
      return (RuntimeException) goodResponsePayload;
    else if (goodResponsePayload instanceof Throwable)
      return OException.wrapException(new ODistributedException(composeConflictMessage()), (Throwable) goodResponsePayload);
    else {
      if (responseGroups.size() <= 2) {
        // CHECK IF THE BAD RESPONSE IS AN EXCEPTION, THEN PROPAGATE IT
        for (int i = 0; i < responseGroups.size(); ++i) {
          if (i == bestResponsesGroupIndex)
            continue;
          final List<ODistributedResponse> badResponses = responseGroups.get(i);
          if (badResponses.get(0).getPayload() instanceof RuntimeException)
            return (RuntimeException) badResponses.get(0).getPayload();
        }
      }

      return new ODistributedOperationException(composeConflictMessage());
    }
  }

  private String composeConflictMessage() {
    final StringBuilder msg = new StringBuilder(256);
    msg.append("Quorum " + getQuorum() + " not reached for request (" + request + "). Elapsed="
        + ((System.nanoTime() - sentOn) / 1000000f) + "ms");
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
    for (Map.Entry<String, OResponseData> response : responses.entrySet()) {
      msg.append("\n - ");
      msg.append(response.getKey());
      msg.append(": ");
      msg.append(response.getValue().response);
    }

    return msg.toString();
  }

  protected boolean undoRequest() {
    final ORemoteTask task = request.getTask();

    if (task.isIdempotent()) {
      // NO UNDO IS NECESSARY
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "No undo because the task (%s) is idempotent", task);
      return false;
    }

    // DETERMINE IF ANY CREATE FAILED TO RESTORE RIDS
    for (ODistributedResponse r : getReceivedResponses()) {
      if (r.getPayload() instanceof Throwable)
        // NO NEED TO UNDO AN OPERATION THAT RETURNED EXCEPTION
        // TODO: CONSIDER DIFFERENT TYPE OF EXCEPTION, SOME OF THOSE COULD REQUIRE AN UNDO
        continue;

      if (r == localResponse)
        // SKIP LOCAL SERVER (IT'S MANAGED APART)
        continue;

      final String targetNode = r.getExecutorNodeName();
      if (targetNode.equals(dManager.getLocalNodeName()))
        // AVOID TO UNDO LOCAL NODE BECAUSE THE OPERATION IS MANAGED APART
        continue;

      if (task instanceof OAbstractReplicatedTask) {
        final ORemoteTask undoTask = ((OAbstractReplicatedTask) task).getUndoTask(request.getId());

        if (undoTask != null) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), targetNode, DIRECTION.OUT,
              "Sending undo message (%s) for request (%s) to server %s", undoTask, request, targetNode);

          dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(targetNode), undoTask,
              dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);
        }
      }
    }
    return true;
  }

  protected boolean fixNodesInConflict(final List<ODistributedResponse> bestResponsesGroup, final int conflicts) {
    // NO FIFTY/FIFTY CASE: FIX THE CONFLICTED NODES BY OVERWRITING THE RECORD WITH THE WINNER'S RESULT
    final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

    if (goodResponse.getPayload() instanceof Throwable)
      return false;

    ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
        "Detected %d conflicts, but the quorum (%d) has been reached. Fixing remote records. Request (%s)", conflicts, quorum,
        request);

    for (List<ODistributedResponse> responseGroup : responseGroups) {
      if (responseGroup != bestResponsesGroup) {
        // CONFLICT GROUP: FIX THEM ONE BY ONE
        for (ODistributedResponse r : responseGroup) {
          final ORemoteTask fixTask = ((OAbstractReplicatedTask) request.getTask()).getFixTask(request, request.getTask(),
              r.getPayload(), goodResponse.getPayload(), r.getExecutorNodeName(), dManager);

          if (fixTask == null)
            // FIX NOT AVAILABLE: UNDO THE ENTIRE OPERATION
            return false;

          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), r.getExecutorNodeName(), DIRECTION.OUT,
              "Sending fix message (%s) for response (%s) on request (%s) to be: %s", fixTask, r, request, goodResponse);

          dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(r.getExecutorNodeName()), fixTask,
              dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
        }
      }
    }
    return true;
  }

  protected boolean checkNoWinnerCase(final List<ODistributedResponse> bestResponsesGroup) {
    // CHECK IF THERE ARE 2 PARTITIONS EQUAL IN SIZE
    final int maxCoherentResponses = bestResponsesGroup.size();

    if (maxCoherentResponses < quorum)
      return false;

    for (List<ODistributedResponse> responseGroup : responseGroups) {
      if (responseGroup != bestResponsesGroup && responseGroup.size() == maxCoherentResponses) {
        final List<String> a = new ArrayList<String>();
        Object aResponse = null;
        for (ODistributedResponse r : bestResponsesGroup) {
          a.add(r.getExecutorNodeName());
          aResponse = r.getPayload();
        }

        final List<String> b = new ArrayList<String>();
        Object bResponse = null;
        for (ODistributedResponse r : responseGroup) {
          b.add(r.getExecutorNodeName());
          bResponse = r.getPayload();
        }

        final StringBuilder details = new StringBuilder();
        details.append(" A=").append(aResponse);
        details.append(", B=").append(bResponse);

        ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
            "Detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached. Request (%s) responses:%s",
            a, b, quorum, request, details);

        // DON'T FIX RECORDS BECAUSE THERE ISN'T A CLEAR WINNER
        return true;
      }
    }
    return false;
  }

  public void setLocalResult(final String localNodeName, final Serializable localResult) {
    localResponse = new ODistributedResponse(request.getId(), localNodeName, localNodeName, localResult);
    collectResponse(localResponse);
  }

  public void removeServerBecauseUnreachable(final String node) {
    if (responses.remove(node) != null) {
      totalExpectedResponses--;
      nodesConcurInQuorum.remove(node);
    }
  }
}
