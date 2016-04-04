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
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronous response manager
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class ODistributedResponseManager {
  public static final int                        ADDITIONAL_TIMEOUT_CLUSTER_SHAPE = 10000;
  private static final String                    NO_RESPONSE                      = "waiting-for-response";
  private final ODistributedServerManager        dManager;
  private final ODistributedRequest              request;
  private final long                             sentOn;
  private final HashMap<String, Object>          responses                        = new HashMap<String, Object>();
  private final boolean                          groupResponsesByResult;
  private final List<List<ODistributedResponse>> responseGroups                   = new ArrayList<List<ODistributedResponse>>();
  private final int                              expectedSynchronousResponses;
  private final long                             synchTimeout;
  private final long                             totalTimeout;
  private final Lock                             synchronousResponsesLock         = new ReentrantLock();
  private final Condition                        synchronousResponsesArrived      = synchronousResponsesLock.newCondition();
  private final int                              quorum;
  private final boolean                          waitForLocalNode;
  private volatile int                           receivedResponses                = 0;
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
    final String senderNode = response.getSenderNodeName();

    synchronousResponsesLock.lock();
    try {
      if (!executorNode.equals(dManager.getLocalNodeName()) && !responses.containsKey(executorNode)) {
        ODistributedServerLog.warn(this, senderNode, executorNode, DIRECTION.IN,
            "received response for request (%s) from unexpected node. Expected are: %s", request, getExpectedNodes());

        Orient.instance().getProfiler().updateCounter("distributed.node.unexpectedNodeResponse",
            "Number of responses from unexpected nodes", +1);

        return false;
      }

      Orient.instance().getProfiler().stopChrono("distributed.node.latency", "Latency of distributed messages", sentOn,
          "distributed.node.latency");

      Orient.instance().getProfiler().stopChrono("distributed.node." + executorNode + ".latency",
          "Latency of distributed messages per node", sentOn, "distributed.node.*.latency");

      boolean completed = false;
      responses.put(executorNode, response);
      receivedResponses++;

      if (waitForLocalNode && executorNode.equals(senderNode))
        receivedCurrentNode = true;

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, senderNode, executorNode, DIRECTION.IN,
            "received response '%s' for request (%s) (receivedCurrentNode=%s receivedResponses=%d expectedSynchronousResponses=%d quorum=%d)",
            response, request, receivedCurrentNode, receivedResponses, expectedSynchronousResponses, quorum);

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

      completed = getExpectedResponses() == receivedResponses;

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

  public long getSentOn() {
    return sentOn;
  }

  @SuppressWarnings("unchecked")
  public ODistributedResponse merge(final ODistributedResponse merged) {
    final StringBuilder executor = new StringBuilder(64);
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
      while (currentTimeout > 0 && !isMinimumQuorumReached() && receivedResponses < expectedSynchronousResponses) {

        // WAIT FOR THE RESPONSES
        synchronousResponsesArrived.await(currentTimeout, TimeUnit.MILLISECONDS);

        if (isMinimumQuorumReached() || receivedResponses >= expectedSynchronousResponses)
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
          for (Iterator<Map.Entry<String, Object>> iter = responses.entrySet().iterator(); iter.hasNext();) {
            final Map.Entry<String, Object> curr = iter.next();

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
              "no more active nodes to wait for request (%s): anticipate timeout (saved %d ms)", request, currentTimeout);
          break;
        }

        final long lastClusterChange = dManager.getLastClusterChangeOn();
        if (lastClusterChange > 0 && now - lastClusterChange < (synchTimeout + ADDITIONAL_TIMEOUT_CLUSTER_SHAPE)) {
          // CHANGED CLUSTER SHAPE DURING WAIT: ENLARGE TIMEOUT
          currentTimeout = synchTimeout;
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "cluster shape changed during request (%s): enlarge timeout +%dms, wait again for %dms", request, synchTimeout,
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

      return isMinimumQuorumReached() || receivedResponses >= expectedSynchronousResponses;

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
        if (quorum > 0)
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
        for (Map.Entry<String, Object> entry : responses.entrySet())
          if (entry.getValue() != NO_RESPONSE)
            payloads.put(entry.getKey(), ((ODistributedResponse) entry.getValue()).getPayload());

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
      for (Map.Entry<String, Object> entry : responses.entrySet())
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
      for (Map.Entry<String, Object> entry : responses.entrySet())
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
    if (isWaitForLocalNode() && !isReceivedCurrentNode()) {
      return false;
    }

    if (quorum == 0)
      return true;

    if (!groupResponsesByResult)
      return receivedResponses >= quorum;

    for (List<ODistributedResponse> group : responseGroups)
      if (group.size() >= quorum)
        return true;

    return false;
  }

  /**
   * Returns the received response objects.
   */
  protected List<ODistributedResponse> getReceivedResponses() {
    final List<ODistributedResponse> parsed = new ArrayList<ODistributedResponse>();
    for (Object r : responses.values())
      if (r != NO_RESPONSE)
        parsed.add((ODistributedResponse) r);
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

    // final Object payload = bestResponsesGroup.get(0).getPayload();
    // if (payload instanceof RuntimeException)
    // // PROPAGATE RUNTIME EXCEPTION
    // throw (RuntimeException) payload;
    // else if (payload instanceof Throwable)
    // // WRAP EXCEPTION
    // throw OException.wrapException(new ODistributedException("Error on executing distributed request"), (Throwable) payload);

    final int maxCoherentResponses = bestResponsesGroup.size();
    final int conflicts = getExpectedResponses() - (maxCoherentResponses);

    boolean requireUndo = false;
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
        "detected %d node(s) in timeout or in conflict and quorum (%d) has not been reached, rolling back changes for request (%s)",
        conflicts, quorum, request);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, composeConflictMessage());

    undoRequest();

    final Object goodResponsePayload = bestResponsesGroup.isEmpty() ? null : bestResponsesGroup.get(0).getPayload();
    if (goodResponsePayload instanceof RuntimeException)
      // RESPONSE IS ALREADY AN EXCEPTION: THROW THIS
      return (RuntimeException) goodResponsePayload;
    else if (goodResponsePayload instanceof Throwable)
      return OException.wrapException(new ODistributedException(composeConflictMessage()), (Throwable) goodResponsePayload);
    else
      return new ODistributedOperationException(composeConflictMessage());
  }

  private String composeConflictMessage() {
    final StringBuilder msg = new StringBuilder(256);
    msg.append("Quorum " + getQuorum() + " not reached for request (" + request + "). Elapsed="
        + (System.currentTimeMillis() - sentOn) + "ms");
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
    for (Map.Entry<String, Object> response : responses.entrySet()) {
      msg.append("\n - ");
      msg.append(response.getKey());
      msg.append(": ");
      msg.append(response.getValue());
    }

    return msg.toString();
  }

  protected void undoRequest() {
    // DETERMINE IF ANY CREATE FAILED TO RESTORE RIDS
    if (!realignRecordClusters()) {
      for (ODistributedResponse r : getReceivedResponses()) {
        if (r.getPayload() instanceof Throwable)
          // NO NEED TO UNDO AN OPERATION THAT RETURNED EXCEPTION
          // TODO: CONSIDER DIFFERENT TYPE OF EXCEPTION, SOME OF THOSE COULD REQUIRE AN UNDO
          continue;

        final ORemoteTask task = request.getTask();
        if (task instanceof OAbstractReplicatedTask) {
          final ORemoteTask undoTask = ((OAbstractReplicatedTask) task).getUndoTask(request.getId());

          if (undoTask != null) {
            final String targetNode = r.getExecutorNodeName();
            if (!dManager.getLocalNodeName().equals(targetNode)) {
              // REMOTE
              ODistributedServerLog.warn(this, dManager.getLocalNodeName(), targetNode, DIRECTION.OUT,
                  "Sending undo message (%s) for request (%s) to server %s", undoTask, request, targetNode);

              dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(targetNode), undoTask,
                  ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);
            }
          }
        }
      }
    }
  }

  private boolean realignRecordClusters() {
    final Set<Integer> clusterIds = new HashSet<Integer>();

    long minClusterPos = Long.MAX_VALUE;
    long maxClusterPos = Long.MIN_VALUE;

    for (ODistributedResponse r : getReceivedResponses()) {
      final ORemoteTask task = request.getTask();
      if (task instanceof OCreateRecordTask) {
        final Object badResponse = r.getPayload();
        if (badResponse instanceof Throwable)
          // FOUND AN EXCEPTION
          return false;

        final OPlaceholder badResult = (OPlaceholder) badResponse;
        clusterIds.add(badResult.getIdentity().getClusterId());

        if (clusterIds.size() > 1)
          // DIFFERENT CLUSTERS
          return false;

        final long clPos = ((OPlaceholder) badResponse).getIdentity().getClusterPosition();

        // DEFINE THE RANGE
        if (clPos < minClusterPos)
          minClusterPos = clPos;
        if (clPos > maxClusterPos)
          maxClusterPos = clPos;
      }
    }

    if (clusterIds.isEmpty())
      // NO CREATE
      return false;

    if (minClusterPos == maxClusterPos)
      // NO HOLE
      return false;

    // FOUND HOLE(S)
    for (ODistributedResponse r : getReceivedResponses()) {
      final ORemoteTask task = request.getTask();

      final OPlaceholder origPh = (OPlaceholder) r.getPayload();

      for (long i = minClusterPos; i <= maxClusterPos; ++i) {

        if (i > origPh.getIdentity().getClusterPosition()) {
          // CREATE THE RECORD FIRST AND THEN DELETE IT TO LEAVE THE HOLE AND ALIGN CLUSTER POS NUMERATION
          ORecordInternal.setIdentity(((OCreateRecordTask) task).getRecord(),
              ((OCreateRecordTask) task).getRecord().getIdentity().getClusterId(), -1);
          dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(r.getExecutorNodeName()), task,
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
        }

        final OPlaceholder ph = new OPlaceholder(new ORecordId(origPh.getIdentity().getClusterId(), i), -1);

        final ODeleteRecordTask undoTask = new ODeleteRecordTask(new ORecordId(ph.getIdentity()), ph.getVersion())
            .setDelayed(false);
        if (undoTask != null) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "sending undo message (%s) for request (%s) to server %s", undoTask, request, r.getExecutorNodeName());

          dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(r.getExecutorNodeName()), undoTask,
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
        }
      }
    }

    return true;
  }

  protected boolean fixNodesInConflict(final List<ODistributedResponse> bestResponsesGroup, final int conflicts) {
    // NO FIFTY/FIFTY CASE: FIX THE CONFLICTED NODES BY OVERWRITING THE RECORD WITH THE WINNER'S RESULT
    ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
        "detected %d conflicts, but the quorum (%d) has been reached. Fixing remote records. Request (%s)", conflicts, quorum,
        request);

    final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

    if (goodResponse.getPayload() instanceof Throwable)
      return false;

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
              "sending fix message (%s) for response (%s) on request (%s) to be: %s", fixTask, r, request, goodResponse);

          dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(r.getExecutorNodeName()), fixTask,
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
        }
      }
    }
    return false;
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
            "detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached. Request (%s) responses:%s",
            a, b, quorum, request, details);

        // DON'T FIX RECORDS BECAUSE THERE ISN'T A CLEAR WINNER
        return true;
      }
    }
    return false;
  }
}
