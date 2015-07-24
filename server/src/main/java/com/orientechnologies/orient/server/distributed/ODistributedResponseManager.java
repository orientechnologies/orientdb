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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private volatile int                           discardedResponses               = 0;
  private volatile boolean                       receivedCurrentNode;
  private Object                                 responseLock                     = new Object();

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

    synchronized (responseLock) {
      if (!responses.containsKey(executorNode)) {
        ODistributedServerLog.warn(this, response.getSenderNodeName(), executorNode, DIRECTION.IN,
            "received response for request (%s) from unexpected node. Expected are: %s", request, getExpectedNodes());

        Orient.instance().getProfiler()
            .updateCounter("distributed.node.unexpectedNodeResponse", "Number of responses from unexpected nodes", +1);

        return false;
      }

      Orient.instance().getProfiler()
          .stopChrono("distributed.node.latency", "Latency of distributed messages", sentOn, "distributed.node.latency");

      Orient
          .instance()
          .getProfiler()
          .stopChrono("distributed.node." + executorNode + ".latency", "Latency of distributed messages per node", sentOn,
              "distributed.node.*.latency");

      boolean completed = false;
      responses.put(executorNode, response);
      receivedResponses++;

      if (waitForLocalNode && response.isExecutedOnLocalNode())
        receivedCurrentNode = true;

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(
                this,
                response.getSenderNodeName(),
                executorNode,
                DIRECTION.IN,
                "received response '%s' for request (%s) (receivedCurrentNode=%s receivedResponses=%d expectedSynchronousResponses=%d quorum=%d)",
                response, request, receivedCurrentNode, receivedResponses, expectedSynchronousResponses, quorum);

      if (response.getPayload() instanceof ODiscardedResponse)
        discardedResponses++;
      else if (groupResponsesByResult) {
        // PUT THE RESPONSE IN THE RIGHT RESPONSE GROUP
        // TODO: AVOID TO KEEP ALL THE RESULT FOR THE SAME RESP GROUP, BUT RATHER THE FIRST ONE + COUNTER
        boolean foundBucket = false;
        for (int i = 0; i < responseGroups.size(); ++i) {
          final List<ODistributedResponse> responseGroup = responseGroups.get(i);

          if (responseGroup.isEmpty())
            // ABSENT
            foundBucket = true;
          else {
            final Object rgPayload = responseGroup.get(0).getPayload();
            final Object responsePayload = response.getPayload();

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

      if (receivedResponses >= expectedSynchronousResponses && (!waitForLocalNode || receivedCurrentNode)) {
        if (completed || isMinimumQuorumReached(false)) {
          // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
          notifyWaiters();
        }
      }
      return completed;
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

  public long getMessageId() {
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
      while (currentTimeout > 0 && ((waitForLocalNode && !receivedCurrentNode) || receivedResponses < expectedSynchronousResponses)) {
        // WAIT FOR THE RESPONSES
        synchronousResponsesArrived.await(currentTimeout, TimeUnit.MILLISECONDS);

        if ((!waitForLocalNode || receivedCurrentNode) && (receivedResponses >= expectedSynchronousResponses))
          // OK
          break;

        if (Thread.currentThread().isInterrupted()) {
          // INTERRUPTED
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "thread has been interrupted wait for request (%s)", request);
          Thread.currentThread().interrupt();
          break;
        }

        final long now = System.currentTimeMillis();
        final long elapsed = now - beginTime;
        currentTimeout = synchTimeout - elapsed;

        // CHECK IF ANY NODE ARE UNREACHABLE IN THE MEANWHILE
        int synchronizingNodes = 0;
        int missingActiveNodes = 0;

        synchronized (responseLock) {
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

      return receivedResponses >= expectedSynchronousResponses;

    } finally {
      synchronousResponsesLock.unlock();

      Orient
          .instance()
          .getProfiler()
          .stopChrono("distributed.synchResponses", "Time to collect all the synchronous responses from distributed nodes",
              beginTime);
    }
  }

  public boolean isWaitForLocalNode() {
    return waitForLocalNode;
  }

  public boolean isReceivedCurrentNode() {
    return receivedCurrentNode;
  }

  public ODistributedResponse getFinalResponse() {
    synchronized (responseLock) {

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

        final ODistributedResponse response = (ODistributedResponse) getReceivedResponses().iterator().next();
        response.setExecutorNodeName(responses.keySet().toString());
        response.setPayload(payloads);
        return response;
      }
      }

      final int bestResponsesGroupIndex = getBestResponsesGroup();
      final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);
      return bestResponsesGroup.get(0);
    }
  }

  public String getDatabaseName() {
    return request.getDatabaseName();
  }

  public void timeout() {
    synchronized (responseLock) {
      manageConflicts();
    }
  }

  /**
   * Returns the list of node names that didn't provide a response.
   */
  public List<String> getMissingNodes() {
    synchronized (responseLock) {
      final List<String> missingNodes = new ArrayList<String>();
      for (Map.Entry<String, Object> entry : responses.entrySet())
        if (entry.getValue() == NO_RESPONSE)
          missingNodes.add(entry.getKey());
      return missingNodes;
    }
  }

  public Set<String> getExpectedNodes() {
    synchronized (responseLock) {
      return new HashSet<String>(responses.keySet());
    }
  }

  /**
   * Returns the list of node names that provided a response.
   */
  public List<String> getRespondingNodes() {
    final List<String> respondedNodes = new ArrayList<String>();
    synchronized (responseLock) {
      for (Map.Entry<String, Object> entry : responses.entrySet())
        if (entry.getValue() != NO_RESPONSE)
          respondedNodes.add(entry.getKey());
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

  protected boolean isMinimumQuorumReached(final boolean iCheckAvailableNodes) {
    if (isWaitForLocalNode() && !isReceivedCurrentNode()) {
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), dManager.getLocalNodeName(), DIRECTION.IN,
          "no response received from local node about request %s", request);
      return false;
    }

    if (quorum == 0)
      return true;

    if (!groupResponsesByResult)
      return receivedResponses >= quorum;

    for (List<ODistributedResponse> group : responseGroups)
      if (group.size() + discardedResponses >= quorum)
        return true;

    if (receivedResponses < quorum && iCheckAvailableNodes) {
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
              "overridden quorum (%d) for request (%s) because available nodes (%d) are less than quorum, received responses: %s",
              quorum, request, availableNodes, responses);
          return true;
        }
      }
    }

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

  protected void manageConflicts() {
    if (!groupResponsesByResult || request.getTask().getQuorumType() == OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE)
      // NO QUORUM
      return;

    if (dManager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // CURRENT NODE OFFLINE: JUST RETURN
      return;

    final int bestResponsesGroupIndex = getBestResponsesGroup();
    final List<ODistributedResponse> bestResponsesGroup = responseGroups.get(bestResponsesGroupIndex);

    final int maxCoherentResponses = bestResponsesGroup.size();
    final int conflicts = getExpectedResponses() - (maxCoherentResponses + discardedResponses);

    if (isMinimumQuorumReached(true)) {
      // QUORUM SATISFIED

      if (responseGroups.size() == 1)
        // NO CONFLICT
        return;

      if (checkNoWinnerCase(bestResponsesGroup))
        return;

      fixNodesInConflict(bestResponsesGroup, conflicts);

    } else {
      // QUORUM HASN'T BEEN REACHED
      ODistributedServerLog
          .warn(
              this,
              dManager.getLocalNodeName(),
              null,
              DIRECTION.NONE,
              "detected %d node(s) in timeout or in conflict and quorum (%d) has not been reached, rolling back changes for request (%s)",
              conflicts, quorum, request);

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
      msg.append(responses);

      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, msg.toString());

      undoRequest();

      throw new ODistributedException(msg.toString());
    }
  }

  protected void undoRequest() {
    for (ODistributedResponse r : getReceivedResponses()) {
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "sending undo message for request (%s) to server %s", request, r.getExecutorNodeName());

      final OAbstractRemoteTask task = request.getTask();
      if (task instanceof OAbstractReplicatedTask) {
        final OAbstractRemoteTask undoTask = ((OAbstractReplicatedTask) task).getUndoTask(request, r.getPayload());

        if (undoTask != null)
          dManager.sendRequest(request.getDatabaseName(), null, Collections.singleton(r.getExecutorNodeName()), undoTask,
              ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);
      }
    }
  }

  protected void fixNodesInConflict(final List<ODistributedResponse> bestResponsesGroup, final int conflicts) {
    // NO FIFTY/FIFTY CASE: FIX THE CONFLICTED NODES BY OVERWRITING THE RECORD WITH THE WINNER'S RESULT
    ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
        "detected %d conflicts, but the quorum (%d) has been reached. Fixing remote records. Request (%s)", conflicts, quorum,
        request);

    final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

    for (List<ODistributedResponse> responseGroup : responseGroups) {
      if (responseGroup != bestResponsesGroup) {
        // CONFLICT GROUP: FIX THEM ONE BY ONE
        for (ODistributedResponse r : responseGroup) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "fixing response (%s) for request (%s) in server %s to be: %s", r, request, r.getExecutorNodeName(), goodResponse);

          final OAbstractRemoteTask fixTask = ((OAbstractReplicatedTask) request.getTask()).getFixTask(request, request.getTask(),
              r.getPayload(), goodResponse.getPayload());

          if (fixTask != null)
            dManager.sendRequest(request.getDatabaseName(), null, Collections.singleton(r.getExecutorNodeName()), fixTask,
                ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);
        }
      }
    }
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

        ODistributedServerLog
            .error(
                this,
                dManager.getLocalNodeName(),
                null,
                DIRECTION.NONE,
                "detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached. Request (%s) responses:%s",
                a, b, quorum, request, details);

        // DON'T FIX RECORDS BECAUSE THERE ISN'T A CLEAR WINNER
        return true;
      }
    }
    return false;
  }
}
