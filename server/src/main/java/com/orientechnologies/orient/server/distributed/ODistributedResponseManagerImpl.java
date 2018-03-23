/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronous response manager. All the public methods have to pay attention on synchronizing access by using
 * synchronousResponsesLock lock.
 * <p>
 * TODO: - set flags during collecting of response for fast computation on checking for the status
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedResponseManagerImpl implements ODistributedResponseManager {
  public static final  int    ADDITIONAL_TIMEOUT_CLUSTER_SHAPE = 10000;
  private static final String NO_RESPONSE                      = "waiting-for-response";
  private final ODistributedServerManager dManager;
  private final ODistributedRequest       request;
  private final long                      sentOn;
  private final Set<String>               nodesConcurInQuorum;
  private final HashMap<String, Object> responses = new HashMap<String, Object>();
  private final boolean groupResponsesByResult;
  private final List<List<ODistributedResponse>> responseGroups = new ArrayList<List<ODistributedResponse>>();
  private final OCallable<Void, ODistributedResponseManager> endCallback;
  private       int                                          totalExpectedResponses;
  private final long                                         synchTimeout;
  private final long                                         totalTimeout;
  private final Lock           synchronousResponsesLock    = new ReentrantLock();
  private final CountDownLatch synchronousResponsesArrived = new CountDownLatch(1);
  private final int                  quorum;
  private final boolean              waitForLocalNode;
  private       ODistributedResponse localResponse;
  private volatile int receivedResponses = 0;
  private volatile boolean receivedCurrentNode;
  private       ODistributedResponse quorumResponse  = null;
  private final Set<String>          followupToNodes = new HashSet<String>();
  private       AtomicBoolean        canceled        = new AtomicBoolean(false);

  public ODistributedResponseManagerImpl(final ODistributedServerManager iManager, final ODistributedRequest iRequest,
      final Collection<String> expectedResponses, final Set<String> iNodesConcurInQuorum, final int iTotalExpectedResponses,
      final int iQuorum, final boolean iWaitForLocalNode, final long iSynchTimeout, final long iTotalTimeout,
      final boolean iGroupResponsesByResult, final OCallable<Void, ODistributedResponseManager> endCallback) {
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
    this.endCallback = endCallback;

    for (String node : expectedResponses)
      responses.put(node, NO_RESPONSE);

    if (groupResponsesByResult)
      responseGroups.add(new ArrayList<ODistributedResponse>());
  }

  /**
   * Not synchronized, it's called when a message arrives
   *
   * @param response Received response to collect
   *
   * @return True if all the nodes responded, otherwise false
   */
  public boolean collectResponse(final ODistributedResponse response) {
    final String executorNode = response.getExecutorNodeName();
    final String senderNode = response.getSenderNodeName();
    response.setDistributedResponseManager(this);

    synchronousResponsesLock.lock();
    try {
      if (!executorNode.equals(dManager.getLocalNodeName()) && !responses.containsKey(executorNode)) {
        ODistributedServerLog.warn(this, senderNode, executorNode, DIRECTION.IN,
            "Received response for request (%s) from unexpected node. Expected are: %s", request, getExpectedNodes());

        Orient.instance().getProfiler()
            .updateCounter("distributed.node.unexpectedNodeResponse", "Number of responses from unexpected nodes", +1);

        return false;
      }

      dManager.getMessageService().updateLatency(executorNode, sentOn);

      responses.put(executorNode, response);
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
              if (rgPayload instanceof ODocument && responsePayload instanceof ODocument && !((ODocument) rgPayload).getIdentity()
                  .isValid() && ((ODocument) rgPayload).hasSameContentOf((ODocument) responsePayload))
                // SAME RESULT
                foundBucket = true;
              else if (rgPayload.equals(responsePayload))
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

      // FOR EVERY RESPONSE COLLECTED, COMPUTE THE FINAL QUORUM RESPONSE IF POSSIBLE
      computeQuorumResponse(false);

      return checkForCompletion();

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  private boolean checkForCompletion() {
    final boolean completed = getExpectedResponses() == receivedResponses;

    if (completed)
      // FINALIZE THE REQUEST
      end();

    if (completed || isMinimumQuorumReached(false)) {
      // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "Sending signal of synchronous request completed (reqId=%s thread=%d)", request.getId(), Thread.currentThread().getId());

      synchronousResponsesArrived.countDown();
    }
    return completed;
  }

  public ODistributedRequestId getMessageId() {
    return request.getId();
  }

  public long getSentOn() {
    return sentOn;
  }

  /**
   * @param localNodeName
   * @param localResult
   *
   * @return
   */
  public boolean setLocalResult(final String localNodeName, final Object localResult) {
    localResponse = new ODistributedResponse(this, request.getId(), localNodeName, localNodeName, localResult);
    receivedCurrentNode = true;
    return collectResponse(localResponse);
  }

  public boolean isSynchronousWaiting() {
    return synchronousResponsesArrived.getCount() > 0;
  }

  public void removeServerBecauseUnreachable(final String node) {
    synchronousResponsesLock.lock();
    try {

      if (responses.remove(node) != null) {
        totalExpectedResponses--;
        nodesConcurInQuorum.remove(node);

        checkForCompletion();
      }

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public int getQuorum() {
    return quorum;
  }

  /**
   * Waits until the minimum responses are collected or timeout occurs. If "waitForLocalNode" wait also for local node.
   *
   * @return True if the received responses are major or equals then the expected synchronous responses, otherwise false
   *
   * @throws InterruptedException
   */
  public boolean waitForSynchronousResponses() throws InterruptedException {
    final long beginTime = System.currentTimeMillis();
    try {

      boolean reachedTimeout = false;
      long currentTimeout = synchTimeout;
      while (currentTimeout > 0) {

        if (currentTimeout > 10000)
          // CUT THE TIMEOUT IN BLOCKS OF 10S EACH TO ALLOW CHECKING FOR ANY SERVER IF UNREACHABLE
          currentTimeout = 10000;

        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "Waiting max %dms for collecting all synchronous responses... (timeout=%d reqId=%s thread=%d)", currentTimeout,
              synchTimeout, request.getId(), Thread.currentThread().getId());

        // WAIT FOR THE RESPONSES
        if (synchronousResponsesArrived.await(currentTimeout, TimeUnit.MILLISECONDS)) {
          if (canceled.get())
            throw new ODistributedOperationException("Request has been canceled");

          // COMPLETED
          if (ODistributedServerLog.isDebugEnabled())
            ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "All synchronous responses collected in %dms (reqId=%s thread=%d)", (System.currentTimeMillis() - beginTime),
                request.getId(), Thread.currentThread().getId());

          return true;
        }

        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
              "All synchronous responses not collected in %dms, waiting again... (reqId=%s thread=%d)",
              (System.currentTimeMillis() - beginTime), request.getId(), Thread.currentThread().getId());

        if (Thread.currentThread().isInterrupted()) {
          // INTERRUPTED
          ODistributedServerLog
              .warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, "Thread has been interrupted wait for request (%s)",
                  request);
          Thread.currentThread().interrupt();
          break;
        }

        synchronousResponsesLock.lock();
        try {

          final long now = System.currentTimeMillis();
          final long elapsed = now - beginTime;
          if (elapsed > synchTimeout)
            reachedTimeout = true;
          currentTimeout = synchTimeout - elapsed;

          // CHECK IF ANY NODE ARE UNREACHABLE IN THE MEANWHILE
          int synchronizingNodes = 0;
          int missingActiveNodes = 0;

          Map<String, ODistributedServerManager.DB_STATUS> missingResponseNodeStatuses = new HashMap<String, ODistributedServerManager.DB_STATUS>(
              responses.size());

          int missingResponses = 0;

          for (Iterator<Map.Entry<String, Object>> iter = responses.entrySet().iterator(); iter.hasNext(); ) {
            final Map.Entry<String, Object> curr = iter.next();

            if (curr.getValue() == NO_RESPONSE) {
              missingResponses++;

              // ANALYZE THE NODE WITHOUT A RESPONSE
              final ODistributedServerManager.DB_STATUS dbStatus = dManager.getDatabaseStatus(curr.getKey(), getDatabaseName());

              missingResponseNodeStatuses.put(curr.getKey(), dbStatus);

              switch (dbStatus) {
              case BACKUP:
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

          if (missingResponses == 0) {
            // ALL RESPONSE COLLECTED, BUT NO QUORUM REACHED
            ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "All responses collected %s, but no quorum reached (reqId=%s)", responses, request.getId());
            break;
          }

          request.getTask().checkIsValid(dManager);

          if (missingActiveNodes == 0) {
            // NO MORE ACTIVE NODES TO WAIT
            ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "No more active nodes to wait for request (%s): anticipate timeout (saved %d ms). Missing servers: %s", request,
                currentTimeout, missingResponseNodeStatuses);
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
            // currentTimeout = synchTimeout;
            // ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
            // "%d nodes are in synchronization mode during request (%s): enlarge timeout +%dms, wait again for %dms",
            // synchronizingNodes, request, synchTimeout, currentTimeout);
          }

        } finally {
          synchronousResponsesLock.unlock();
        }
      }

      if (canceled.get())
        throw new ODistributedOperationException("Request has been canceled");

      return isMinimumQuorumReached(reachedTimeout);

    } finally {
      Orient.instance().getProfiler()
          .stopChrono("distributed.synchResponses", "Time to collect all the synchronous responses from distributed nodes",
              beginTime);
    }
  }

  public boolean executeInLock(final OCallable<Boolean, ODistributedResponseManager> callback) {
    synchronousResponsesLock.lock();
    try {
      return callback.call(this);
    } finally {
      synchronousResponsesLock.unlock();
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
        return new ODistributedResponse(this, request.getId(), dManager.getLocalNodeName(), dManager.getLocalNodeName(), failure);

      if (receivedResponses == 0) {
        if (quorum > 0 && !request.getTask().isIdempotent())
          throw new ODistributedException(
              "No response received from any of nodes " + getExpectedNodes() + " for request " + request + " after " + (
                  (System.nanoTime() - sentOn) / 1000000) + "ms");

        // NO QUORUM, RETURN NULL
        return null;
      }

      // MANAGE THE RESULT BASED ON RESULT STRATEGY
      switch (request.getTask().getResultStrategy()) {
      case UNION: {
        // COLLECT ALL THE RESPONSE IN A MAP OF <NODE, RESULT>
        final Map<String, Object> payloads = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : responses.entrySet())
          if (entry.getValue() != NO_RESPONSE)
            payloads.put(entry.getKey(), ((ODistributedResponse) entry.getValue()).getPayload());

        if (payloads.isEmpty())
          return null;

        final ODistributedResponse response = getReceivedResponses().iterator().next();
        response.setExecutorNodeName(responses.keySet().toString());
        response.setPayload(payloads);
        return response;
      }

      default:
        // DEFAULT: RETURN BEST ANSWER
        return quorumResponse;
      }

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

      end();

      // NOTIFY TO THE WAITER THE RESPONSE IS COMPLETE NOW
      synchronousResponsesArrived.countDown();

    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public boolean isCanceled() {
    return canceled.get();
  }

  public void cancel() {
    this.canceled.set(true);

    // UNLOCK WAITER
    synchronousResponsesArrived.countDown();
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

  /**
   * Returns all the servers in conflict.
   */
  public List<String> getConflictServers() {
    synchronousResponsesLock.lock();
    try {

      final List<String> servers = new ArrayList<String>();
      if (quorumResponse != null) {
        for (Map.Entry<String, Object> entry : responses.entrySet()) {
          if (!quorumResponse.equals(entry.getValue()))
            servers.add(entry.getKey());
        }
      }
      return servers;

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

  public boolean addFollowupToServer(final String server) {
    synchronousResponsesLock.lock();
    try {
      return followupToNodes.add(server);
    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public Set<String> getServersWithoutFollowup() {
    if (!groupResponsesByResult)
      return Collections.EMPTY_SET;

    synchronousResponsesLock.lock();
    try {
      final HashSet<String> servers = new HashSet<String>(responses.keySet());
      servers.removeAll(followupToNodes);
      return servers;
    } finally {
      synchronousResponsesLock.unlock();
    }
  }

  public ODistributedResponse getQuorumResponse() {
    return quorumResponse;
  }

  public Object getResponseFromServer(final String s) {
    synchronousResponsesLock.lock();
    try {
      final Object r = responses.get(s);
      if (r == NO_RESPONSE)
        return null;
      return r;
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

  public ODistributedRequest getRequest() {
    return request;
  }

  /**
   * Returns all the responses in conflict.
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

  public synchronized boolean isFinished() {
    return getExpectedResponses() - receivedResponses == 0;
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

  protected boolean isMinimumQuorumReached(final boolean reachedTimeout) {
    if (isWaitForLocalNode() && !isReceivedCurrentNode())
      return false;

    if (quorum == 0)
      return true;

    return computeQuorumResponse(reachedTimeout);
  }

  /**
   * Computes the quorum response if possible by returning true and setting the field quorumResponse with the ODistributedResponse.
   */
  private boolean computeQuorumResponse(boolean reachedTimeout) {
    if (quorumResponse != null)
      // ALREADY COMPUTED
      return true;

    if (groupResponsesByResult) {
      for (List<ODistributedResponse> group : responseGroups) {
        if (group.size() >= quorum) {
          int responsesForQuorum = 0;
          for (ODistributedResponse r : group) {
            if (nodesConcurInQuorum.contains(r.getExecutorNodeName())) {
              final Object payload = r.getPayload();

              if (payload instanceof Throwable) {
                if (payload instanceof ODistributedRecordLockedException)
                  // JUST ONE ODistributedRecordLockedException IS ENOUGH TO FAIL THE OPERATION BECAUSE RESOURCES CANNOT BE LOCKED
                  break;
                if (payload instanceof OConcurrentCreateException)
                  // JUST ONE OConcurrentCreateException IS ENOUGH TO FAIL THE OPERATION BECAUSE RID ARE DIFFERENT
                  break;
              } else if (++responsesForQuorum >= quorum) {
                // QUORUM REACHED
                setQuorumResponse(r);
                return true;
              }
            }
          }
        }
      }

    } else {
      if (receivedResponses >= quorum) {
        int responsesForQuorum = 0;
        for (Map.Entry<String, Object> response : responses.entrySet()) {
          if (response.getValue() != NO_RESPONSE && nodesConcurInQuorum.contains(response.getKey())
              && ++responsesForQuorum >= quorum) {
            // QUORUM REACHED
            ODistributedResponse resp = (ODistributedResponse) response.getValue();
            if (resp != null && !(resp.getPayload() instanceof Throwable))
              setQuorumResponse(resp);
            return true;
          }
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

    if (isMinimumQuorumReached(true)) {
      // QUORUM SATISFIED
      if (responseGroups.size() == 1)
        // NO CONFLICT
        return null;

      if (checkNoWinnerCase(bestResponsesGroup))
        // TODO: CALL THE RECORD CONFLICT PIPELINE
        return null;

      if (fixNodesInConflict(bestResponsesGroup, conflicts, false))
        // FIX SUCCEED
        return null;
    }

    // QUORUM HASN'T BEEN REACHED
    if (ODistributedServerLog.isDebugEnabled()) {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
          "Detected %d node(s) in timeout or in conflict and quorum (%d) has not been reached, rolling back changes for request (%s)",
          conflicts, quorum, request);

      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, composeConflictMessage());
    }

    if (!undoRequest()) {
      // SKIP UNDO
      return null;
    }

    // CHECK IF THERE IS AT LEAST ONE ODistributedRecordLockedException or OConcurrentCreateException
    for (Object r : responses.values()) {
      if (r instanceof ODistributedRecordLockedException)
        throw (ODistributedRecordLockedException) r;
      if (r instanceof OConcurrentCreateException)
        throw (OConcurrentCreateException) r;
    }

    final Object goodResponsePayload = bestResponsesGroup.isEmpty() ? null : bestResponsesGroup.get(0).getPayload();
    if (goodResponsePayload instanceof RuntimeException)
      // RESPONSE IS ALREADY AN EXCEPTION: THROW THIS
      return (RuntimeException) goodResponsePayload;
    else if (goodResponsePayload instanceof Throwable)
      return OException
          .wrapException(new ODistributedOperationException(composeConflictMessage()), (Throwable) goodResponsePayload);
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
    msg.append("Quorum " + getQuorum() + " not reached for request (" + request + "). Elapsed=" + ((System.nanoTime() - getSentOn())
        / 1000000) + "ms.");
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

    if (responses.isEmpty()) {
      msg.append("Received no responses");
    } else {
      msg.append("Received: ");
      for (Map.Entry<String, Object> response : responses.entrySet()) {
        msg.append("\n - ");
        msg.append(response.getKey());
        msg.append(": ");
        msg.append(response.getValue());
      }
    }

    return msg.toString();
  }

  protected boolean undoRequest() {
    final ORemoteTask task = request.getTask();

    if (task.isIdempotent()) {
      // NO UNDO IS NECESSARY
      ODistributedServerLog
          .warn(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, "No undo because the task (%s) is idempotent", task);
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
        final List<String> servers = OMultiValue.getSingletonList(targetNode);
        final ORemoteTask undoTask = ((OAbstractReplicatedTask) task).getUndoTask(dManager, request.getId(), servers);

        if (undoTask != null) {
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), targetNode, DIRECTION.OUT,
              "Sending undo message (%s) for request (%s) database '%s' to server %s", undoTask, request, request.getDatabaseName(),
              targetNode);

          final ODistributedResponse result = dManager
              .sendRequest(request.getDatabaseName(), null, servers, undoTask, dManager.getNextMessageIdCounter(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), targetNode, DIRECTION.OUT,
              "Received response from undo message (%s) for request (%s) to server %s: %s", undoTask, request, targetNode, result);
        }
      }
    }
    return true;
  }

  protected boolean fixNodesInConflict(final List<ODistributedResponse> bestResponsesGroup, final int conflicts,
      final boolean cannotFixRecordLockException) {
    // NO FIFTY/FIFTY CASE: FIX THE CONFLICTED NODES BY OVERWRITING THE RECORD WITH THE WINNER'S RESULT
    final ODistributedResponse goodResponse = bestResponsesGroup.get(0);

    if (goodResponse.getPayload() instanceof Throwable)
      return false;

    ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
        "Detected %d conflicts, but the quorum (%d) has been reached. Checking responses and in case fixing remote records (reqId=%s)",
        conflicts, quorum, request.getId());

    for (List<ODistributedResponse> responseGroup : responseGroups) {
      if (responseGroup != bestResponsesGroup) {
        // CONFLICT GROUP: FIX THEM ONE BY ONE
        for (ODistributedResponse r : responseGroup) {
          if (cannotFixRecordLockException && r.getPayload() instanceof ODistributedRecordLockedException) {
            // NO FIX, THE RECORD WAS LOCKED
            if (ODistributedServerLog.isDebugEnabled())
              ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                  "Node '%s' responded with a record lock exception: cannot fix the operation, undo the entire operation (reqId=%s)",
                  r.getExecutorNodeName(), request);
            return false;
          }

          final ORemoteTask fixTask = ((OAbstractReplicatedTask) request.getTask())
              .getFixTask(request, request.getTask(), r.getPayload(), goodResponse.getPayload(), r.getExecutorNodeName(), dManager);

          if (fixTask == null) {
            // FIX NOT AVAILABLE: UNDO THE ENTIRE OPERATION
            ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
                "No fix operation available: cannot fix the operation, undo the entire operation (reqId=%s)", request);
            return false;
          }

          executeFix(r.getExecutorNodeName(), fixTask, r.getPayload(), goodResponse);
        }
      }
    }
    return true;
  }

  private void executeFix(final String server, final ORemoteTask fixTask, final Object r, final Object goodResponse) {
    if (server.equals(dManager.getLocalNodeName())) {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), server, DIRECTION.OUT,
          "Executing the fix locally (%s) for response (%s) on request (%s) to be: %s", fixTask, r, request, goodResponse);

      ODatabaseDocumentInternal oldDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
      final ODatabaseDocumentInternal database = dManager.getMessageService().getDatabase(getDatabaseName()).getDatabaseInstance();
      try {
        database.activateOnCurrentThread();
        dManager
            .executeOnLocalNode(new ODistributedRequestId(dManager.getLocalNodeId(), dManager.getNextMessageIdCounter()), fixTask,
                database);
      } finally {
        database.close();
        ODatabaseRecordThreadLocal.instance().set(oldDb);
      }
    } else {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), server, DIRECTION.OUT,
          "Sending fix message (%s) for response (%s) on request (%s) to be: %s", fixTask, r, request, goodResponse);

      dManager.sendRequest(request.getDatabaseName(), null, OMultiValue.getSingletonList(server), fixTask,
          dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null, null);
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

        ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, DIRECTION.NONE,
            "Detected possible split brain network where 2 groups of servers A%s and B%s have different contents. Cannot decide who is the winner even if the quorum (%d) has been reached. Request (%s) responses:%s",
            a, b, quorum, request, details);

        // DON'T FIX RECORDS BECAUSE THERE ISN'T A CLEAR WINNER
        return true;
      }
    }
    return false;
  }

  private void setQuorumResponse(final ODistributedResponse quorumResponse) {
    this.quorumResponse = quorumResponse;
    ODistributedServerLog
        .debug(this, dManager.getLocalNodeName(), null, DIRECTION.NONE, "Reached the quorum (%d) for value '%s' (reqId=%s)", quorum,
            quorumResponse, request);
  }

  private void end() {
    if (endCallback != null)
      // CUSTOM CALLBACK
      endCallback.call(this);
    else {
      // DEFAULT CALLBACK
      if (groupResponsesByResult) {
        final String localNodeName = dManager.getLocalNodeName();

        final Set<String> serversToFollowup = getServersWithoutFollowup();
        serversToFollowup.remove(localNodeName);

        if (!serversToFollowup.isEmpty()) {
          ODistributedServerLog.debug(this, localNodeName, serversToFollowup.toString(), ODistributedServerLog.DIRECTION.OUT,
              "Distributed response (reqId=%s quorum=%d result=%s), checking for any fix needed...", request.getId(), quorum,
              quorumResponse);

          for (String s : serversToFollowup) {
            Object response = responses.get(s);
            if (response == NO_RESPONSE)
              response = null;

            if (quorumResponse != null && !quorumResponse.equals(response)) {
              // SEND FIX
              final Object payload = response instanceof ODistributedResponse ?
                  ((ODistributedResponse) response).getPayload() :
                  null;

              final ORemoteTask fixTask = ((OAbstractReplicatedTask) request.getTask())
                  .getFixTask(request, request.getTask(), payload, quorumResponse.getPayload(), s, dManager);

              if (fixTask == null) {
                // FIX NOT AVAILABLE: UNDO THE ENTIRE OPERATION
                ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
                    "No fix operation available: cannot fix the operation, undo the entire operation (reqId=%s)", request);
                continue;
              }

              executeFix(s, fixTask, response, quorumResponse.getPayload());
            }
          }
        }
      }
    }
  }
}
