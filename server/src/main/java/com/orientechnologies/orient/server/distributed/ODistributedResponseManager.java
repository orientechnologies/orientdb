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
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask.RESULT_STRATEGY;

/**
 * Asynchronous response manager
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedResponseManager {
  private long                                    messageId;
  private final long                              sentOn;
  private ODistributedResponse                    firstResponse;
  private final ConcurrentHashMap<String, Object> responses         = new ConcurrentHashMap<String, Object>();
  private final int                               expectedSynchronousResponses;
  private int                                     receivedResponses = 0;
  private int                                     quorum;
  private boolean                                 waitForLocalNode;
  private final long                              totalTimeout;
  private boolean                                 receivedCurrentNode;

  private static final String                     NO_RESPONSE       = "waiting-for-response";

  public ODistributedResponseManager(final long iMessageId, final Set<String> expectedResponses,
      final int iExpectedSynchronousResponses, final int iQuorum, final boolean iWaitForLocalNode, final long iTotalTimeout) {
    this.messageId = iMessageId;
    this.sentOn = System.currentTimeMillis();
    this.expectedSynchronousResponses = iExpectedSynchronousResponses;
    this.quorum = iQuorum;
    this.waitForLocalNode = iWaitForLocalNode;
    this.totalTimeout = iTotalTimeout;

    for (String node : expectedResponses)
      responses.put(node, NO_RESPONSE);
  }

  public boolean addResponse(final ODistributedResponse response) {
    final String executorNode = response.getExecutorNodeName();

    if (!responses.containsKey(executorNode)) {
      ODistributedServerLog.warn(this, response.getSenderNodeName(), executorNode, DIRECTION.IN,
          "received response for message %d from unexpected node. Expected are: %s", messageId, getExpectedNodes());

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

    if (firstResponse == null)
      firstResponse = response;
    responses.put(executorNode, response);
    receivedResponses++;

    if (waitForLocalNode && response.isExecutedOnLocalNode())
      receivedCurrentNode = true;

    // TODO: CHECK FOR CONFLICTS

    return getExpectedResponses() == receivedResponses;
  }

  public long getMessageId() {
    return messageId;
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

  public int getReceivedResponses() {
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

  public void setMessageId(long messageId) {
    this.messageId = messageId;
  }

  public int getExpectedSynchronousResponses() {
    return expectedSynchronousResponses;
  }

  public int getQuorum() {
    return quorum;
  }

  public boolean waitForSynchronousResponses() {
    return (waitForLocalNode && !receivedCurrentNode) || receivedResponses < expectedSynchronousResponses;
  }

  public boolean isWaitForLocalNode() {
    return waitForLocalNode;
  }

  public boolean isReceivedCurrentNode() {
    return receivedCurrentNode;
  }

  public ODistributedResponse getResponse(final RESULT_STRATEGY resultStrategy) {
    switch (resultStrategy) {
    case FIRST_RESPONSE:
      return firstResponse;

    case MERGE:
      // return merge( m new OHazelcastDistributedResponse(firstResponse.getRequestId(), null, firstResponse.getSenderNodeName(),
      // firstResponse.getSenderThreadId(), null));
      return firstResponse;

    case UNION:
      final Map<String, Object> payloads = new HashMap<String, Object>();
      for (Map.Entry<String, Object> entry : responses.entrySet())
        if (entry.getValue() != NO_RESPONSE)
          payloads.put(entry.getKey(), ((ODistributedResponse) entry.getValue()).getPayload());
      firstResponse.setPayload(payloads);
      return firstResponse;
    }

    return firstResponse;
  }
}
