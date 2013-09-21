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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

/**
 * Asynchronous response manager
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedResponseManager {
  private final long                              messageId;
  private final long                              sentOn;
  private final ConcurrentHashMap<String, Object> responses         = new ConcurrentHashMap<String, Object>();
  private int                                     receivedResponses = 0;
  private long                                    totalTimeout;

  private static final String                     NO_RESPONSE       = "waiting-for-response";

  public ODistributedResponseManager(final long messageId, final Set<String> expectedResponses, final long iTotalTimeout) {
    this.messageId = messageId;
    this.sentOn = System.currentTimeMillis();
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

    responses.put(executorNode, response);
    receivedResponses++;

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
}
