/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.serialization.OStreamableHelper;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class ODistributedResponse {
  private ODistributedResponseManager distributedResponseManager;
  private ODistributedRequestId requestId;
  private ONodeId executorNodeName;
  private ONodeId senderNodeName;
  private Object payload;

  /** Constructor used by serializer. */
  public ODistributedResponse() {}

  public ODistributedResponse(
      final ODistributedResponseManager msg,
      final ODistributedRequestId iRequestId,
      final ONodeId executorNodeName,
      final ONodeId senderNodeName,
      final Object payload) {
    this.distributedResponseManager = msg;
    this.requestId = iRequestId;
    this.executorNodeName = executorNodeName;
    this.senderNodeName = senderNodeName;
    this.payload = payload;
  }

  public ODistributedRequestId getRequestId() {
    return requestId;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ODistributedResponse && ((ODistributedResponse) obj).payload != null)
      return ((ODistributedResponse) obj).payload.equals(payload);

    return false;
  }

  public ONodeId getExecutorNodeName() {
    return executorNodeName;
  }

  public ONodeId getSenderNodeName() {
    return senderNodeName;
  }

  public Object getPayload() {
    return payload;
  }

  public ODistributedResponse setPayload(final Object payload) {
    this.payload = payload;
    return this;
  }

  public ODistributedResponse setExecutorNodeName(final ONodeId executorNodeName) {
    this.executorNodeName = executorNodeName;
    return this;
  }

  public void toStream(final DataOutput out) throws IOException {
    requestId.toStream(out);
    executorNodeName.writeNetwork(out);
    senderNodeName.writeNetwork(out);
    OStreamableHelper.toStream(out, payload);
  }

  public void fromStream(final DataInput in) throws IOException {
    requestId = new ODistributedRequestId();
    requestId.fromStream(in);
    executorNodeName = ONodeId.readNetwork(in);
    senderNodeName = ONodeId.readNetwork(in);
    payload = OStreamableHelper.fromStream(in);
  }

  public ODistributedResponseManager getDistributedResponseManager() {
    return distributedResponseManager;
  }

  @Override
  public String toString() {
    if (payload == null) return "null";

    if (payload.getClass().isArray()) return Arrays.toString((Object[]) payload);

    return payload.toString();
  }

  public void setDistributedResponseManager(
      final ODistributedResponseManager distributedResponseManager) {
    this.distributedResponseManager = distributedResponseManager;
  }
}
