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
package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.orient.server.distributed.ODistributedResponse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Hazelcast implementation of distributed peer.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedResponse implements ODistributedResponse, Externalizable {
  private long   requestId;
  private String executorNodeName;
  private String senderNodeName;
  private Object payload;

  /**
   * Constructor used by serializer.
   */
  public OHazelcastDistributedResponse() {
  }

  public OHazelcastDistributedResponse(final long iRequestId, final String executorNodeName, final String senderNodeName,
      final Serializable payload) {
    this.requestId = iRequestId;
    this.executorNodeName = executorNodeName;
    this.senderNodeName = senderNodeName;
    this.payload = payload;
  }

  @Override
  public boolean isExecutedOnLocalNode() {
    return getExecutorNodeName().equals(getSenderNodeName());
  }

  @Override
  public long getRequestId() {
    return requestId;
  }

  @Override
  public String getExecutorNodeName() {
    return executorNodeName;
  }

  @Override
  public String getSenderNodeName() {
    return senderNodeName;
  }

  public void setSenderNodeName(final String senderNodeName) {
    this.senderNodeName = senderNodeName;
  }

  public Object getPayload() {
    return payload;
  }

  public ODistributedResponse setPayload(final Object payload) {
    this.payload = payload;
    return this;
  }

  public ODistributedResponse setExecutorNodeName(final String executorNodeName) {
    this.executorNodeName = executorNodeName;
    return this;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(requestId);
    out.writeUTF(executorNodeName);
    out.writeUTF(senderNodeName);
    out.writeObject(payload);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    requestId = in.readLong();
    executorNodeName = in.readUTF();
    senderNodeName = in.readUTF();
    payload = (Serializable) in.readObject();
  }

  @Override
  public String toString() {
    if (payload == null)
      return "null";

    if (payload.getClass().isArray())
      return Arrays.toString((Object[]) payload);

    return payload.toString();
  }

}
