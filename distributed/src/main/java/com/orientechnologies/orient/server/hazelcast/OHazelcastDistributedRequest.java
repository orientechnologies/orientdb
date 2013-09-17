/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.hazelcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import com.orientechnologies.orient.server.distributed.ODistributedRequest;

/**
 * Hazelcast implementation of distributed peer.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedRequest implements ODistributedRequest, Externalizable {
  private String       senderNodeName;
  private final long[] id = new long[2];
  private String       databaseName;
  private String       clusterName;
  private long         threadId;
  private Serializable payload;

  /**
   * Constructor used by serializer.
   */
  public OHazelcastDistributedRequest() {
  }

  public OHazelcastDistributedRequest(final String senderNodeName, final long iRunId, final long iOperationId,
      final String databaseName, final String clusterName, final Serializable payload) {
    this.senderNodeName = senderNodeName;
    this.id[0] = iRunId;
    this.id[1] = iOperationId;
    this.databaseName = databaseName;
    this.clusterName = clusterName;
    this.threadId = Thread.currentThread().getId();
    this.payload = payload;
  }

  @Override
  public Object getId() {
    return id;
  }

  @Override
  public OHazelcastDistributedRequest setId(final Object id) {
    this.id[0] = ((long[]) id)[0];
    this.id[1] = ((long[]) id)[1];
    return this;
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String getClusterName() {
    return clusterName;
  }

  @Override
  public long getSenderThreadId() {
    return threadId;
  }

  @Override
  public Serializable getPayload() {
    return payload;
  }

  @Override
  public OHazelcastDistributedRequest setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  @Override
  public OHazelcastDistributedRequest setClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  @Override
  public OHazelcastDistributedRequest setSenderThreadId(long threadId) {
    this.threadId = threadId;
    return this;
  }

  @Override
  public OHazelcastDistributedRequest setPayload(Serializable payload) {
    this.payload = payload;
    return this;
  }

  public String getSenderNodeName() {
    return senderNodeName;
  }

  public OHazelcastDistributedRequest setSenderNodeName(String senderNodeName) {
    this.senderNodeName = senderNodeName;
    return this;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(id[0]);
    out.writeLong(id[1]);
    out.writeUTF(databaseName);
    out.writeUTF(clusterName);
    out.writeLong(threadId);
    out.writeObject(payload);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    id[0] = in.readLong();
    id[1] = in.readLong();
    databaseName = in.readUTF();
    clusterName = in.readUTF();
    threadId = in.readLong();
    payload = (Serializable) in.readObject();
  }

  @Override
  public String toString() {
    return payload != null ? payload.toString() : null;
  }
}
