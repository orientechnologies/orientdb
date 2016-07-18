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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class ODistributedRequest implements Externalizable {
  public enum EXECUTION_MODE {
    RESPONSE, NO_RESPONSE
  }

  private final ORemoteTaskFactory taskFactory;
  private ODistributedRequestId    id;
  private String                   databaseName;
  private long                     senderThreadId;
  private ORemoteTask              task;
  private ORID                     userRID;       // KEEP ALSO THE RID TO AVOID SECURITY PROBLEM ON DELETE & RECREATE USERS

  public ODistributedRequest(final ORemoteTaskFactory taskFactory) {
    this.taskFactory = taskFactory;
  }

  public ODistributedRequest(final ORemoteTaskFactory taskFactory, final int senderNodeId, final long msgSequence,
      final String databaseName, final ORemoteTask payload) {
    this.taskFactory = taskFactory;
    this.id = new ODistributedRequestId(senderNodeId, msgSequence);
    this.databaseName = databaseName;
    this.senderThreadId = Thread.currentThread().getId();
    this.task = payload;
  }

  public ODistributedRequestId getId() {
    return id;
  }

  public void setId(final ODistributedRequestId reqId) {
    id = reqId;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public ODistributedRequest setDatabaseName(final String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  public ORemoteTask getTask() {
    return task;
  }

  public ODistributedRequest setTask(final ORemoteTask payload) {
    this.task = payload;
    return this;
  }

  public ORID getUserRID() {
    return userRID;
  }

  public void setUserRID(final ORID iUserRID) {
    this.userRID = iUserRID;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(id);
    out.writeLong(senderThreadId);
    out.writeUTF(databaseName != null ? databaseName : "");
    out.writeByte(task.getFactoryId());
    task.writeExternal(out);
    out.writeObject(userRID);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    id = (ODistributedRequestId) in.readObject();
    senderThreadId = in.readLong();
    databaseName = in.readUTF();
    if (databaseName.isEmpty())
      databaseName = null;
    task = (ORemoteTask) taskFactory.createTask(in.readByte());
    task.readExternal(in);
    userRID = (ORID) in.readObject();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(256);
    buffer.append("id=");
    buffer.append(id);
    if (task != null) {
      buffer.append(" task=");
      buffer.append(task.toString());
    }
    if (userRID != null) {
      buffer.append(" user=");
      buffer.append(userRID);
    }
    return buffer.toString();
  }
}
