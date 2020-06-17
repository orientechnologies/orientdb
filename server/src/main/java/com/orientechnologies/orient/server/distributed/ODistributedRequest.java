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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) */
public class ODistributedRequest {
  public enum EXECUTION_MODE {
    RESPONSE,
    NO_RESPONSE
  }

  private final ODistributedServerManager manager;

  private ODistributedRequestId id;
  private String databaseName;
  private long senderThreadId;
  private ORemoteTask task;
  private ORecordId
      userRID; // KEEP ALSO THE RID TO AVOID SECURITY PROBLEM ON DELETE & RECREATE USERS

  public ODistributedRequest(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  public ODistributedRequest(
      final ODistributedServerManager manager,
      final int senderNodeId,
      final long msgSequence,
      final String databaseName,
      final ORemoteTask payload) {
    this.manager = manager;
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

  public ORecordId getUserRID() {
    return userRID;
  }

  public void setUserRID(final ORecordId iUserRID) {
    this.userRID = iUserRID;
  }

  public void toStream(final DataOutput out) throws IOException {
    id.toStream(out);
    out.writeLong(senderThreadId);
    out.writeUTF(databaseName != null ? databaseName : "");

    out.writeByte(task.getFactoryId());
    task.toStream(out);

    if (userRID != null) {
      out.writeBoolean(true);
      userRID.toStream(out);
    } else out.writeBoolean(false);
  }

  public void fromStream(final DataInput in) throws IOException {
    id = new ODistributedRequestId();
    id.fromStream(in);
    senderThreadId = in.readLong();
    databaseName = in.readUTF();
    if (databaseName.isEmpty()) databaseName = null;

    final ORemoteTaskFactory taskFactory =
        manager.getTaskFactoryManager().getFactoryByServerId(id.getNodeId());
    task = taskFactory.createTask(in.readByte());
    task.fromStream(in, taskFactory);

    if (in.readBoolean()) {
      userRID = new ORecordId();
      userRID.fromStream(in);
    }
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
