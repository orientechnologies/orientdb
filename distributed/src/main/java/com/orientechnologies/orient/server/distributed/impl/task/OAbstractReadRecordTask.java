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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract class for distributed reads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OAbstractReadRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;

  public OAbstractReadRecordTask() {
  }

  public OAbstractReadRecordTask(final ORecordId iRid) {
    rid = iRid;
  }

  @Override
  public ORecord getRecord() {
    return rid.getRecord();
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    final ORecord record = database.load(rid);
    if (record == null)
      return null;

    return new ORawBuffer(record);
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    rid.toStream(out);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    rid = new ORecordId();
    rid.fromStream(in);
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.READ;
  }

  @Override
  public String toString() {
    return getName() + "(" + rid + ")";
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }
}
