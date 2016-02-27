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
package com.orientechnologies.orient.server.distributed.task;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

public class OReadRecordIfNotLatestTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  protected ORecordId rid;
  protected int       recordVersion;

  public OReadRecordIfNotLatestTask() {
  }

  public OReadRecordIfNotLatestTask(final ORecordId iRid, final int recordVersion) {
    rid = iRid;
    this.recordVersion = recordVersion;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    final ORecord record = database.loadIfVersionIsNotLatest(rid, recordVersion, null, true);

    if (record == null)
      return null;

    return new ORawBuffer(record);
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeUTF(rid.toString());
    out.writeInt(recordVersion);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    rid = new ORecordId(in.readUTF());
    recordVersion = in.readInt();
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.READ;
  }

  @Override
  public String getName() {
    return "record_read_if_not_latest";
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }
}
