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
package com.orientechnologies.orient.server.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed updated record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OUpdateRecordDistributedTask extends OAbstractRecordDistributedTask<Integer> {
  private static final long serialVersionUID = 1L;

  protected byte[]          content;
  protected byte            recordType;

  public OUpdateRecordDistributedTask() {
  }

  public OUpdateRecordDistributedTask(final String iNodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super(iNodeSource, iDbName, iMode, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
    OLogManager.instance().warn(this, "DISTRIBUTED -> route UPDATE RECORD in %s mode to %s %s{%s} v.%d", iMode, nodeSource,
        iDbName, iRid, iVersion);
  }

  public OUpdateRecordDistributedTask(final long iRunId, final long iOperationId, final ORecordId iRid, final byte[] iContent,
      final int iVersion, final byte iRecordType) {
    super(iRunId, iOperationId, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  @Override
  public Integer executeOnLocalNode(final OStorageSynchronizer dbSynchronizer) {
    OLogManager.instance().warn(this, "DISTRIBUTED <- UPDATE RECORD db %s %s{%s} v.%d", nodeSource, databaseName, rid, version);
    return getStorage().updateRecord(rid, content, version, recordType, 0, null);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
    out.writeInt(version);
    out.write(recordType);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    version = in.readInt();
    recordType = in.readByte();
  }

  @Override
  public String getName() {
    return "record_update";
  }

  @Override
  protected OPERATION_TYPES getOperationType() {
    return OPERATION_TYPES.RECORD_UPDATE;
  }
}
