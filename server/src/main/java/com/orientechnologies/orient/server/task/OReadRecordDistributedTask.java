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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed read record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OReadRecordDistributedTask extends OAbstractRecordDistributedTask<ORawBuffer> {
  private static final long serialVersionUID = 1L;

  public OReadRecordDistributedTask() {
    executionType = EXEC_TYPE.LOCAL_ONLY;
  }

  public OReadRecordDistributedTask(final String nodeSource, final String iDbName, final ORecordId iRid) {
    super(nodeSource, iDbName, EXECUTION_MODE.SYNCHRONOUS, iRid, OVersionFactory.instance().createUntrackedVersion());
    executionType = EXEC_TYPE.LOCAL_ONLY;
  }

  public OReadRecordDistributedTask(final long iRunId, final long iOperationId, final ORecordId iRid) {
    super(iRunId, iOperationId, iRid, OVersionFactory.instance().createUntrackedVersion());
    executionType = EXEC_TYPE.LOCAL_ONLY;
  }

  @Override
  protected ORawBuffer executeOnLocalNode(final OStorageSynchronizer dbSynchronizer) {
    final ODatabaseDocumentTx database = openDatabase();
    try {
      final ORecordInternal<?> record = database.load(rid);
      if (record == null)
        return null;

      return new ORawBuffer(record);

    } finally {
      closeDatabase(database);
    }
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    rid = new ORecordId(in.readUTF());
  }

  @Override
  public String getName() {
    return "record_read";
  }

  @Override
  protected OPERATION_TYPES getOperationType() {
    return null;
  }
}
