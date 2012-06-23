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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed delete record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODeleteRecordDistributedTask extends OAbstractRecordDistributedTask<Boolean> {
  private static final long serialVersionUID = 1L;

  protected ORecordId       rid;
  protected int             version;

  public ODeleteRecordDistributedTask() {
  }

  public ODeleteRecordDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final ORecordId iRid, final int iVersion) {
    super(nodeSource, iDbName, iMode, iRid, iVersion);
  }

  public ODeleteRecordDistributedTask(final ORecordId iRid, final int iVersion) {
    super(iRid);
    version = iVersion;
  }

  @Override
  protected Boolean executeOnLocalNode(final OStorageSynchronizer dbSynchronizer) {
    return getStorage().deleteRecord(rid, version, 0, null);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
    out.writeInt(version);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    rid = new ORecordId(in.readUTF());
    version = in.readInt();
  }

  @Override
  public String getName() {
    return "record_delete";
  }

  @Override
  protected OPERATION_TYPES getOperationType() {
    return OPERATION_TYPES.RECORD_DELETE;
  }
}
