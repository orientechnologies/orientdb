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
package com.orientechnologies.orient.server.distributed.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Execute a read of a record from a distributed node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OReadRecordTask extends OAbstractRemoteTask<ORawBuffer> {
  private static final long serialVersionUID = 1L;

  protected ORecordId       rid;

  public OReadRecordTask() {
  }

  public OReadRecordTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String iDbName, final ORecordId iRid) {
    super(iServer, iDistributedSrvMgr, iDbName, EXECUTION_MODE.SYNCHRONOUS);
    rid = iRid;
  }

  @Override
  public ORawBuffer call() throws Exception {
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
}
