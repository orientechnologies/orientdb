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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OReadRecordIfNotLatestTask extends OAbstractReadRecordTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 2;

  protected int             recordVersion;

  public OReadRecordIfNotLatestTask() {
    this.lockRecords = false;
  }

  public OReadRecordIfNotLatestTask(final ORecordId iRid, final int recordVersion) {
    super(iRid);
    this.recordVersion = recordVersion;
    this.lockRecords = false;
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    final ORecord record = database.loadIfVersionIsNotLatest(rid, recordVersion, null, true);

    if (record == null)
      return null;

    return new ORawBuffer(record);
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    super.toStream(out);
    out.writeInt(recordVersion);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    recordVersion = in.readInt();
  }

  @Override
  public String getName() {
    return "record_read_if_not_latest";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
