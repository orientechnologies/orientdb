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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Execute a read of a record from a distributed node.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OReadRecordTask extends OAbstractReadRecordTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 1;

  public OReadRecordTask() {
    this.lockRecords = false;
  }

  public OReadRecordTask(final ORecordId iRid) {
    super(iRid);
    this.lockRecords = false;
  }

  @Override
  public void checkRecordExists() {
  }

  @Override
  public ORecord prepareUndoOperation() {
    return null;
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    if (rid.clusterPosition < 0)
      // USED TO JUST LOCK THE CLUSTER
      return null;

    final ORecord record = database.load(rid);
    if (record == null)
      return null;

    return new ORawBuffer(record);
  }

  @Override
  public String getName() {
    return "record_read";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
