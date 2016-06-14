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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Distributed delete record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODeleteRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 4;

  public ODeleteRecordTask() {
  }

  public ODeleteRecordTask(final ORecord record) {
    super(record);
  }

  public ODeleteRecordTask(final ORecordId rid, final int version) {
    super(rid, version);
  }

  @Override
  public ORecord getRecord() {
    return null;
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), null, DIRECTION.IN, "Delete record %s/%s v.%d",
        database.getName(), rid.toString(), version);

    prepareUndoOperation();
    if (previousRecord == null)
      // ALREADY DELETED
      return true;

    final ORecord loadedRecord = previousRecord.copy();
    if (loadedRecord != null)
      loadedRecord.delete();

    return true;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, final ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, String executorNodeName, ODistributedServerManager dManager) {
    return new ODeleteRecordTask(rid, version);
  }

  @Override
  public ORemoteTask getUndoTask(final ODistributedRequestId reqId) {
    if (previousRecord == null)
      return null;

    final OResurrectRecordTask task = new OResurrectRecordTask(previousRecord);
    task.setLockRecords(false);
    return task;
  }

  @Override
  public String getName() {
    return "record_delete";
  }

  @Override
  public String toString() {
    return getName() + "(" + rid + ")";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
