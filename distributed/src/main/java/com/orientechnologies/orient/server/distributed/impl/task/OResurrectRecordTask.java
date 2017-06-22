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
import com.orientechnologies.orient.core.exception.OPaginatedClusterException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.Arrays;
import java.util.List;

/**
 * Distributed task to fix delete record in conflict on synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OResurrectRecordTask extends OUpdateRecordTask {
  public static final  int  FACTORYID        = 11;

  public OResurrectRecordTask() {
  }

  public OResurrectRecordTask init(final ORecord record) {
    super.init(record);
    return this;
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog
        .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "Resurrecting deleted record %s/%s v.%d reqId=%s",
            database.getName(), rid.toString(), version, requestId);

    try {
      database.recycle(getRecord());

      ODistributedServerLog
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "+-> resurrected deleted record");
      return Boolean.TRUE;

    } catch (OPaginatedClusterException e) {
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "+-> no resurrection, because the record was not deleted");

      // CHECK THE RECORD CONTENT IS THE SAME
      final OStorageOperationResult<ORawBuffer> storedRecord = database.getStorage().readRecord(rid, null, true, false, null);
      return Arrays.equals(content, storedRecord.getResult().getBuffer());

    } catch (Exception e) {
      ODistributedServerLog.error(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "+-> error on resurrecting deleted record: the record is already deleted");
    }
    return Boolean.FALSE;
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public ORemoteTask getUndoTask(ODistributedServerManager dManager, ODistributedRequestId reqId, List<String> servers) {
    return null;
  }

  @Override
  public ORemoteTask getFixTask(ODistributedRequest iRequest, ORemoteTask iOriginalTask, Object iBadResponse, Object iGoodResponse,
      String executorNodeName, ODistributedServerManager dManager) {
    return null;
  }

  @Override
  public String getName() {
    return "fix_record_delete";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
