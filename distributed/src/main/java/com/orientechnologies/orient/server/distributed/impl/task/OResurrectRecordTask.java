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
import com.orientechnologies.orient.core.exception.OPaginatedClusterException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Distributed task to fix delete record in conflict on synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OResurrectRecordTask extends OUpdateRecordTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 11;

  public OResurrectRecordTask() {
  }

  public OResurrectRecordTask(final ORecord record) {
    super(record);
  }

  @Override
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "resurrecting deleted record %s/%s v.%d", database.getName(), rid.toString(), version);

    try {
      database.getStorage().recyclePosition(rid, content, version, recordType);
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "+-> resurrected deleted record");
      return Boolean.TRUE;

    } catch (OPaginatedClusterException e) {
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "+-> no resurrection, because the record was not deleted");
      return Boolean.TRUE;

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
  public ORemoteTask getUndoTask(ODistributedRequestId reqId) {
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
