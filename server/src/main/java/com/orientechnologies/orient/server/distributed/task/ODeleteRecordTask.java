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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Distributed delete record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODeleteRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 4;
  private boolean           delayed          = false;

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
      final ODatabaseDocumentTx database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), null, DIRECTION.IN, "Delete record %s/%s v.%d",
        database.getName(), rid.toString(), version);

    prepareUndoOperation();

    final ORecord loadedRecord = previousRecord.copy();
    if (loadedRecord != null) {
      if (delayed)
        if (loadedRecord.getVersion() == version)
          // POSTPONE DELETION TO BE UNDO IN CASE QUORUM IS NOT RESPECTED
          ((ODistributedStorage) database.getStorage()).pushDeletedRecord(rid, version);
        else
          throw new OConcurrentModificationException(rid, loadedRecord.getVersion(), version, ORecordOperation.DELETED);
      else
        // DELETE IT RIGHT NOW
        loadedRecord.delete();
    }

    return true;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, final ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, String executorNodeName, ODistributedServerManager dManager) {
    return new OResurrectRecordTask(rid, version);
  }

  @Override
  public ORemoteTask getUndoTask(ODistributedRequestId reqId) {
    return new OResurrectRecordTask(rid, version);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeBoolean(delayed);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    delayed = in.readBoolean();
  }

  @Override
  public String getName() {
    return "record_delete";
  }

  public ODeleteRecordTask setDelayed(final boolean delayed) {
    this.delayed = delayed;
    return this;
  }

  @Override
  public String toString() {
    return getName() + "(" + rid + " delayed=" + delayed + ")";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
