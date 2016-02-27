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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

/**
 * Distributed delete record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class ODeleteRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  private boolean           delayed          = false;
  private transient boolean lockRecord       = true;

  public ODeleteRecordTask() {
  }

  public ODeleteRecordTask(final ORecordId iRid, final int iVersion) {
    super(iRid, iVersion);
  }

  @Override
  public ORecord getRecord() {
    return null;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), null, DIRECTION.IN, "delete record %s/%s v.%d",
        database.getName(), rid.toString(), version);

    // TRY LOCKING RECORD
    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());
    if (!inTx) {
      if (lockRecord && !ddb.lockRecord(rid, nodeSource))
        throw new ODistributedRecordLockedException(rid);
    }

    try {
      final ORecord record = database.load(rid);
      if (record != null) {
        if (delayed)
          if (record.getVersion() == version)
            // POSTPONE DELETION TO BE UNDO IN CASE QUORUM IS NOT RESPECTED
            ((ODistributedStorage) database.getStorage()).pushDeletedRecord(rid, version);
          else
            throw new OConcurrentModificationException(rid, record.getVersion(), version, ORecordOperation.DELETED);
        else
          // DELETE IT RIGHT NOW
          record.delete();
      }
    } finally {
      if (!inTx)
        ddb.unlockRecord(rid);
    }

    return true;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public List<OAbstractRemoteTask> getFixTask(final ODistributedRequest iRequest, final ORemoteTask iOriginalTask, final Object iBadResponse, final Object iGoodResponse,
                                                 String executorNodeName, OHazelcastPlugin dManager) {
    final List<OAbstractRemoteTask> fixTasks = new ArrayList<OAbstractRemoteTask>(1);
    fixTasks.add(new OResurrectRecordTask(rid, version));
    return fixTasks;

  }

  @Override
  public OAbstractRemoteTask getUndoTask(final ODistributedRequest iRequest, final Object iBadResponse) {
    return new OResurrectRecordTask(rid, version);
  }

  public boolean isLockRecord() {
    return lockRecord;
  }

  @Override
  public void setLockRecord(final boolean lockRecord) {
    this.lockRecord = lockRecord;
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    super.writeData(out);
    out.writeBoolean(delayed);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    super.readData(in);
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
}
