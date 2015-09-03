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
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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

  public ODeleteRecordTask(final ORecordId iRid, final ORecordVersion iVersion) {
    super(iRid, iVersion);
  }

  @Override
  public ORecord getRecord() {
    return null;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), null, DIRECTION.IN, "delete record %s/%s v.%s",
        database.getName(), rid.toString(), version.toString());

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
          if (record.getRecordVersion().equals(version))
            // POSTPONE DELETION TO BE UNDO IN CASE QUORUM IS NOT RESPECTED
            ((ODistributedStorage) database.getStorage()).pushDeletedRecord(rid, version);
          else
            throw new OConcurrentModificationException(rid, record.getRecordVersion(), version, ORecordOperation.DELETED);
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
  public OResurrectRecordTask getFixTask(final ODistributedRequest iRequest, OAbstractRemoteTask iOriginalTask,
      final Object iBadResponse, final Object iGoodResponse) {
    return new OResurrectRecordTask(rid, version);
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
}
