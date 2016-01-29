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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed updated record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OUpdateRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;

  protected byte[] previousContent;
  protected int    previousVersion;
  protected byte   recordType;
  protected byte[] content;

  private transient ORecord record;
  private transient boolean lockRecord = true;

  public class VersionPlaceholder {
    protected ORecord record;

    public VersionPlaceholder(ORecord record) {
      this.record = record;
    }

    public int getVersion() {
      return record.getVersion();
    }
  }

  public OUpdateRecordTask() {
  }

  public OUpdateRecordTask(final ORecordId iRid, final byte[] iPreviousContent, final int iPreviousVersion, final byte[] iContent,
      final int iVersion, final byte iRecordType) {
    super(iRid, iVersion);
    previousContent = iPreviousContent;
    previousVersion = iPreviousVersion;
    content = iContent;
    recordType = iRecordType;
  }

  @Override
  public ORecord getRecord() {
    if (record == null) {
      record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
      ORecordInternal.fill(record, rid, version, content, true);
    }
    return record;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "updating record %s/%s v.%d",
        database.getName(), rid.toString(), version);

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());
    if (!inTx) {
      // TRY LOCKING RECORD
      if (lockRecord && !ddb.lockRecord(rid, nodeSource))
        throw new ODistributedRecordLockedException(rid);
    }

    try {
      ORecord loadedRecord = rid.getRecord();
      if (loadedRecord == null)
        throw new ORecordNotFoundException("Record " + rid + " was not found on update");

      if (loadedRecord instanceof ODocument) {
        // APPLY CHANGES FIELD BY FIELD TO MARK DIRTY FIELDS FOR INDEXES/HOOKS
        final ODocument newDocument = (ODocument) getRecord();

        ODocument loadedDocument = (ODocument) loadedRecord;
        int loadedRecordVersion = loadedDocument.merge(newDocument, false, false).getVersion();
        if (loadedRecordVersion != version) {
          loadedDocument.setDirty();
        }
        ORecordInternal.setVersion(loadedDocument, version);
      } else
        ORecordInternal.fill(loadedRecord, rid, version, content, true);

      loadedRecord = database.save(loadedRecord);

      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "+-> updated record %s/%s v.%d",
          database.getName(), rid.toString(), loadedRecord.getVersion());

      // RETURN THE SAME OBJECT (NOT A COPY), SO AFTER COMMIT THE VERSIONS IS UPDATED AND SENT TO THE CALLER
      if (database.getTransaction().isActive()) {
        return new VersionPlaceholder(loadedRecord);
      } else {
        return loadedRecord.getVersion();
      }

    } finally {
      if (!inTx)
        ddb.unlockRecord(rid);
    }
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public List<OAbstractRemoteTask> getFixTask(final ODistributedRequest iRequest, OAbstractRemoteTask iOriginalTask,
      final Object iBadResponse, final Object iGoodResponse) {
    final int versionCopy = ORecordVersionHelper.setRollbackMode(previousVersion);

    final List<OAbstractRemoteTask> fixTasks = new ArrayList<OAbstractRemoteTask>(1);
    fixTasks.add(new OUpdateRecordTask(rid, null, -1, ((OUpdateRecordTask) iOriginalTask).content, versionCopy, recordType));
    return fixTasks;
  }

  @Override
  public OAbstractRemoteTask getUndoTask(final ODistributedRequest iRequest, final Object iBadResponse) {
    final int versionCopy = ORecordVersionHelper.setRollbackMode(previousVersion);
    return new OUpdateRecordTask(rid, null, -1, previousContent, versionCopy, recordType);
  }

  public boolean isLockRecord() {
    return lockRecord;
  }

  public void setLockRecord(final boolean lockRecord) {
    this.lockRecord = lockRecord;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeInt(content.length);
    out.write(content);
    out.write(recordType);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    final int contentSize = in.readInt();
    content = new byte[contentSize];
    in.readFully(content);
    recordType = in.readByte();
  }

  public byte[] getPreviousContent() {
    return previousContent;
  }

  public int getPreviousVersion() {
    return previousVersion;
  }

  @Override
  public String getName() {
    return "record_update";
  }

  @Override
  public String toString() {
    if (ORecordVersionHelper.isTemporary(version))
      return getName() + "(" + rid + " v." + (version - Integer.MIN_VALUE) + " realV." + version + ")";
    else
      return super.toString();
  }

  public byte[] getContent() {
    return content;
  }
}
