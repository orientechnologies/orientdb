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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Distributed updated record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OUpdateRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 3;

  protected byte            recordType;
  protected byte[]          content;

  private transient ORecord record;

  public OUpdateRecordTask() {
  }

  public OUpdateRecordTask(final ORecord iRecord) {
    super((ORecordId) iRecord.getIdentity(), iRecord.getVersion() - 1);
    content = iRecord.toStream();
    recordType = ORecordInternal.getRecordType(iRecord);
  }

  public OUpdateRecordTask(final ORecord iRecord, final int version) {
    super((ORecordId) iRecord.getIdentity(), version);
    content = iRecord.toStream();
    recordType = ORecordInternal.getRecordType(iRecord);
  }

  public OUpdateRecordTask(final ORecordId iRecordId, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super(iRecordId, iVersion);
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
  public Object executeRecordTask(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "updating record %s/%s v.%d",
          database.getName(), rid.toString(), version);

    checkRecordExists();

    ORecord loadedRecord = previousRecord.copy();

    if (loadedRecord instanceof ODocument) {
      // APPLY CHANGES FIELD BY FIELD TO MARK DIRTY FIELDS FOR INDEXES/HOOKS
      final ODocument newDocument = (ODocument) getRecord();

      ODocument loadedDocument = (ODocument) loadedRecord;
      int loadedRecordVersion = loadedDocument.merge(newDocument, false, false).getVersion();
      ORecordInternal.setVersion(loadedDocument, version);
    } else
      ORecordInternal.fill(loadedRecord, rid, version, content, true);

    loadedRecord.setDirty();

    record = database.save(loadedRecord);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "+-> updated record %s/%s v.%d [%s]",
          database.getName(), rid.toString(), record.getVersion(), record);

    return record.getVersion();
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, String executorNodeName, ODistributedServerManager dManager) {

    if (iGoodResponse instanceof Integer) {
      // JUST VERSION
      final int versionCopy = ORecordVersionHelper.setRollbackMode((Integer) iGoodResponse);
      return new OUpdateRecordTask(rid, content, versionCopy, recordType);

    } else if (iGoodResponse instanceof ORecord) {
      // RECORD
      final ORecord goodRecord = (ORecord) iGoodResponse;
      final int versionCopy = ORecordVersionHelper.setRollbackMode(goodRecord.getVersion());
      return new OUpdateRecordTask(rid, goodRecord.toStream(), versionCopy, recordType);
    }

    return null;
  }

  @Override
  public ORemoteTask getUndoTask(final ODistributedRequestId reqId) {
    if (previousRecord == null)
      return null;

    final int versionCopy = ORecordVersionHelper.setRollbackMode(previousRecord.getVersion());
    final OUpdateRecordTask task = new OUpdateRecordTask(rid, previousRecord.toStream(), versionCopy, recordType);
    task.setLockRecords(false);
    return task;
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

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
