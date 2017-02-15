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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Distributed updated record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 *
 */
public class OUpdateRecordTask extends OAbstractRecordReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 3;

  protected byte            recordType;
  protected byte[]          content;

  private transient ORecord record;
  private byte[]            previousRecordContent;

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
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "Updating record %s/%s v.%d reqId=%s...", database.getName(), rid.toString(), version, requestId);

    prepareUndoOperation();
    if (previousRecord == null) {
      // RESURRECT/CREATE IT

      final OPlaceholder ph = (OPlaceholder) new OCreateRecordTask(rid, content, version, recordType).executeRecordTask(requestId,
          iServer, iManager, database);
      record = ph.getRecord();

      if (record == null)
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
            "+-> Error on updating record %s", rid);

    } else {
      // UPDATE IT
      final int loadedVersion = previousRecord.getVersion();

      if (loadedVersion == version + 1 && Arrays.equals(content, previousRecordContent)) {
        // OPERATION ALREADY EXECUTED
        record = previousRecord;
      } else {
        final ORecord loadedRecord = previousRecord.copy();

        if (loadedRecord instanceof ODocument) {
          // APPLY CHANGES FIELD BY FIELD TO MARK DIRTY FIELDS FOR INDEXES/HOOKS
          final ODocument newDocument = (ODocument) getRecord();

          final ODocument loadedDocument = (ODocument) loadedRecord;
          loadedDocument.merge(newDocument, false, false).getVersion();
          ORecordInternal.setVersion(loadedDocument, version);
        } else
          ORecordInternal.fill(loadedRecord, rid, version, content, true);

        loadedRecord.setDirty();

        record = database.save(loadedRecord);

        if (record == null)
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
              "+-> Error on updating record %s", rid);

        if (version < 0 && ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
              "+-> Reverted %s from version %d to %d", rid, loadedVersion, record.getVersion());
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "+-> updated record %s/%s v.%d [%s]", database.getName(), rid.toString(), record.getVersion(), record);

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
      return new OFixUpdateRecordTask(rid, content, versionCopy, recordType);

    } else if (iGoodResponse instanceof ORecord) {
      // RECORD
      final ORecord goodRecord = (ORecord) iGoodResponse;
      final int versionCopy = ORecordVersionHelper.setRollbackMode(goodRecord.getVersion());
      return new OFixUpdateRecordTask(rid, goodRecord.toStream(), versionCopy, recordType);
    }

    return null;
  }

  @Override
  public ORemoteTask getUndoTask(final ODistributedRequestId reqId) {
    if (previousRecord == null)
      return null;

    final int versionCopy = ORecordVersionHelper.setRollbackMode(previousRecord.getVersion());
    final OUpdateRecordTask task = new OFixUpdateRecordTask(rid, previousRecord.toStream(), versionCopy, recordType);
    task.setLockRecords(false);
    return task;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    super.toStream(out);
    out.writeInt(content.length);
    out.write(content);
    out.writeByte(recordType);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
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

  @Override
  public ORecord prepareUndoOperation() {
    if (previousRecord == null) {
      // READ DIRECTLY FROM THE UNDERLYING STORAGE
      final OStorageOperationResult<ORawBuffer> loaded = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying()
          .readRecord(rid, null, true, false, null);

      if (loaded == null || loaded.getResult() == null)
        return null;

      // SAVE THE CONTENT TO COMPARE IN CASE
      previousRecordContent = loaded.getResult().buffer;

      previousRecord = Orient.instance().getRecordFactoryManager().newInstance(loaded.getResult().recordType);
      ORecordInternal.fill(previousRecord, rid, loaded.getResult().version, loaded.getResult().getBuffer(), false);
    }
    return previousRecord;
  }
}
