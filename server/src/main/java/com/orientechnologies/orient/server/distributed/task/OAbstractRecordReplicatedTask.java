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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public abstract class OAbstractRecordReplicatedTask extends OAbstractReplicatedTask {
  protected ORecordId          rid;
  protected int                version;
  protected int                partitionKey = -1;
  protected boolean            lockRecords  = true;
  protected OLogSequenceNumber lastLSN;

  protected transient ORecord  previousRecord;

  public OAbstractRecordReplicatedTask() {
  }

  protected OAbstractRecordReplicatedTask(final ORecord record) {
    this((ORecordId) record.getIdentity(), record.getVersion());
  }

  protected OAbstractRecordReplicatedTask(final ORecordId iRid, final int iVersion) {
    this.rid = iRid;
    this.version = iVersion;

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null) {
      final OClass clazz = db.getMetadata().getSchema().getClassByClusterId(rid.clusterId);
      if (clazz != null) {
        final Set<OIndex<?>> indexes = clazz.getIndexes();
        if (indexes != null && !indexes.isEmpty()) {
          for (OIndex idx : indexes)
            if (idx.isUnique())
              // UNIQUE INDEX: RETURN THE HASH OF THE NAME TO USE THE SAME PARTITION ID AVOIDING CONCURRENCY ON INDEX UPDATES
              partitionKey = idx.getName().hashCode();
        }
      }
    }
  }

  public abstract Object executeRecordTask(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception;

  public abstract ORecord getRecord();

  @Override
  public final Object execute(final ODistributedRequestId requestId, final OServer iServer,
      final ODistributedServerManager iManager, final ODatabaseDocumentInternal database) throws Exception {

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());
    if (lockRecords) {
      // TRY LOCKING RECORD
      final ODistributedRequestId lockHolder = ddb.lockRecord(rid, requestId);
      if (lockHolder != null)
        throw new ODistributedRecordLockedException(rid, lockHolder);
    }

    try {

      return executeRecordTask(requestId, iServer, iManager, database);

    } finally {
      if (lockRecords)
        // UNLOCK THE SINGLE OPERATION. IN TX WAIT FOR THE 2-PHASE COMMIT/ROLLBACK/FIX MESSAGE
        ddb.unlockRecord(rid, requestId);
    }
  }

  @Override
  public int getPartitionKey() {
    return partitionKey > -1 ? partitionKey : rid.clusterId;
  }

  @Override
  public String toString() {
    return super.toString() + "(" + rid + " v." + version + ")";
  }

  public ORecordId getRid() {
    return rid;
  }

  public int getVersion() {
    return version;
  }

  public boolean checkForClusterAvailability(final String localNode, final ODistributedConfiguration cfg) {
    final String clusterName = ODatabaseRecordThreadLocal.INSTANCE.get().getClusterNameById(rid.clusterId);
    return cfg.hasCluster(localNode, clusterName);
  }

  public ORecord prepareUndoOperation() {
    if (previousRecord == null) {
      // READ DIRECTLY FROM THE UNDERLYING STORAGE
      final OStorageOperationResult<ORawBuffer> loaded = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying()
          .readRecord(rid, null, true, null);

      if (loaded == null || loaded.getResult() == null)
        return null;

      previousRecord = Orient.instance().getRecordFactoryManager().newInstance(loaded.getResult().recordType);
      ORecordInternal.fill(previousRecord, rid, loaded.getResult().version, loaded.getResult().getBuffer(), false);
    }
    return previousRecord;
  }

  public void checkRecordExists() {
    prepareUndoOperation();
    if (previousRecord == null)
      throw new ORecordNotFoundException(rid);
  }

  public OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  public void setLastLSN(final OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(rid.toString());
    out.writeInt(version);
    out.writeInt(partitionKey);
    if (lastLSN != null) {
      out.writeBoolean(true);
      lastLSN.writeExternal(out);
    } else
      out.writeBoolean(false);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readUTF());
    version = in.readInt();
    partitionKey = in.readInt();
    final boolean hasLastLSN = in.readBoolean();
    if (hasLastLSN)
      lastLSN = new OLogSequenceNumber(in);
  }

  public void setLockRecords(final boolean lockRecords) {
    this.lockRecords = lockRecords;
  }

  @Override
  public String getPayload() {
    return "rid=" + rid + " v=" + version;
  }
}
