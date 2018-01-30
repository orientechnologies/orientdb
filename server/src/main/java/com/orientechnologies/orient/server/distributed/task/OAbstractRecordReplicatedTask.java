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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
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
import com.orientechnologies.orient.server.distributed.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OAbstractRecordReplicatedTask extends OAbstractReplicatedTask {
  protected ORecordId rid;
  protected int       version;
  protected int     partitionKey = -1;
  protected boolean lockRecords  = true;

  protected transient ORecord previousRecord;

  public OAbstractRecordReplicatedTask init(final ORecord record) {
    init((ORecordId) record.getIdentity(), record.getVersion());
    return this;
  }

  public OAbstractRecordReplicatedTask init(final ORecordId iRid, final int iVersion) {
    this.rid = iRid;
    this.version = iVersion;

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      final OClass clazz = db.getMetadata().getSchema().getClassByClusterId(rid.getClusterId());
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
    return this;
  }

  public abstract Object executeRecordTask(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception;

  public abstract ORecord getRecord();

  @Override
  public final Object execute(final ODistributedRequestId requestId, final OServer iServer,
      final ODistributedServerManager iManager, final ODatabaseDocumentInternal database) throws Exception {

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    ORecordId rid2Lock = rid;
    if (!rid.isPersistent())
      // CREATE A COPY TO MAINTAIN THE LOCK ON THE CLUSTER AVOIDING THE RID IS TRANSFORMED IN PERSISTENT. THIS ALLOWS TO HAVE
      // PARALLEL TX BECAUSE NEW RID LOCKS THE ENTIRE CLUSTER.
      rid2Lock = new ORecordId(rid.getClusterId(), -1l);

    if (lockRecords) {
      // TRY LOCKING RECORD
      ddb.lockRecord(rid2Lock, requestId, OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong() / 2);
    }

    try {

      return executeRecordTask(requestId, iServer, iManager, database);

    } finally {
      if (lockRecords)
        // UNLOCK THE SINGLE OPERATION. IN TX WAIT FOR THE 2-PHASE COMMIT/ROLLBACK/FIX MESSAGE
        ddb.unlockRecord(rid2Lock, requestId);
    }
  }

  @Override
  public int[] getPartitionKey() {
    return new int[] { partitionKey > -1 ? partitionKey : rid.getClusterId() };
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
    final String clusterName = ODatabaseRecordThreadLocal.instance().get().getClusterNameById(rid.getClusterId());
    return cfg.isServerContainingCluster(localNode, clusterName);
  }

  public ORecord prepareUndoOperation() {
    if (previousRecord == null) {
      // READ DIRECTLY FROM THE UNDERLYING STORAGE
      final OStorageOperationResult<ORawBuffer> loaded = ODatabaseRecordThreadLocal.instance().get().getStorage().getUnderlying()
          .readRecord(rid, null, true, false, null);

      if (loaded == null || loaded.getResult() == null)
        return null;

      previousRecord = Orient.instance().getRecordFactoryManager().newInstance(loaded.getResult().recordType, rid.getClusterId(), ODatabaseRecordThreadLocal.instance().getIfDefined());
      ORecordInternal.fill(previousRecord, rid, loaded.getResult().version, loaded.getResult().getBuffer(), false);
    }
    return previousRecord;
  }

  public void checkRecordExists() {
    prepareUndoOperation();
    if (previousRecord == null)
      throw new ORecordNotFoundException(rid);
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    rid.toStream(out);
    out.writeInt(version);
    out.writeInt(partitionKey);
    if (lastLSN != null) {
      out.writeBoolean(true);
      lastLSN.toStream(out);
    } else
      out.writeBoolean(false);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    rid = new ORecordId();
    rid.fromStream(in);
    version = in.readInt();
    partitionKey = in.readInt();
    final boolean hasLastLSN = in.readBoolean();
    if (hasLastLSN)
      lastLSN = new OLogSequenceNumber(in);
  }

  public void setLockRecords(final boolean lockRecords) {
    this.lockRecords = lockRecords;
  }

  public void setLastLSN(final OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }
}
