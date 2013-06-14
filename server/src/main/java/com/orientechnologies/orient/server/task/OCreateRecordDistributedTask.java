/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed create record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCreateRecordDistributedTask extends OAbstractRecordDistributedTask<OPhysicalPosition> {
  private static final long serialVersionUID = 1L;

  protected byte[]          content;
  protected byte            recordType;

  public OCreateRecordDistributedTask() {
  }

  public OCreateRecordDistributedTask(final long iRunId, final long iOperationId, final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType) {
    super(iRunId, iOperationId, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  public OCreateRecordDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final ORecordId iRid, final byte[] iContent, final ORecordVersion iVersion, final byte iRecordType) {
    super(nodeSource, iDbName, iMode, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  public Object getDistributedKey() {
    return rid;
  }

  @Override
  protected OPhysicalPosition executeOnLocalNode(final OStorageSynchronizer dbSynchronizer) {
    OLogManager.instance().info(this, "DISTRIBUTED <-[%s/%s] CREATE RECORD %s v.%s", nodeSource, databaseName, rid.toString(),
        version.toString());
    final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);

    final ODatabaseDocumentTx database = openDatabase();
    rid.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(-1);
    try {
      record.fill(rid, version, content, true);
      if (rid.getClusterId() != -1)
        record.save(database.getClusterNameById(rid.getClusterId()));
      else
        record.save();

      rid = (ORecordId) record.getIdentity();

      return new OPhysicalPosition(rid.getClusterPosition(), record.getRecordVersion());
    } finally {
      closeDatabase(database);
    }
  }

  /**
   * Handles conflict between local and remote execution results.
   * 
   * @param localResult
   *          The result on local node
   * @param remoteResult
   *          the result on remote node
   */
  @Override
  public void handleConflict(final String iRemoteNodeId, final Object localResult, final Object remoteResult) {
    final OReplicationConflictResolver resolver = getDatabaseSynchronizer().getConflictResolver();
    resolver.handleCreateConflict(iRemoteNodeId, rid, new ORecordId(rid.getClusterId(),
        ((OPhysicalPosition) remoteResult).clusterPosition));
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
    if (content == null)
      out.writeInt(0);
    else {
      out.writeInt(content.length);
      out.write(content);
    }
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().writeTo(out, version);
    out.write(recordType);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    rid = new ORecordId(in.readUTF());
    final int contentSize = in.readInt();
    if (contentSize == 0)
      content = null;
    else {
      content = new byte[contentSize];
      in.readFully(content);
    }
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().readFrom(in, version);
    recordType = in.readByte();
  }

  @Override
  public String getName() {
    return "record_create";
  }

  /**
   * Write the status with the new RID too.
   */
  @Override
  protected void setAsCompleted(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().changeOperationStatus(operationLogOffset, rid);
  }

  @Override
  protected OPERATION_TYPES getOperationType() {
    return OPERATION_TYPES.RECORD_CREATE;
  }
}
