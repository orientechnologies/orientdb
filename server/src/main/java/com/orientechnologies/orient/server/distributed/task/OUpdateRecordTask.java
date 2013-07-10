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
package com.orientechnologies.orient.server.distributed.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed updated record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OUpdateRecordTask extends OAbstractRecordReplicatedTask<ORecordVersion> {
  private static final long serialVersionUID = 1L;

  protected byte[]          content;
  protected byte            recordType;

  public OUpdateRecordTask() {
  }

  public OUpdateRecordTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String iDbName, final EXECUTION_MODE iMode, final ORecordId iRid, final byte[] iContent, final ORecordVersion iVersion,
      final byte iRecordType) {
    super(iServer, iDistributedSrvMgr, iDbName, iMode, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  public OUpdateRecordTask(final long iRunId, final long iOperationId, final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType) {
    super(iRunId, iOperationId, iRid, iVersion);
    content = iContent;
    recordType = iRecordType;
  }

  @Override
  public ORecordVersion executeOnLocalNode() {
    ODistributedServerLog.debug(this, getDistributedServerManager().getLocalNodeId(), getNodeSource(), DIRECTION.IN,
        "update record %s/%s v.%s oper=%d.%d", databaseName, rid.toString(), version.toString(), runId, operationSerial);
    final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);

    final ODatabaseDocumentTx database = openDatabase();
    try {
      if (version.getCounter() > -1)
        version.setRollbackMode();
      record.fill(rid, version, content, true);
      record.save();

      return record.getRecordVersion();

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
    resolver.handleUpdateConflict(iRemoteNodeId, rid, version, (ORecordVersion) remoteResult);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(rid.toString());
    out.writeInt(content.length);
    out.write(content);
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
    content = new byte[contentSize];
    in.readFully(content);
    if (version == null)
      version = OVersionFactory.instance().createUntrackedVersion();
    version.getSerializer().readFrom(in, version);
    recordType = in.readByte();
  }

  @Override
  public String getName() {
    return "record_update";
  }

  @Override
  public OPERATION_TYPES getOperationType() {
    return OPERATION_TYPES.RECORD_UPDATE;
  }
}
