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

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Base class for Replicated tasks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractReplicatedTask<T> extends OAbstractRemoteTask<T> {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor used from unmarshalling.
   */
  public OAbstractReplicatedTask() {
  }

  /**
   * Constructor used on creation from log.
   * 
   * @param iRunId
   * @param iOperationId
   */
  public OAbstractReplicatedTask(final long iRunId, final long iOperationId) {
    super(iRunId, iOperationId);
  }

  public OAbstractReplicatedTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String databaseName, final EXECUTION_MODE iMode) {
    super(iServer, iDistributedSrvMgr, databaseName, iMode);
    // ASSIGN A UNIQUE OPERATION ID TO BE LOGGED
    this.operationSerial = iDistributedSrvMgr.incrementDistributedSerial(databaseName);

    ODistributedServerLog.debug(this, getNodeSource(), nodeDestination, DIRECTION.OUT,
        "creating operation id %d.%d for db=%s class=%s", runId, operationSerial, databaseName, getClass().getSimpleName());
  }

  public abstract OPERATION_TYPES getOperationType();

  public abstract String getPayload();

  /**
   * Remote node execution
   */
  @SuppressWarnings("unchecked")
  public T call() throws Exception {
    // EXECUTE IT LOCALLY
    final Object localResult = getDistributedServerManager().enqueueLocalExecution(this);

    if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
      return (T) localResult;

    // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
    return null;
  }

  /**
   * Handles conflict between local and remote execution results.
   * 
   * @param localResult
   *          The result on local node
   * @param remoteResult
   *          the result on remote node
   * @param remoteResult2
   */
  public void handleConflict(final String iRemoteNode, Object localResult, Object remoteResult) {
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(getNodeSource());
    out.writeUTF(nodeDestination);
    out.writeUTF(databaseName);
    out.writeLong(runId);
    out.writeLong(operationSerial);
    out.writeByte(mode.ordinal());
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    setNodeSource(in.readUTF());
    nodeDestination = in.readUTF();
    serverInstance = OServer.getInstance(nodeDestination);
    databaseName = in.readUTF();
    runId = in.readLong();
    operationSerial = in.readLong();
    mode = EXECUTION_MODE.values()[in.readByte()];
  }

  public void setAsCommitted(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().setOperationStatus(operationLogOffset, null, ODatabaseJournal.OPERATION_STATUS.COMMITTED);
  }

  public void setAsCanceled(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().setOperationStatus(operationLogOffset, null, ODatabaseJournal.OPERATION_STATUS.CANCELED);
  }

}
