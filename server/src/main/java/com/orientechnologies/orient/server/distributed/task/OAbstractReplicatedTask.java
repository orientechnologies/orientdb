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

import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
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

  protected boolean         align            = false;

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
    this.align = true;
  }

  public OAbstractReplicatedTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String databaseName) {
    this(iServer, iDistributedSrvMgr, databaseName, iDistributedSrvMgr.incrementDistributedSerial(databaseName));
  }

  public OAbstractReplicatedTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String databaseName, final long iOperationSerial) {
    super(iServer, iDistributedSrvMgr, databaseName);
    // ASSIGN A UNIQUE OPERATION ID TO BE LOGGED
    this.operationSerial = iOperationSerial;

    ODistributedServerLog.debug(this, getNodeSource(), nodeDestination, DIRECTION.OUT,
        "creating operation id %d.%d for db=%s class=%s", runId, operationSerial, databaseName, getClass().getSimpleName());
  }

  public abstract OAbstractReplicatedTask<T> copy();

  public abstract OPERATION_TYPES getOperationType();

  public abstract String getPayload();

  /**
   * Remote node execution
   */
  @SuppressWarnings("unchecked")
  public T call() throws Exception {
    // EXECUTE IT LOCALLY
    final ORef<Long> journalOffset = new ORef<Long>();

    final T result;
    try {
      result = (T) getDistributedServerManager().enqueueLocalExecution(this, journalOffset);
      getDistributedServerManager().updateJournal(this, getDatabaseSynchronizer(), journalOffset.value, true);
      return result;

    } catch (Exception e) {
      getDistributedServerManager().updateJournal(this, getDatabaseSynchronizer(), journalOffset.value, false);
      throw e;
    }
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

  @SuppressWarnings("unchecked")
  @Override
  public OAbstractRemoteTask<? extends Object> copy(final OAbstractRemoteTask<? extends Object> iCopy) {
    super.copy(iCopy);
    ((OAbstractReplicatedTask<T>) iCopy).align = align;
    return iCopy;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(getNodeSource());
    out.writeUTF(nodeDestination);
    out.writeUTF(databaseName);
    out.writeLong(runId);
    out.writeLong(operationSerial);
    out.writeBoolean(align);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    setNodeSource(in.readUTF());
    nodeDestination = in.readUTF();
    serverInstance = OServer.getInstance(nodeDestination);
    databaseName = in.readUTF();
    runId = in.readLong();
    operationSerial = in.readLong();
    align = in.readBoolean();
  }

  public void setAsCommitted(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().setOperationStatus(operationLogOffset, null, ODatabaseJournal.OPERATION_STATUS.COMMITTED);
  }

  public void setAsCanceled(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().setOperationStatus(operationLogOffset, null, ODatabaseJournal.OPERATION_STATUS.CANCELED);
  }

}
