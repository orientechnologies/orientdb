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

import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;

/**
 * Base class for Replicated tasks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractReplicatedTask extends OAbstractRemoteTask {
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
    this.runId = iRunId;
    this.operationSerial = iOperationId;
  }

  public abstract OAbstractReplicatedTask copy();

  public abstract String getPayload();

  /**
   * Handles conflict between local and remote execution results.
   * 
   * @param localResult
   *          The result on local node
   * @param remoteResult
   *          the result on remote node
   * @param remoteResult2
   */
  public void handleConflict(String iDatabaseName, final String iRemoteNode, Object localResult, Object remoteResult,
      OReplicationConflictResolver iConfictStrategy) {
  }

  @Override
  public OAbstractRemoteTask copy(final OAbstractRemoteTask iCopy) {
    super.copy(iCopy);
    ((OAbstractReplicatedTask) iCopy).runId = runId;
    ((OAbstractReplicatedTask) iCopy).operationSerial = operationSerial;
    ((OAbstractReplicatedTask) iCopy).align = align;
    return iCopy;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(runId);
    out.writeLong(operationSerial);
    out.writeBoolean(align);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    runId = in.readLong();
    operationSerial = in.readLong();
    align = in.readBoolean();
  }

  public long getOperationSerial() {
    return operationSerial;
  }

  public long getRunId() {
    return runId;
  }

  public boolean isAlign() {
    return align;
  }

  public void setAlign(boolean align) {
    this.align = align;
  }

  @Override
  public String toString() {
    return "{" + runId + "." + operationSerial + "} " + super.toString();
  }

}
