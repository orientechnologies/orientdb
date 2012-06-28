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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OServerOfflineException;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed create record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractRecordDistributedTask<T> extends OAbstractDistributedTask<T> {
  protected ORecordId rid;
  protected int       version;

  public OAbstractRecordDistributedTask() {
  }

  public OAbstractRecordDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final ORecordId iRid, final int iVersion) {
    super(nodeSource, iDbName, iMode);
    this.rid = iRid;
    this.version = iVersion;
  }

  public OAbstractRecordDistributedTask(final long iRunId, final long iOperationId, final ORecordId iRid, final int iVersion) {
    super(iRunId, iOperationId);
    this.rid = iRid;
    this.version = iVersion;
  }

  protected abstract OPERATION_TYPES getOperationType();

  protected abstract T executeOnLocalNode(final OStorageSynchronizer dbSynchronizer);

  public T call() {
    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "DISTRIBUTED <-[%s] %s %s v.%d", nodeSource, getName(), rid, version);

    if (status != STATUS.ALIGN && !getDistributedServerManager().checkStatus("online"))
      // NODE NOT ONLINE, REFUSE THE OEPRATION
      throw new OServerOfflineException();

    final OStorageSynchronizer dbSynchronizer = getDatabaseSynchronizer();

    // LOG THE OPERATION BEFORE TO SEND TO OTHER NODES
    final long operationLogOffset;
    try {
      operationLogOffset = dbSynchronizer.getLog().journalOperation(runId, operationSerial, getOperationType(), this);
    } catch (IOException e) {
      OLogManager.instance().error(this, "DISTRIBUTED <-[%s] error on logging operation %s %s v.%d", e, nodeSource, getName(), rid,
          version);
      throw new ODistributedException("Error on logging operation", e);
    }

    // EXECUTE IT LOCALLY
    final T result = executeOnLocalNode(dbSynchronizer);

    try {
      setAsCompleted(dbSynchronizer, operationLogOffset);
    } catch (IOException e) {
      OLogManager.instance().error(this, "DISTRIBUTED <-[%s] error on changing the log status for operation %s %s v.%d", e,
          nodeSource, getName(), rid, version);
      throw new ODistributedException("Error on changing the log status", e);
    }

    if (status == STATUS.DISTRIBUTE)
      // SEND OPERATION ACROSS THE CLUSTER TO THE TARGET NODES
      dbSynchronizer.distributeOperation(ORecordOperation.CREATED, rid, this);

    if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
      return result;

    // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
    return null;
  }

  @Override
  public String toString() {
    return getName() + "(" + rid + " v." + version + ")";
  }

  public ORecordId getRid() {
    return rid;
  }

  public void setRid(ORecordId rid) {
    this.rid = rid;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
