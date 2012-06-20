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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;

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

  public OAbstractRecordDistributedTask(String nodeSource, String iDbName, EXECUTION_MODE iMode, final ORecordId iRid,
      final int iVersion) {
    super(nodeSource, iDbName, iMode);
    this.rid = iRid;
    this.version = iVersion;
  }

  protected abstract T executeOnLocalNode(final OStorageSynchronizer dbSynchronizer);

  public T call() {
    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "DISTRIBUTED <-[%s] %s %s v.%d", nodeSource, getName(), rid, version);

    final OStorageSynchronizer dbSynchronizer = getDatabaseSynchronizer();
    if (isRedistribute())
      // LOG THE OPERATION BEFORE TO SEND TO OTHER NODES
      dbSynchronizer.logOperation(ORecordOperation.CREATED, rid, version, nodeSource);

    // EXECUTE IT LOCALLY
    final T result = executeOnLocalNode(dbSynchronizer);

    if (isRedistribute())
      // SEND OPERATION ACROSS THE CLUSTER TO THE TARGET NODES
      dbSynchronizer.distributeOperation(ORecordOperation.CREATED, rid, this);

    if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
      return result;

    // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
    return null;
  }
}
