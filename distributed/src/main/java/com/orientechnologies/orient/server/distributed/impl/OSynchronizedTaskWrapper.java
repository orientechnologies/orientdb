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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.concurrent.CountDownLatch;

/**
 * Task wrapper to manage synchronized operations like transactions.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSynchronizedTaskWrapper extends OAbstractRemoteTask {
  private boolean usesDatabase;
  private CountDownLatch latch;
  private ORemoteTask task;

  public OSynchronizedTaskWrapper(
      final CountDownLatch iLatch, final String iNodeName, final ORemoteTask iTask) {
    this.latch = iLatch;
    this.task = iTask;
    this.task.setNodeSource(iNodeName);
    this.usesDatabase = true;
  }

  public OSynchronizedTaskWrapper(final CountDownLatch iLatch) {
    latch = iLatch;
    usesDatabase = false;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return null;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    try {
      if (task != null) return task.execute(requestId, iServer, iManager, database);
      return null;
    } finally {
      // RELEASE ALL PENDING WORKERS
      latch.countDown();
    }
  }

  @Override
  public int getFactoryId() {
    return 0;
  }

  @Override
  public String toString() {
    return "(" + (task != null ? task.toString() : "-") + ")";
  }

  @Override
  public boolean isUsingDatabase() {
    return usesDatabase;
  }

  @Override
  public boolean hasResponse() {
    if (task == null) return super.hasResponse();
    else return task.hasResponse();
  }
}
