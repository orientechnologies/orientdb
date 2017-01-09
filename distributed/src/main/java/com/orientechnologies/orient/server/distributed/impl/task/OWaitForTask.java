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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.util.concurrent.CountDownLatch;

/**
 * Task implementation that waits for a task to be completed.
 * 
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 * 
 */
public class OWaitForTask extends OAbstractRemoteTask {
  private CountDownLatch latch;

  public OWaitForTask(final CountDownLatch iLatch) {
    latch = iLatch;
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
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    // WAIT UNTIL THE REAL TASK IS EXECUTED
    latch.await();
    return null;
  }

  @Override
  public int getFactoryId() {
    return 0;
  }

  @Override
  public String toString() {
    return "WaitFor";
  }

  @Override
  public boolean isUsingDatabase() {
    return false;
  }
}
