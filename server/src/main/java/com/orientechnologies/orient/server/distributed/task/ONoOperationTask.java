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

import com.orientechnologies.orient.server.journal.ODatabaseJournal.OPERATION_TYPES;

/**
 * Distributed task used for synchronization. It doesn't execute any operation, but it assures the other nodes update the task id.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONoOperationTask extends OAbstractReplicatedTask<Boolean> {
  private static final long serialVersionUID = 1L;

  public ONoOperationTask() {
  }

  public ONoOperationTask(final OAbstractReplicatedTask<?> iTaskToClone) {
    super(iTaskToClone.getServerInstance(), iTaskToClone.getDistributedServerManager(), iTaskToClone.getDatabaseName(),
        iTaskToClone.getMode(), iTaskToClone.getOperationSerial());
  }

  @Override
  public Object executeOnLocalNode() throws Exception {
    return null;
  }

  @Override
  public String getName() {
    return "noop";
  }

  @Override
  public OPERATION_TYPES getOperationType() {
    return OPERATION_TYPES.NOOP;
  }

  @Override
  public String getPayload() {
    return "noop=" + runId + "." + operationSerial;
  }
}
