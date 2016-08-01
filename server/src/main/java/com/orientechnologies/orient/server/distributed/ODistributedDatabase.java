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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collection;

/**
 * Generic Distributed Database interface.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public interface ODistributedDatabase {

  String getDatabaseName();

  ODistributedResponse send2Nodes(ODistributedRequest iRequest, Collection<String> iClusterNames, Collection<String> iNodes,
      ODistributedRequest.EXECUTION_MODE iExecutionMode, Object localResult,
      OCallable<Void, ODistributedRequestId> iAfterSentCallback);

  void setOnline();

  /**
   * Locks the record to be sure distributed transactions never work concurrently against the same records in the meanwhile the
   * transaction is executed and the OCompleteTxTask is not arrived.
   *
   * @see #unlockRecord(OIdentifiable, ODistributedRequestId)
   * @param iRecord
   *          Record to lock
   * @param iRequestId
   *          Request id
   * @param timeout
   *          Timeout in ms to wait for the lock
   * @throws com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException
   *           if the record wasn't locked
   */
  boolean lockRecord(OIdentifiable iRecord, final ODistributedRequestId iRequestId, long timeout);

  /**
   * Unlocks the record previously locked through #lockRecord method.
   *
   * @see #lockRecord(OIdentifiable, ODistributedRequestId, long)
   * @param iRecord
   * @param requestId
   */
  void unlockRecord(OIdentifiable iRecord, ODistributedRequestId requestId);

  /**
   * Unlocks all the record locked by node iNodeName
   * 
   * @param iNodeId
   *          node id
   */
  void handleUnreachableNode(int iNodeId);

  ODistributedSyncConfiguration getSyncConfiguration();

  void processRequest(ODistributedRequest request);

  ODistributedTxContext registerTxContext(ODistributedRequestId reqId);

  ODistributedTxContext popTxContext(ODistributedRequestId requestId);

  ODistributedServerManager getManager();

  ODatabaseDocumentInternal getDatabaseInstance();

  long getReceivedRequests();

  long getProcessedRequests();
}
