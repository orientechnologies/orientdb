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
  ODistributedResponse send2Nodes(ODistributedRequest iRequest, Collection<String> iClusterNames, Collection<String> iNodes,
      ODistributedRequest.EXECUTION_MODE iExecutionMode, Object localResult);

  void setOnline();

  /**
   * Locks the record to be sure distributed transactions never work concurrently against the same records in the meanwhile the
   * transaction is executed and the OCompleteTxTask is not arrived.
   *
   * @see #unlockRecord(OIdentifiable)
   * @param iRecord
   *          Record to lock
   * @param iRequestId
   *          Request id
   * @return true if the lock succeed, otherwise false
   */
  boolean lockRecord(OIdentifiable iRecord, final ODistributedRequestId iRequestId);

  /**
   * Unlocks the record previously locked through #lockRecord method.
   *
   * @see #lockRecord(OIdentifiable, ODistributedRequestId)
   * @param iRecord
   *          Record to lock
   */
  void unlockRecord(OIdentifiable iRecord);

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

  ODatabaseDocumentTx getDatabaseInstance();
}
