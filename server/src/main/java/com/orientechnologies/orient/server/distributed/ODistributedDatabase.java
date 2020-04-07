/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Generic Distributed Database interface.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface ODistributedDatabase {

  String getDatabaseName();

  ODistributedResponse send2Nodes(ODistributedRequest iRequest, Collection<String> iClusterNames, Collection<String> iNodes,
      ODistributedRequest.EXECUTION_MODE iExecutionMode, Object localResult,
      OCallable<Void, ODistributedRequestId> iAfterSentCallback, OCallable<Void, ODistributedResponseManager> endCallback);

  void setOnline();

  /**
   * Returns the locked record for read-only purpose. This avoid to have dirty reads until the transaction is fully committed.
   *
   * @param iRecord record to load.
   * @return The record if it is locked, otherwise null.
   */
  ORawBuffer getRecordIfLocked(ORID iRecord);

  /**
   * Replace the record content if it is locked.
   *
   * @param rid   Record ID of the record to find
   * @param bytes Content as byte[] of the record to replace
   */
  void replaceRecordContentIfLocked(ORID rid, byte[] bytes);

  /**
   * Locks the record to be sure distributed transactions never work concurrently against the same records in the meanwhile the
   * transaction is executed and the OCompleteTxTask is not arrived.
   *
   * @param record    Record to lock
   * @param requestId Request id
   * @param timeout   Timeout in ms to wait for the lock
   * @throws com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException if the record wasn't locked
   * @see #unlockRecord(OIdentifiable, ODistributedRequestId)
   */
  boolean lockRecord(ORID record, ODistributedRequestId requestId, long timeout);

  /**
   * Unlocks the record previously locked through #lockRecord method.
   *
   * @param record    Record to unlock
   * @param requestId Request id
   * @see #lockRecord(ORID, ODistributedRequestId, long)
   */
  void unlockRecord(OIdentifiable record, ODistributedRequestId requestId);

  /**
   * Force the locking of a record. If the record was previously locked by a transaction (context), then that transaction is
   * canceled.
   *
   * @param record    Record to lock
   * @param requestId Request id
   */
  boolean forceLockRecord(ORID record, ODistributedRequestId requestId);

  String dump();

  void unlockResourcesOfServer(ODatabaseDocumentInternal database, String serverName);

  /**
   * Unlocks all the record locked by node iNodeName
   *
   * @param nodeName node id
   */
  void handleUnreachableNode(String nodeName);

  ODistributedSyncConfiguration getSyncConfiguration();

  void waitForOnline();

  void reEnqueue(final int senderNodeId, final long msgSequence, final String databaseName, final ORemoteTask payload,
      int retryCount, int autoRetryDelay);

  void processRequest(ODistributedRequest request, boolean waitForAcceptingRequests);

  Optional<OTransactionId> validate(OTransactionId id);

  Optional<OTransactionSequenceStatus> status();

  void rollback(OTransactionId id);

  OTxMetadataHolder commit(OTransactionId id);

  ODistributedTxContext registerTxContext(ODistributedRequestId reqId);

  ODistributedTxContext registerTxContext(final ODistributedRequestId reqId, ODistributedTxContext ctx);

  ODistributedTxContext popTxContext(ODistributedRequestId requestId);

  ODistributedTxContext getTxContext(ODistributedRequestId requestId);

  ODistributedServerManager getManager();

  ODatabaseDocumentInternal getDatabaseInstance();

  long getReceivedRequests();

  long getProcessedRequests();

  void checkNodeInConfiguration(ODistributedConfiguration cfg, String serverName);

  void setLSN(String sourceNodeName, OLogSequenceNumber taskLastLSN, boolean writeLastOperation) throws IOException;

  ODistributedDatabaseRepairer getDatabaseRepairer();

  Optional<OTransactionId> nextId();

  List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState);

  void validateStatus(OTransactionSequenceStatus status);

  void checkReverseSync(OTransactionSequenceStatus lastState);
}